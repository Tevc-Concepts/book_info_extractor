import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from typing import Dict, List, Optional
import time
import logging
import json
from dataclasses import dataclass, asdict
from ratelimit import limits, sleep_and_retry
import sqlite3
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@dataclass
class BookInfo:
    isbn: str
    title: Optional[str] = None
    authors: Optional[List[str]] = None
    publication_year: Optional[int] = None
    publisher: Optional[str] = None
    description: Optional[str] = None
    cover_url: Optional[str] = None
    cover_path: Optional[str] = None  # Local path to saved cover image
    reviews: Optional[List[Dict]] = None
    source: Optional[str] = None

class BookDataExtractor:
    def __init__(self, google_api_key: str, db_path: str = 'books.db', image_folder: str = 'book_covers'):
        self.google_api_key = google_api_key
        self.db_path = db_path
        self.image_folder = image_folder
        self.session = None
        self.initialize_database()
        self.ensure_image_folder()

    def ensure_image_folder(self):
        """Create image folder if it doesn't exist."""
        Path(self.image_folder).mkdir(parents=True, exist_ok=True)

    def initialize_database(self):
        """Initialize SQLite database with required schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS books (
                isbn TEXT PRIMARY KEY,
                title TEXT,
                authors TEXT,
                publication_year INTEGER,
                publisher TEXT,
                description TEXT,
                cover_url TEXT,
                cover_path TEXT,
                reviews TEXT,
                source TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

    async def initialize_session(self):
        """Initialize aiohttp session."""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def download_cover_image(self, isbn: str, cover_url: str) -> Optional[str]:
        """Download and save book cover image."""
        if not cover_url:
            return None

        try:
            # Determine file extension from URL or default to .jpg
            ext = os.path.splitext(cover_url)[1].lower()
            if not ext or ext not in ['.jpg', '.jpeg', '.png']:
                ext = '.jpg'

            # Create filename with ISBN
            filename = f"{isbn}{ext}"
            filepath = os.path.join(self.image_folder, filename)

            # Download and save image
            async with self.session.get(cover_url) as response:
                if response.status == 200:
                    with open(filepath, 'wb') as f:
                        f.write(await response.read())
                    return filepath
                return None

        except Exception as e:
            logging.error(f"Error downloading cover image for ISBN {isbn}: {str(e)}")
            return None

    @sleep_and_retry
    @limits(calls=1000, period=3600)  # 1000 requests per hour
    async def fetch_google_books(self, isbn: str) -> Optional[BookInfo]:
        """Fetch book information from Google Books API."""
        try:
            url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}&key={self.google_api_key}"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('items'):
                        volume_info = data['items'][0]['volumeInfo']
                        cover_url = volume_info.get('imageLinks', {}).get('thumbnail')
                        
                        # Download cover image if available
                        cover_path = await self.download_cover_image(isbn, cover_url) if cover_url else None
                        
                        return BookInfo(
                            isbn=isbn,
                            title=volume_info.get('title'),
                            authors=volume_info.get('authors', []),
                            publication_year=int(volume_info.get('publishedDate', '').split('-')[0])
                                if volume_info.get('publishedDate') else None,
                            publisher=volume_info.get('publisher'),
                            description=volume_info.get('description'),
                            cover_url=cover_url,
                            cover_path=cover_path,
                            source='google_books'
                        )
                return None
        except Exception as e:
            logging.error(f"Error fetching from Google Books API for ISBN {isbn}: {str(e)}")
            return None

    async def fetch_open_library(self, isbn: str) -> Optional[BookInfo]:
        """Fetch book information from Open Library API."""
        try:
            url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        book_data = data.get(f"ISBN:{isbn}")
                        if book_data:
                            cover_url = book_data.get('cover', {}).get('large')
                            cover_path = await self.download_cover_image(isbn, cover_url) if cover_url else None
                            
                            return BookInfo(
                                isbn=isbn,
                                title=book_data.get('title'),
                                authors=[author['name'] for author in book_data.get('authors', [])],
                                publication_year=book_data.get('publish_date'),
                                publisher=book_data.get('publishers', [None])[0],
                                cover_url=cover_url,
                                cover_path=cover_path,
                                source='open_library'
                            )
                return None
        except Exception as e:
            logging.error(f"Error fetching from Open Library API for ISBN {isbn}: {str(e)}")
            return None

    async def scrape_goodreads(self, isbn: str) -> Optional[BookInfo]:
        """Scrape book information from Goodreads."""
        try:
            url = f"https://www.goodreads.com/search?q={isbn}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            
            async with self.session.get(url, headers=headers) as response:
                if response.status == 200:
                    content = await response.text()
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    title = soup.find('h1', class_='bookTitle')
                    author = soup.find('a', class_='authorName')
                    cover_img = soup.find('img', class_='bookCover')
                    
                    cover_url = cover_img.get('src') if cover_img else None
                    cover_path = await self.download_cover_image(isbn, cover_url) if cover_url else None
                    
                    return BookInfo(
                        isbn=isbn,
                        title=title.text.strip() if title else None,
                        authors=[author.text.strip()] if author else None,
                        cover_url=cover_url,
                        cover_path=cover_path,
                        source='goodreads'
                    )
                return None
        except Exception as e:
            logging.error(f"Error scraping Goodreads for ISBN {isbn}: {str(e)}")
            return None

    def save_to_database(self, book: BookInfo):
        """Save book information to SQLite database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO books 
                (isbn, title, authors, publication_year, publisher, description, 
                cover_url, cover_path, reviews, source) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                book.isbn,
                book.title,
                json.dumps(book.authors) if book.authors else None,
                book.publication_year,
                book.publisher,
                book.description,
                book.cover_url,
                book.cover_path,
                json.dumps(book.reviews) if book.reviews else None,
                book.source
            ))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"Error saving to database for ISBN {book.isbn}: {str(e)}")

    async def process_isbn(self, isbn: str) -> Optional[BookInfo]:
        """Process a single ISBN through multiple sources."""
        # Try Google Books API first
        book_info = await self.fetch_google_books(isbn)
        
        # If not found, try Open Library
        if not book_info:
            book_info = await self.fetch_open_library(isbn)
        
        # If still not found, try web scraping
        if not book_info:
            book_info = await self.scrape_goodreads(isbn)
        
        if book_info:
            self.save_to_database(book_info)
            return book_info
        
        return None

    async def process_isbn_list(self, isbns: List[str], batch_size: int = 50):
        """Process a list of ISBNs in batches."""
        await self.initialize_session()
        
        for i in range(0, len(isbns), batch_size):
            batch = isbns[i:i + batch_size]
            tasks = [self.process_isbn(isbn) for isbn in batch]
            results = await asyncio.gather(*tasks)
            
            # Process results
            for result in results:
                if result:
                    logging.info(f"Successfully processed ISBN: {result.isbn}")
                    
        await self.session.close()

    def export_to_csv(self, output_file: str):
        """Export database contents to CSV file."""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM books", conn)
        df.to_csv(output_file, index=False)
        conn.close()

def read_isbns_from_csv(file_path: str, isbn_column: str = 'ISBN') -> List[str]:
    """Read ISBNs from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        if isbn_column not in df.columns:
            raise ValueError(f"Column '{isbn_column}' not found in CSV file")
        
        # Clean ISBNs (remove spaces, hyphens, etc.)
        isbns = df[isbn_column].astype(str).str.replace(r'[-\s]', '', regex=True).tolist()
        return isbns
    
    except Exception as e:
        logging.error(f"Error reading ISBNs from CSV: {str(e)}")
        return []

def main():
    # Configuration
    api_key = "YOUR_GOOGLE_BOOKS_API_KEY"
    input_csv = "isbn_list.csv"  # Your input CSV file
    output_csv = "book_data.csv"
    image_folder = "book_covers"
    
    # Read ISBNs from CSV
    isbns = read_isbns_from_csv(input_csv)
    if not isbns:
        logging.error("No ISBNs found in CSV file")
        return
    
    logging.info(f"Found {len(isbns)} ISBNs in CSV file")
    
    # Initialize extractor
    extractor = BookDataExtractor(api_key, image_folder=image_folder)
    
    # Run the async process
    asyncio.run(extractor.process_isbn_list(isbns))
    
    # Export results
    extractor.export_to_csv(output_csv)
    
    logging.info(f"Process completed. Results exported to {output_csv}")
    logging.info(f"Book covers saved in {image_folder} directory")

if __name__ == "__main__":
    main()   