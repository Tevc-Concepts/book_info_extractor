import asyncio
import sys
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
from pathlib import Path
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configuration class
class Config:
    GOOGLE_BOOKS_API_KEY = os.getenv('GOOGLE_BOOKS_API_KEY')
    INPUT_CSV_PATH = os.getenv('INPUT_CSV_PATH', 'isbn_list.csv')
    OUTPUT_CSV_PATH = os.getenv('OUTPUT_CSV_PATH', 'book_data.csv')
    IMAGE_FOLDER = os.getenv('IMAGE_FOLDER', 'book_covers')
    DATABASE_PATH = os.getenv('DATABASE_PATH', 'books.db')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '50'))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    GOODREADS_USER_AGENT = os.getenv('GOODREADS_USER_AGENT', 
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

    @classmethod
    def validate(cls):
        if not cls.GOOGLE_BOOKS_API_KEY:
            raise ValueError("GOOGLE_BOOKS_API_KEY is required in .env file")
        
        if not os.path.exists(os.path.dirname(os.path.abspath(cls.OUTPUT_CSV_PATH))):
            os.makedirs(os.path.dirname(os.path.abspath(cls.OUTPUT_CSV_PATH)))
            
        if not os.path.exists(cls.IMAGE_FOLDER):
            os.makedirs(cls.IMAGE_FOLDER)

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('book_extractor.log'),
        logging.StreamHandler()
    ]
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
    cover_path: Optional[str] = None
    reviews: Optional[List[Dict]] = None
    source: Optional[str] = None

class BookDataExtractor:
    def __init__(self):
        self.google_api_key = Config.GOOGLE_BOOKS_API_KEY
        self.db_path = Config.DATABASE_PATH
        self.image_folder = Config.IMAGE_FOLDER
        self.session = None
        self.initialize_database()
        self.ensure_image_folder()
        # Add rate limiting parameters
        self.requests_per_minute = 50  # Google Books API default limit is 60 but I used 50
        self.last_request_time = time.time()
        self.request_count = 0

    def ensure_image_folder(self):
        """Create image folder if it doesn't exist."""
        Path(self.image_folder).mkdir(parents=True, exist_ok=True)

    async def cleanup(self):
        if self.session and not self.session.closed:
            await self.session.close()

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
            ext = os.path.splitext(cover_url)[1].lower()
            if not ext or ext not in ['.jpg', '.jpeg', '.png']:
                ext = '.jpg'

            filename = f"{isbn}{ext}"
            filepath = os.path.join(self.image_folder, filename)

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
    @limits(calls=1000, period=3600)
    
    async def fetch_google_books(self, isbn: str) -> Optional[BookInfo]:
        """Fetch book information from Google Books API with rate limiting."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Implement rate limiting
                current_time = time.time()
                if self.request_count >= self.requests_per_minute:
                    time_passed = current_time - self.last_request_time
                    if time_passed < 60:
                        sleep_time = 60 - time_passed + 2  # Add 2 second buffer
                        logging.info(f"Rate limit reached. Waiting {sleep_time:.2f} seconds...")
                        await asyncio.sleep(sleep_time)
                        self.request_count = 0
                        self.last_request_time = time.time()

                url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}&key={self.google_api_key}"
                async with self.session.get(url) as response:
                    if response.status == 200:
                        self.request_count += 1
                        data = await response.json()
                        if data.get('items'):
                            volume_info = data['items'][0]['volumeInfo']
                            cover_url = volume_info.get('imageLinks', {}).get('thumbnail')
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
                    elif response.status == 429:
                        retry_count += 1
                        wait_time = min(2 ** retry_count, 60)  # Exponential backoff
                        logging.warning(f"Rate limit hit for ISBN {isbn}. Waiting {wait_time} seconds before retry {retry_count}/{max_retries}")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logging.error(f"Failed to fetch data for ISBN {isbn}. Status: {response.status}")
                        return None
                        
            except Exception as e:
                logging.error(f"Error fetching from Google Books API for ISBN {isbn}: {str(e)}")
                return None

        logging.error(f"Max retries reached for ISBN {isbn}")
        return None
    def save_to_database(self, book: BookInfo):
        """Save book information to SQLite database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Log the data being saved
            logging.debug(f"Saving to database: {asdict(book)}")
            
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
            cursor.execute("SELECT * FROM books WHERE isbn = ?", (book.isbn,))
            result = cursor.fetchone()
            logging.debug(f"Verified database entry for ISBN {book.isbn}: {result}")
            
            conn.close()
        except Exception as e:
            logging.error(f"Error saving to database for ISBN {book.isbn}: {str(e)}")

    async def process_isbn(self, isbn: str) -> Optional[BookInfo]:
        """Process a single ISBN through multiple sources."""
        book_info = await self.fetch_google_books(isbn)
        if book_info:
            self.save_to_database(book_info)
            return book_info
        return None

    async def process_isbn_list(self, isbns: List[str]):
        """Process a list of ISBNs in smaller batches with rate limiting."""
        try:
            await self.initialize_session()
            
            processed_count = 0
            successful_count = 0
            failed_isbns = []
            
            # Reduce batch size for better rate limit handling
            batch_size = 10  # Smaller batch size
            
            for i in range(0, len(isbns), batch_size):
                batch = isbns[i:i + batch_size]
                logging.info(f"Processing batch {i//batch_size + 1} of {len(isbns)//batch_size + 1}")
                
                # Process each ISBN in batch with delay between requests
                for isbn in batch:
                    result = await self.process_isbn(isbn)
                    processed_count += 1
                    
                    if result:
                        successful_count += 1
                        logging.info(f"Successfully processed ISBN: {result.isbn} ({successful_count}/{processed_count})")
                    else:
                        failed_isbns.append(isbn)
                        logging.warning(f"Failed to process ISBN: {isbn}")
                    
                    # Add small delay between requests
                    await asyncio.sleep(0.5)
                
                # Add delay between batches
                if i + batch_size < len(isbns):
                    logging.info("Waiting between batches...")
                    await asyncio.sleep(2)
                
                logging.info(f"Progress: {processed_count}/{len(isbns)} ISBNs processed, {successful_count} successful")
            
            # Log failed ISBNs for retry
            if failed_isbns:
                logging.info(f"Failed ISBNs ({len(failed_isbns)}): {failed_isbns}")
                # Save failed ISBNs to file for later retry
                with open('failed_isbns.txt', 'w') as f:
                    f.write('\n'.join(failed_isbns))
                
            logging.info(f"Final results: {successful_count}/{len(isbns)} ISBNs processed successfully")
            
        finally:
            if self.session and not self.session.closed:
                await self.session.close()

    def export_to_csv(self, output_file: str):
        """Export database contents to CSV file."""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get count of records
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM books")
            count = cursor.fetchone()[0]
            logging.info(f"Exporting {count} records to CSV")
            
            # Read and export data
            df = pd.read_sql_query("SELECT * FROM books", conn)
            logging.debug(f"DataFrame shape before export: {df.shape}")
            logging.debug(f"DataFrame columns: {df.columns}")
            logging.debug(f"First few records: {df.head()}")
            
            df.to_csv(output_file, index=False)
            
            # Verify the export
            if os.path.exists(output_file):
                file_size = os.path.getsize(output_file)
                logging.info(f"CSV file created successfully. Size: {file_size} bytes")
            else:
                logging.error("CSV file was not created")
                
            conn.close()
        except Exception as e:
            logging.error(f"Error exporting to CSV: {str(e)}")

def read_isbns_from_csv(file_path: str, isbn_column: str = 'ISBN') -> List[str]:
    """Read ISBNs from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        if isbn_column not in df.columns:
            raise ValueError(f"Column '{isbn_column}' not found in CSV file")
        
        isbns = df[isbn_column].astype(str).str.replace(r'[-\s]', '', regex=True).tolist()
        return isbns
    
    except Exception as e:
        logging.error(f"Error reading ISBNs from CSV: {str(e)}")
        return []

async def main():
    try:
        # Validate configuration
        Config.validate()
        
        # Read ISBNs from CSV
        isbns = read_isbns_from_csv(Config.INPUT_CSV_PATH)
        if not isbns:
            logging.error("No ISBNs found in CSV file")
            return
        
        logging.info(f"Found {len(isbns)} ISBNs in CSV file")
        
        # Initialize extractor
        extractor = BookDataExtractor()
        
        # Instead of asyncio.run(), directly await the process_isbn_list
        await extractor.process_isbn_list(isbns)
        
        # Export results
        extractor.export_to_csv(Config.OUTPUT_CSV_PATH)
        
        logging.info(f"Process completed. Results exported to {Config.OUTPUT_CSV_PATH}")
        logging.info(f"Book covers saved in {Config.IMAGE_FOLDER} directory")
        
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        raise

    finally:
        # Ensure proper cleanup of any remaining tasks
        tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    if sys.platform.startswith('win32'):
        # Windows specific event loop policy
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise