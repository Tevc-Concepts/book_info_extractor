import asyncio
import aiohttp
import logging
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import Optional, List, Dict
import json
import sqlite3
import os
from datetime import datetime
from urllib.parse import quote
import re
from pathlib import Path
from dotenv import load_dotenv
import random
import time
from book_info import BookInfo, Config  # Import from original file

# Load environment variables
load_dotenv()

class ScraperConfig:
    DATABASE_PATH = Config.DATABASE_PATH
    IMAGE_FOLDER = Config.IMAGE_FOLDER
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }

    # Rotate between different user agents
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    ]

class FallbackScraper:
    def __init__(self):
        self.session = None
        self.db_path = ScraperConfig.DATABASE_PATH
        self.image_folder = ScraperConfig.IMAGE_FOLDER
        Path(self.image_folder).mkdir(parents=True, exist_ok=True)
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('fallback_scraper.log'),
                logging.StreamHandler()
            ]
        )

    async def initialize_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def cleanup(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def get_random_headers(self):
        headers = ScraperConfig.HEADERS.copy()
        headers['User-Agent'] = random.choice(ScraperConfig.USER_AGENTS)
        return headers

    async def download_cover_image(self, isbn: str, cover_url: str) -> Optional[str]:
        """Download and save book cover image."""
        if not cover_url:
            return None

        try:
            ext = os.path.splitext(cover_url)[1].lower()
            if not ext or ext not in ['.jpg', '.jpeg', '.png']:
                ext = '.jpg'

            filename = f"{isbn}_fallback{ext}"
            filepath = os.path.join(self.image_folder, filename)

            async with self.session.get(cover_url, headers=self.get_random_headers()) as response:
                if response.status == 200:
                    with open(filepath, 'wb') as f:
                        f.write(await response.read())
                    return filepath
                return None

        except Exception as e:
            logging.error(f"Error downloading cover image for ISBN {isbn}: {str(e)}")
            return None

    async def scrape_bookfinder(self, isbn: str) -> Optional[BookInfo]:
        """Scrape book information from BookFinder."""
        try:
            url = f"https://www.bookfinder.com/search/?isbn={isbn}&mode=isbn&st=sr&ac=qr"
            await asyncio.sleep(random.uniform(2, 4))  # Random delay

            async with self.session.get(url, headers=self.get_random_headers()) as response:
                if response.status != 200:
                    return None

                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

                # Extract book information
                title_elem = soup.find('div', {'class': 'details-header'})
                title = title_elem.find('h1').text.strip() if title_elem else None

                author_elem = soup.find('span', {'class': 'author'})
                authors = [author_elem.text.strip()] if author_elem else None

                

                # Try to find publication year
                pub_info = soup.find('div', {'class': 'pubInfo'})
                year_match = re.search(r'\b(19|20)\d{2}\b', pub_info.text) if pub_info else None
                publication_year = int(year_match.group()) if year_match else None

                # Try to find publisher
                publisher_elem = pub_info.find('span', {'class': 'publisher'}) if pub_info else None
                publisher = publisher_elem.text.strip() if publisher_elem else None

                # Try to find cover image
                cover_elem = soup.find('img', {'class': 'cover'})
                cover_url = cover_elem.get('src') if cover_elem else None
                cover_path = await self.download_cover_image(isbn, cover_url) if cover_url else None

                if title:
                    return BookInfo(
                        isbn=isbn,
                        title=title,
                        authors=authors,
                        publication_year=publication_year,
                        publisher=publisher,
                        cover_url=cover_url,
                        cover_path=cover_path,
                        source='bookfinder'
                    )

        except Exception as e:
            logging.error(f"Error scraping BookFinder for ISBN {isbn}: {str(e)}")
        return None

    async def scrape_amazon(self, isbn: str) -> Optional[BookInfo]:
        """Scrape book information from Amazon."""
        try:
            url = f"https://www.amazon.com/s?k={isbn}&i=stripbooks"
            await asyncio.sleep(random.uniform(3, 5))  # Random delay

            async with self.session.get(url, headers=self.get_random_headers()) as response:
                if response.status != 200:
                    return None

                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

                # Find the first book result
                book_div = soup.find('div', {'data-component-type': 's-search-result'})
                if not book_div:
                    return None

                # Extract book information
                title_elem = book_div.find('h2')
                title = title_elem.text.strip() if title_elem else None

                author_elem = book_div.find('div', {'class': 'a-row a-size-base'})
                authors = [a.strip() for a in author_elem.text.split('by')[1].split(',')] if author_elem else None

                # Try to find publication year from the title or description
                year_match = re.search(r'\b(19|20)\d{2}\b', str(book_div))
                publication_year = int(year_match.group()) if year_match else None

                # Try to find cover image
                cover_elem = book_div.find('img', {'class': 's-image'})
                cover_url = cover_elem.get('src') if cover_elem else None
                cover_path = await self.download_cover_image(isbn, cover_url) if cover_url else None

                if title:
                    return BookInfo(
                        isbn=isbn,
                        title=title,
                        authors=authors,
                        publication_year=publication_year,
                        cover_url=cover_url,
                        cover_path=cover_path,
                        source='amazon'
                    )

        except Exception as e:
            logging.error(f"Error scraping Amazon for ISBN {isbn}: {str(e)}")
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
            logging.info(f"Saved book information for ISBN {book.isbn} from {book.source}")

        except Exception as e:
            logging.error(f"Error saving to database for ISBN {book.isbn}: {str(e)}")

    async def process_isbn(self, isbn: str) -> Optional[BookInfo]:
        """Process a single ISBN through multiple sources."""
        # Try BookFinder first
        book_info = await self.scrape_bookfinder(isbn)
        if book_info:
            self.save_to_database(book_info)
            return book_info

        # If BookFinder fails, try Amazon
        book_info = await self.scrape_amazon(isbn)
        if book_info:
            self.save_to_database(book_info)
            return book_info

        return None

    async def process_failed_isbns(self, failed_isbns_file: str):
        """Process ISBNs that failed in the original Google Books API fetch."""
        try:
            # Read failed ISBNs
            with open(failed_isbns_file, 'r') as f:
                failed_isbns = f.read().splitlines()

            if not failed_isbns:
                logging.info("No failed ISBNs to process")
                return

            logging.info(f"Processing {len(failed_isbns)} failed ISBNs")
            
            await self.initialize_session()
            
            successful_count = 0
            still_failed_isbns = []

            for isbn in failed_isbns:
                result = await self.process_isbn(isbn)
                if result:
                    successful_count += 1
                    logging.info(f"Successfully retrieved data for ISBN {isbn} from fallback sources")
                else:
                    still_failed_isbns.append(isbn)
                    logging.warning(f"Failed to retrieve data for ISBN {isbn} from fallback sources")

            # Save still-failed ISBNs
            if still_failed_isbns:
                with open('still_failed_isbns.txt', 'w') as f:
                    f.write('\n'.join(still_failed_isbns))

            logging.info(f"Fallback processing complete. Retrieved {successful_count}/{len(failed_isbns)} books")
            
        except Exception as e:
            logging.error(f"Error processing failed ISBNs: {str(e)}")
        finally:
            await self.cleanup()

async def main():
    try:
        failed_isbns_file = 'failed_isbns.txt'
        if not os.path.exists(failed_isbns_file):
            logging.error(f"Failed ISBNs file not found: {failed_isbns_file}")
            return

        scraper = FallbackScraper()
        await scraper.process_failed_isbns(failed_isbns_file)

    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    if sys.platform.startswith('win32'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise