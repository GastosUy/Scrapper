## Scrapper de https://comprasestatales.gub.uy/consultas/
#
#Si lo corres solo, baja los de hoy
#Si lo corres con una fecha yyyy-mm-dd baja los de ese dia
#Si lo corres con dos fechas, baja los de entre esas 2 fechas. RSS solo guarda 2000, asi que es lo maximo que podes scrapear, 3-4 dias deberia estar bien
#
#
#
# Creado por @PhDenLogica para @GastosUy



import requests
from bs4 import BeautifulSoup
import csv
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Lock
import logging
from datetime import datetime, timedelta
from ratelimit import limits, sleep_and_retry
import xml.etree.ElementTree as ET
import os
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Global variables
csv_lock = Lock()
CALLS_PER_MINUTE = 300  # Reduced from 500 to avoid rate limiting
CONCURRENT_THREADS = 300  # Reduced from 500 to be gentler on the server
TIMEOUT = 10  # Increased timeout to 60 seconds
MAX_RETRIES = 25  # Number of retries for failed requests

def parse_date(date_str):
    """
    Validate and parse date string in YYYY-MM-DD format
    """
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")

def get_file_paths(date=None):
    """
    Creates folder structure and returns file paths
    """
    if date is None:
        date = datetime.now()
    
    # Create folder name (e.g., "Jan-2025")
    folder_name = date.strftime('%b-%Y')
    
    # Create file name (e.g., "17-1-2025.csv")
    # Remove leading zeros from day and month
    day = str(date.day)
    month = str(date.month)
    year = date.strftime('%Y')
    file_name = f"{day}-{month}-{year}.csv"
    
    # Create full folder path
    folder_path = os.path.join(os.getcwd(), folder_name)
    
    # Create directory if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logging.info(f"Created directory: {folder_path}")
    
    # Full file path
    file_path = os.path.join(folder_path, file_name)
    
    # Temp file for IDs
    temp_file = os.path.join(folder_path, f"ids_{date.strftime('%Y%m%d')}.txt")
    
    return file_path, temp_file

def get_date_range(start_date=None, end_date=None):
    """
    Returns date range for the RSS URL. If no dates provided, uses today's date for both.
    """
    if start_date is None and end_date is None:
        today = datetime.now()
        return today.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def get_rss_feed(start_date=None, end_date=None):
    """
    Fetches and processes the RSS feed to extract IDs
    """
    start_date_str, end_date_str = get_date_range(start_date, end_date)
    rss_url = f"https://www.comprasestatales.gub.uy/consultas/rss/tipo-pub/ADJ/tipo-doc/C/rango-fecha/{start_date_str}_{end_date_str}/filtro-cat/CAT/tipo-orden/DESC"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/rss+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Accept-Charset': 'utf-8',
        'Connection': 'keep-alive',
        'Referer': 'https://www.comprasestatales.gub.uy/'
    }
    
    try:
        response = requests.get(rss_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Parse XML
        root = ET.fromstring(response.content)
        
        # Extract IDs from guid elements
        ids = []
        for item in root.findall('.//item/guid'):
            guid_text = item.text
            if guid_text:
                id_match = re.search(r'/id/([i\d]+)', guid_text)  # Modified to handle IDs starting with 'i'
                if id_match:
                    ids.append(id_match.group(1))  # Store the ID as string to preserve 'i' prefix
        
        return ids
    except Exception as e:
        logging.error(f"Error fetching RSS feed: {e}")
        return []

def get_all_headers(sample_urls):
    """
    Get simplified headers for the CSV file
    """
    headers = [
        'ID', 
        'Organismo', 
        'Resolución', 
        'Objeto de Compra', 
        'Fecha de Compra',
        'Item',
        'monto'
    ]
    return headers

@sleep_and_retry
@limits(calls=CALLS_PER_MINUTE, period=60)
def scrape_data(url, retry_count=0):
    """
    Rate-limited scraping function with retry logic
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Accept-Charset': 'utf-8',
    }

    try:
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        response.encoding = response.apparent_encoding
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except requests.exceptions.RequestException as e:
        if retry_count < MAX_RETRIES:
            logging.warning(f"Attempt {retry_count + 1} failed for {url}: {e}. Retrying...")
            time.sleep(5 * (retry_count + 1))  # Exponential backoff
            return scrape_data(url, retry_count + 1)
        logging.error(f"Error fetching URL {url} after {MAX_RETRIES} attempts: {e}")
        return None

def extract_info(soup, url):
    """
    Extracts information from the BeautifulSoup object and creates separate entries for each item
    """
    if not soup:
        return None

    try:
        # Extract base data that will be common for all items
        base_data = {'ID': url.split('/')[-1]}

        # Extract Organismo
        organismo_element = soup.find('li', class_='active')
        base_data['Organismo'] = organismo_element.find('small').text.strip() if organismo_element else "N/A"

        # Extract basic compra information
        well_div = soup.find('div', class_='well')
        if well_div:
            def extract_value(label):
                list_items = well_div.find_all('li', class_='col-md-6')
                for li in list_items:
                    if label.lower() in li.text.lower():
                        next_li = li.find_next('li')
                        if next_li:
                            strong_tag = next_li.find('strong')
                            return strong_tag.text.strip() if strong_tag else "N/A"
                return "N/A"

            # Get Resolución and check if we should skip this entry
            resolucion = extract_value('Resolución:')
            if resolucion in ["Declarada sin efecto", "Todas las ofertas rechazadas", "Declarada desierta", "N/A"]:
                logging.info(f"Skipping {url} - Resolución: {resolucion}")
                return None
            
            base_data['Resolución'] = resolucion
            
            # Try to get Fecha de Compra first, if N/A then try Fecha Resolución
            fecha_compra = extract_value('Fecha de Compra:')
            if fecha_compra == "N/A":
                fecha_compra = extract_value('Fecha Resolución:')
            base_data['Fecha de Compra'] = fecha_compra

            # Extract Objeto de Compra
            buy_object_element = soup.find('p', class_='buy-object')
            base_data['Objeto de Compra'] = buy_object_element.text.strip() if buy_object_element else "N/A"
        else:
            return None

        # List to store all entries (one per item)
        all_entries = []

        # Extract Items
        item_elements = soup.find_all('div', class_='desc-item')
        for item in item_elements:
            # Create a new entry for this item, starting with the base data
            entry = base_data.copy()
            
            # Extract item description and provider information
            description_element = item.find('h3')
            provider_element = item.find('h4', class_='provider-name')
            
            if description_element:
                # Get the full description text
                full_text = description_element.text.strip()
                
                # Remove the "Ítem Nº X" prefix
                item_prefix_match = re.search(r'Ítem Nº\s*\d+\s*', full_text)
                if item_prefix_match:
                    description = full_text[item_prefix_match.end():].strip()
                else:
                    description = full_text
                
                # Extract code if present
                code_span = description_element.find('span', class_='cod-art')
                if code_span:
                    code_text = code_span.text.strip()
                    # Remove the code from description and add it in parentheses
                    description = description.replace(code_text, '').strip()
                    description = f"{description} {code_text}"
                
                # Add provider information if available
                if provider_element:
                    provider_name = provider_element.find('strong')
                    provider_rut = provider_element.find('span')
                    provider_info = ""
                    if provider_name:
                        provider_info = f" | Proveedor: {provider_name.text.strip()}"
                        if provider_rut:
                            provider_info += f" {provider_rut.text.strip()}"
                    
                    description += provider_info
                
                entry['Item'] = description

            # Extract monto from "Monto total con impuestos"
            monto = "N/A"
            ul_elements = item.find_all('ul', class_='list-inline')
            for ul in ul_elements:
                li_elements = ul.find_all('li')
                for i in range(len(li_elements) - 1):
                    if "Monto total con impuestos:" in li_elements[i].text:
                        monto_text = li_elements[i + 1].find('strong').text.strip()
                        # Remove '$', dots, and everything after comma (including the comma)
                        monto = monto_text.replace('$', '').replace('.', '').split(',')[0].strip()
                        break

            entry['monto'] = monto
            all_entries.append(entry)

        return all_entries if all_entries else None

    except Exception as e:
        logging.error(f"Error extracting info from {url}: {e}")
        return None


def save_to_csv(data, filename, headers=None):
    """
    Thread-safe CSV writing that handles multiple rows per entry
    """
    if not data:  # Skip if no data
        return
        
    with csv_lock:
        try:
            with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                if csvfile.tell() == 0:  # File is empty, write header
                    writer.writeheader()
                # data is now a list of dictionaries, each representing one item
                for row in data:
                    writer.writerow(row)
        except Exception as e:
            logging.error(f"Error saving to CSV: {e}")


def process_url(url, output_file, headers):
    """
    Process a single URL
    """
    try:
        soup = scrape_data(url)
        if soup:
            data = extract_info(soup, url)
            if data:
                save_to_csv(data, output_file, headers)
                logging.info(f"Successfully processed {url}")
                return True
        return False
    except Exception as e:
        logging.error(f"Error processing {url}: {e}")
        return False

def process_failed_urls(failed_urls, output_file, headers):
    """
    Process URLs that failed during the initial run
    Returns set of URLs that still failed after retries
    """
    if not failed_urls:
        return set()

    logging.info(f"Attempting to process {len(failed_urls)} failed URLs with increased timeout...")
    
    # Increase timeout and reduce concurrency for retry attempts
    global TIMEOUT, CONCURRENT_THREADS, CALLS_PER_MINUTE
    original_timeout = TIMEOUT
    original_threads = CONCURRENT_THREADS
    original_calls = CALLS_PER_MINUTE
    
    # More conservative settings for retries
    TIMEOUT = 120
    CONCURRENT_THREADS = 50
    CALLS_PER_MINUTE = 100
    
    still_failed = set()
    processed_count = 0

    try:
        with ThreadPoolExecutor(max_workers=CONCURRENT_THREADS) as executor:
            futures = {executor.submit(process_url, url, output_file, headers): url 
                      for url in failed_urls}
            
            for future in as_completed(futures):
                url = futures[future]
                processed_count += 1
                
                try:
                    if not future.result():
                        still_failed.add(url)
                except Exception as e:
                    still_failed.add(url)
                    logging.error(f"Error in retry processing for {url}: {e}")
                
                # Log progress
                progress = (processed_count / len(failed_urls)) * 100
                logging.info(f"Retry Progress: {progress:.1f}% ({processed_count}/{len(failed_urls)})")
                
                if url not in still_failed:
                    logging.info(f"Successfully recovered: {url}")
    finally:
        # Restore original values
        TIMEOUT = original_timeout
        CONCURRENT_THREADS = original_threads
        CALLS_PER_MINUTE = original_calls
    
    recovered = len(failed_urls) - len(still_failed)
    if recovered > 0:
        logging.info(f"Successfully recovered {recovered} URLs during retry")
    
    return still_failed

def generate_performance_report(start_time, total_ids, processed_ids, failed_urls, retried_urls, output_file):
    """
    Generate a detailed performance report of the scraping process
    """
    end_time = time.time()
    total_time = end_time - start_time
    
    # Count successful entries in CSV
    csv_entries = 0
    try:
        with open(output_file, 'r', encoding='utf-8-sig') as f:
            csv_entries = sum(1 for line in f) - 1  # Subtract 1 for header
    except Exception as e:
        logging.error(f"Error counting CSV entries: {e}")
    
    # Calculate statistics
    successful_urls = len(processed_ids) - len(failed_urls)
    success_rate = (successful_urls / len(processed_ids)) * 100 if processed_ids else 0
    
    # Generate report
    report = "\n" + "="*50 + "\n"
    report += "PERFORMANCE REPORT\n"
    report += "="*50 + "\n"
    report += f"Total Runtime: {total_time:.2f} seconds ({total_time/60:.2f} minutes)\n"
    report += f"Processing Speed: {successful_urls/total_time:.2f} URLs/second\n\n"
    
    report += "DATA STATISTICS\n"
    report += "-"*20 + "\n"
    report += f"Total IDs Found: {total_ids}\n"
    report += f"Unique IDs Processed: {len(processed_ids)}\n"
    report += f"Successfully Scraped: {successful_urls}\n"
    report += f"Failed URLs: {len(failed_urls)}\n"
    report += f"Retried URLs: {len(retried_urls)}\n"
    report += f"Entries in CSV: {csv_entries}\n"
    report += f"Success Rate: {success_rate:.1f}%\n\n"
    
    report += "FILE INFORMATION\n"
    report += "-"*20 + "\n"
    report += f"Output File: {output_file}\n"
    
    if failed_urls:
        report += "\nFAILED URLs (First 5 shown):\n"
        report += "-"*20 + "\n"
        for url in list(failed_urls)[:5]:
            report += f"- {url}\n"
        if len(failed_urls) > 5:
            report += f"... and {len(failed_urls) - 5} more\n"
    
    report += "="*50
    
    logging.info(report)
    
    # Save report to file
    report_file = os.path.splitext(output_file)[0] + "_report.txt"
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        logging.info(f"Performance report saved to: {report_file}")
    except Exception as e:
        logging.error(f"Error saving performance report: {e}")

def main(ids, output_file="output.csv"):
    """
    Main function with progress tracking and error handling
    """
    # Generate URLs from IDs
    urls = [f"https://www.comprasestatales.gub.uy/consultas/detalle/id/{id_}"
            for id_ in ids]

    if not urls:
        logging.error("No URLs to process!")
        return

    # Get headers from sample
    headers = get_all_headers(urls)

    # Initialize tracking
    total_urls = len(urls)
    processed_urls = 0
    start_time = time.time()
    processed_ids = set()
    failed_urls = set()
    retried_urls = set()
    permanently_failed = set()

    logging.info(f"Starting to process {total_urls} URLs")

    # Process URLs concurrently
    with ThreadPoolExecutor(max_workers=CONCURRENT_THREADS) as executor:
        futures = []
        
        # First pass to check for duplicates
        for url in urls:
            id_ = url.split('/')[-1]
            if id_ not in processed_ids:
                processed_ids.add(id_)
                futures.append(executor.submit(process_url, url, output_file, headers))
        
        logging.info(f"After removing duplicates, processing {len(futures)} unique URLs")
        
        for idx, future in enumerate(as_completed(futures)):
            processed_urls += 1
            if not future.result():
                failed_urls.add(urls[idx])

            # Calculate and display progress
            elapsed_time = time.time() - start_time
            progress = (processed_urls / len(futures)) * 100
            urls_per_second = processed_urls / elapsed_time if elapsed_time > 0 else 0
            eta = (len(futures) - processed_urls) / urls_per_second if urls_per_second > 0 else 0

            logging.info(f"Progress: {progress:.1f}% ({processed_urls}/{len(futures)})")
            logging.info(f"Speed: {urls_per_second:.2f} URLs/second")
            logging.info(f"Estimated time remaining: {eta:.1f} seconds")

    logging.info("Initial scraping completed!")
    logging.info(f"Successfully processed: {len(processed_ids) - len(failed_urls)} URLs")
    
    if failed_urls:
        retried_urls = failed_urls.copy()
        logging.info(f"Retrying {len(failed_urls)} failed URLs...")
        permanently_failed = process_failed_urls(failed_urls, output_file, headers)
        
        if permanently_failed:
            logging.warning(f"{len(permanently_failed)} URLs still failed after retries")

    # Generate performance report
    generate_performance_report(start_time, len(ids), processed_ids, permanently_failed, retried_urls, output_file)


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Scrape Compras Estatales data with optional date range')
    parser.add_argument('date', nargs='?', type=parse_date, help='Single date or start date in YYYY-MM-DD format')
    parser.add_argument('end_date', nargs='?', type=parse_date, help='Optional end date in YYYY-MM-DD format')
    
    args = parser.parse_args()
    
    # Handle date arguments
    if args.date:
        start_date = args.date
        end_date = args.end_date if args.end_date else args.date
        
        if args.end_date and end_date < start_date:
            parser.error("End date cannot be earlier than start date")
        
        # Use the end date for file naming
        output_file, temp_file = get_file_paths(end_date)
    else:
        start_date = end_date = None
        output_file, temp_file = get_file_paths()
    
    # Get IDs from RSS feed
    logging.info("Fetching IDs from RSS feed...")
    ids = get_rss_feed(start_date, end_date)
    
    if not ids:
        logging.error("No IDs found in the RSS feed!")
        exit(1)
    
    # Save IDs to temporary file
    with open(temp_file, 'w', encoding='utf-8') as f:
        for id_ in ids:
            f.write(f"{id_}\n")
    
    logging.info(f"Found {len(ids)} IDs. Saved to {temp_file}")
    
    # Run main scraping process
    main(ids, output_file)
