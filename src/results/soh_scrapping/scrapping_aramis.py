import requests
from bs4 import BeautifulSoup
import re
import time
import pandas as pd
import numpy as np
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional
from core.gsheet_utils import get_google_client, load_excel_data, export_to_excel
from activation.config.mappings import mapping_vehicle_type
from core.sql_utils import get_connection
from activation.config.mappings import mapping_vehicle_type

class AramisautoScraper:
    def __init__(self):
        self.base_url = "https://www.aramisauto.com"
        self.session = requests.Session()
        # Headers to simulate a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch the HTML content of a page"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def extract_car_links_from_listing(self, soup: BeautifulSoup) -> List[str]:
        """Extract all car listing links from a listing page"""
        car_links = []
        
        # Different possible selectors for car links
        selectors = [
            'a[href*="/voitures/"]',
            'a[href*="/vehicule/"]',
            '.car-card a',
            '.vehicle-card a',
            '.product-card a',
            'a[href*="vehicleId="]'
        ]
        
        for selector in selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href:
                    # Convert to absolute URL if needed
                    if href.startswith('/'):
                        href = urljoin(self.base_url, href)
                    
                    # Confirm the URL is actually a car page
                    if self.is_car_link(href):
                        car_links.append(href)
        return car_links
    
    def is_car_link(self, url: str) -> bool:
        """Check if a URL points to a car detail page"""
        car_patterns = [
            r'/voitures/.+/.+/.+/rv\d+/',
            r'/vehicule/',
            r'vehicleId=\d+',
            r'/voitures/.+/.+/.+/\d+/'
        ]
        
        for pattern in car_patterns:
            if re.search(pattern, url):
                return True
        return False
    
    def extract_car_info(self, car_url: str) -> Dict:
        """Extract information from a single car page"""
        soup = self.get_page_content(car_url)
        if not soup:
            return {"lien": car_url, "error": "Unable to retrieve the page"}
        
        car_info = {"lien": car_url}
        
        try: 
            page_text = soup.get_text()
            
            # SoH
            soh_partern = r'\d{2,3}\s%'
            soh_match = re.search(soh_partern, page_text)
            if soh_match:
                car_info['SoH'] = soh_match.group(0)
                print(car_info['SoH'])
            
            # Odometer
            km_pattern = r'([\d\s]+)\s*km'
            km_match = re.search(km_pattern, page_text)
            if km_match:
                car_info['Odomètre (km)'] = int(km_match.group(0).replace('km', '').replace(' ', ''))
            
            # Year
            date_pattern = r'\d{1,2}\/\d{1,2}\/\d{2,4}'
            date = re.search(date_pattern, page_text)
            if date:
                car_info['Année'] = int(date.group(0)[-4:].strip())
            
            # Battery capacity
            battery_pattern = r'\d*.\d*\s[k][W][h]'
            battery_match = re.search(battery_pattern, page_text)
            if battery_match:
                car_info['battery_capacity'] = battery_match.group(0)
            
            # Full version (Type)
            price_info_elements = soup.select('.default-body.price-information-row, .price-information-row, .default-body')
            type_car = " ".join(price_info_elements[0].text.split())
            car_info['Type'] = type_car

            # Brand and model from URL
            url_parts = urlparse(car_url).path.split('/')
            if len(url_parts) >= 4 and 'voitures' in url_parts:
                try:
                    brand_index = url_parts.index('voitures') + 1
                    if brand_index < len(url_parts):
                        car_info['OEM'] = url_parts[brand_index].replace('-', ' ').title()
                    if brand_index + 1 < len(url_parts):
                        car_info['Modèle'] = url_parts[brand_index + 1].replace('-', ' ').title()
                except (ValueError, IndexError):
                    pass
            # Price
            price_block = soup.select_one('div.price-amount')
            if price_block:
                price_text = price_block.get_text(strip=True)
                price_clean = re.sub(r'[^\d]', '', price_text)
                if price_clean:
                    car_info['price'] = int(price_clean)
                    
        except Exception as e: 
            car_info['error'] = f"Error extracting car data: {e}"
        return car_info
    
    def scrape_electric_cars(self, max_pages: int = 100, existing_links: Optional[set] = None) -> List[Dict]:
        """Scrape all electric cars from the AramisAuto website"""
        print("Starting electric car scraping...")
        
        all_cars = []
        base_electric_url = "https://www.aramisauto.com/achat/electrique/"
        
        # Load the first page
        soup = self.get_page_content(base_electric_url)
        if not soup:
            print("Unable to load main electric page")
            return []
        
        # Extract car links
        car_links = self.extract_car_links_from_listing(soup)

        print(f"Found {len(car_links)} car links on the main page")
        
        # Look for additional paginated pages
        for page_num in range(2, max_pages + 1):
            page_url = f"{base_electric_url}?page={page_num}"
            page_soup = self.get_page_content(page_url)
            if page_soup:
                page_car_links = self.extract_car_links_from_listing(page_soup)
                if page_car_links:
                    car_links.extend(page_car_links)
                    print(f"Page {page_num}: {len(page_car_links)} links found")
                else:
                    break
    
        if existing_links is not None:
            car_links = set(car_links) - existing_links
        print(f"Total: {len(car_links)} unique car links found")
        
        # Extract data from each car page
        for i, car_url in enumerate(car_links, 1):
            print(f"Extracting {i}/{len(car_links)}: {car_url}")
            car_info = self.extract_car_info(car_url)
            all_cars.append(car_info)
            time.sleep(1)
        
        return all_cars
    
    def clean_data(self, data):
        # Align column order with Google Sheets structure
        df = pd.DataFrame(data)[["OEM", "Modèle", "Type", "Année", "Odomètre (km)", "SoH", "lien", "battery_capacity", 'price']]

        # Drop cars without SoH
        df = df.dropna(subset='SoH')

        # Ensure numeric field is integer
        df['Odomètre (km)'] =  df['Odomètre (km)'].astype(int)

        # Replace missing values with 'unknown'
        df = df.replace(np.nan, "unknown").replace(pd.NA, "unknown")
        return df


def main():
    
    # Load current data from Google Sheet
    data_sheet = load_excel_data(get_google_client(), "Courbes de tendance", "Courbes OS")
    df_sheet = pd.DataFrame(columns=data_sheet[0,:8], data=data_sheet[1:,:8])
    
    scraper = AramisautoScraper()

    # Scrape electric car listings
    infos = scraper.scrape_electric_cars(100, existing_links = set(df_sheet['lien']))
    df_infos = scraper.clean_data(infos)

    # Map with internal database model
    with get_connection() as con:
        cursor = con.cursor()
        cursor.execute("""SELECT vm.model_name, vm.id, vm.type, vm.commissioning_date, vm.end_of_life_date, m.make_name, b.capacity FROM vehicle_model vm
                                                    join make m on vm.make_id=m.id
                                                    join battery b on b.id=vm.battery_id;""")
        model_existing = pd.DataFrame(cursor.fetchall(), columns=["model_name", "id", "type",  "commissioning_date", "vm.end_of_life_date", "make_name", "capacity"])

    df_infos['id'] = df_infos.apply(lambda row: mapping_vehicle_type(row['Type'], row['OEM'], row['Modèle'], model_existing, row['battery_capacity'], row['Année']), axis=1)
    type_mapping = df_infos.merge(model_existing[['id', 'type']], on='id', how='left')['type']
    df_infos['Type'] = [mapped if mapped != 'unknown' else old for old, mapped in zip(df_infos['Type'], type_mapping)]
    df_infos.drop(columns='id', inplace=True)
    df_infos = df_infos.replace(np.nan, "unknown").replace(pd.NA, "unknown")
    export_to_excel(df_infos, "Courbes de tendance", "Courbes OS")

if __name__ == "__main__":
    main()

