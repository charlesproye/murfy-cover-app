import requests
from bs4 import BeautifulSoup
import re
import time
import pandas as pd
import numpy as np
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional
from src.core.gsheet_utils import get_google_client, load_excel_data, export_to_excel
from src.results.trendlines_results.config import TYPE_MAPPING


class AramisautoScraper:
    def __init__(self):
        self.base_url = "https://www.aramisauto.com"
        self.session = requests.Session()
        # Headers pour simuler un navigateur réel
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """Récupère le contenu d'une page web"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            print(f"Erreur lors de la récupération de {url}: {e}")
            return None
    
    def extract_car_links_from_listing(self, soup: BeautifulSoup) -> List[str]:
        """Extrait tous les liens vers les voitures depuis une page de listing"""
        car_links = []
        
        # Différents sélecteurs possibles pour les liens de voitures
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
                    # Convertir en URL absolue si nécessaire
                    if href.startswith('/'):
                        href = urljoin(self.base_url, href)
                    
                    # Vérifier que c'est bien un lien vers une voiture
                    if self.is_car_link(href):
                        car_links.append(href)
        
        return list(set(car_links))  # Supprimer les doublons
    
    def is_car_link(self, url: str) -> bool:
        """Vérifie si une URL pointe vers une fiche de voiture"""
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
        """Extrait les informations d'une voiture depuis sa fiche"""
        soup = self.get_page_content(car_url)
        if not soup:
            return {"lien": car_url, "error": "Impossible de récupérer la page"}
        
        car_info = {"lien": car_url}
        
        try: 
            page_text = soup.get_text()
            # SOH
            soh_partern = r'\d{2,3}\s%'
            soh_match = re.search(soh_partern, page_text)
            if soh_match:
                car_info['SoH'] = soh_match.group(0)
            # Kilométrage
            km_pattern = r'([\d\s]+)\s*km'
            km_match = re.search(km_pattern, page_text)
            if km_match:
                car_info['Odomètre (km)'] = int(km_match.group(0).replace('km', '').replace(' ', ''))
            # Date
            date_pattern = r'\d{1,2}\/\d{1,2}\/\d{2,4}'
            date = re.search(date_pattern, page_text)
            if date:
                car_info['Année'] = int(date.group(0)[-4:].strip())
                # else:
                #     car_info['Année'] = int(date.group(1)[-4:].strip())
                # print(car_info['Année'])
            
            # Batterie capacity
            battery_pattern = r'\d*.\d*\s[k][W][h]'
            battery_match = re.search(battery_pattern, page_text)
            if battery_match:
                car_info['battery_capacity'] = battery_match.group(0)
            
            # Type
            price_info_elements = soup.select('.default-body.price-information-row, .price-information-row, .default-body')
            type_car = " ".join(price_info_elements[0].text.split())
            car_info['Type'] = type_car
            # Marque et modèle depuis l'URL
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
            
        except Exception as e:
            car_info['error'] = f"Erreur lors de l'extraction: {e}"
        
        return car_info
    
    def scrape_electric_cars(self, max_pages: int = 100) -> List[Dict]:
        """Scrape toutes les voitures électriques"""
        print("Début du scraping des voitures électriques...")
        
        all_cars = []
        base_electric_url = "https://www.aramisauto.com/achat/electrique/"
        
        # Récupérer la première page
        soup = self.get_page_content(base_electric_url)
        if not soup:
            print("Impossible de récupérer la page principale")
            return []
        
        # Extraire les liens de voitures
        car_links = self.extract_car_links_from_listing(soup)
        print(f"Trouvé {len(car_links)} liens de voitures sur la page principale")
        
        # Chercher d'autres pages (pagination)
        pagination_links = []
        for page_num in range(2, max_pages + 1):
            page_url = f"{base_electric_url}?page={page_num}"
            page_soup = self.get_page_content(page_url)
            if page_soup:
                page_car_links = self.extract_car_links_from_listing(page_soup)
                if page_car_links:
                    car_links.extend(page_car_links)
                    print(f"Page {page_num}: {len(page_car_links)} liens trouvés")
                else:
                    break
        
        # Supprimer les doublons  
        car_links = list(set(car_links))
        print(f"Total: {len(car_links)} liens uniques trouvés")
        
        # Extraire les informations de chaque voiture
        for i, car_url in enumerate(car_links, 1):
            print(f"Extraction {i}/{len(car_links)}: {car_url}")
            car_info = self.extract_car_info(car_url)
            all_cars.append(car_info)
            time.sleep(1)
            
        
        return all_cars
    
    def clean_data(self, data):
        # ordonner les données par rapport a la gsheet
        df = pd.DataFrame(data)[["OEM", "Modèle", "Type", "Année", "Odomètre (km)", "SoH", "lien", "battery_capacity"]]
        # Si pas de SoH pas intérressant
        df = df.dropna(subset='SoH')
        # éviter que ça casse s'il manque une info
        df = df.replace(np.nan, "unknown").replace(pd.NA, "unknown")
        print(df.battery_capacity.unique())
        return df
        
        
def main():
    scraper = AramisautoScraper()
    
    # Scraper les voitures électriques
    infos = scraper.scrape_electric_cars(100)
    df_infos = scraper.clean_data(infos)
    data_sheet = load_excel_data(get_google_client(), "202505 - Courbes SoH", "Courbes OS")
    df_sheet = pd.DataFrame(columns=data_sheet[0,:8], data=data_sheet[1:,:8])
    df_filtré = df_infos[~df_infos['lien'].isin(df_sheet['lien'])]
   # df_filtré['Type'] =  df_filtré['Type'].map(TYPE_MAPPING)
    print(df_filtré.isna().sum())
    export_to_excel(df_filtré, "202505 - Courbes SoH", "Courbes OS")

if __name__ == "__main__":
    main()
