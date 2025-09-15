
import re
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
import time
import unicodedata
from pathlib import Path
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from core.gsheet_utils import load_excel_data, export_to_excel
from core.sql_utils import get_connection
from activation.config.mappings import mapping_vehicle_type


URL = "https://www.ev-market.fr/voiture-electrique?brand_name=&model_name=&version_name=&finition_name=&brand_id=&model_id=&version_id=&min_soh_percentage=1&max_soh_percentage=&location=&latitude=&longitude=&radius=&min_price=&max_price=&min_odometer=&max_odometer=&min_year=&max_year=&min_first_registration_date=&max_first_registration_date=&min_battery_capacity=&max_battery_capacity=&min_fast_charge_power=&max_fast_charge_power=&min_range_real=&max_range_real=&min_height=&max_height=&min_length=&max_length=&min_width=&max_width=&min_width_mirrors=&max_width_mirrors="
PATH_FILE = Path(__file__).parent / "link_unsuable.txt"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Scraper/1.0; +https://example.org/bot)"
}

def build_page_url(base_url, page_number):
    """Construit l'URL pour une page spécifique"""
    parsed_url = urlparse(base_url)
    query_params = parse_qs(parsed_url.query)
    query_params['page'] = [str(page_number)]
    return f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{urlencode(query_params, doseq=True)}"

def is_car_link(url):
    """Vérifie si l'URL est un lien vers une voiture"""
    return bool(re.search(r'/voitures-electriques/\d+', url))

def get_car_links_from_page(url):
    """Récupère les liens de voitures d'une page"""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        
        links = set()
        for a in soup.find_all("a", href=True):
            full_url = urljoin(url, a["href"])
            if is_car_link(full_url):
                links.add(full_url)
        
        return links, soup
    
    except Exception as e:
        print(f"Erreur sur {url}: {e}")
        return set(), None

def scrape_all_cars(max_pages=None, delay=1):
    """Scrape tous les liens de voitures sur toutes les pages"""
    all_links = set()
    
    print("Analyse de la première page...")
    first_links, first_soup = get_car_links_from_page(URL)
    
    if not first_soup:
        print("Erreur: impossible de récupérer la première page")
        return []
    
    all_links.update(first_links)
    print(f"Page 1: {len(first_links)} liens trouvés")
    
    for page in range(2, max_pages + 1):
        print(f"Page {page}...")
        
        page_url = build_page_url(URL, page)
        page_links, _ = get_car_links_from_page(page_url)
        
        if not page_links:
            print(f"Aucun lien trouvé page {page}, arrêt")
            break
        
        all_links.update(page_links)
        print(f"Page {page}: {len(page_links)} liens trouvés")
        
        if delay > 0:
            time.sleep(delay)
    
    print(f"Terminé! Total: {len(all_links)} liens uniques")
    return sorted(all_links)

def extract_autonomy_wltp(soup):
    """Récupère l'autonomie WLTP ou temps doux depuis le div spécifique."""
    div = soup.find("div", class_="text-sm font-semibold text-gray-900 group-hover:text-green-600 transition-colors")
    if div:
        text = div.get_text(strip=True)
        m = re.search(r"(\d{2,4})\s*km", text)
        if m:
            return int(m.group(1))
    return None

def extract_capacity_useful(soup):
    # Trouver le bloc Batterie, puis la ligne “Capacité utile”
    node = soup.find("h3", string=re.compile("Informations Batterie", re.IGNORECASE))
    if node:
        # chercher dans ce bloc jusqu'à la fin ou jusqu’au bloc suivant
        nexts = node.find_next_siblings()
        for sib in nexts:
            text = sib.get_text(strip=True)
            m = re.search(r"Capacité utile\s*:\s*([0-9]+(?:[.,][0-9]+)?)\s*kWh", text, re.IGNORECASE)
            if m:
                val = m.group(1).replace(",", ".")
                try:
                    return float(val)
                except Exception:
                    return None
    return None

def extract_mileage(soup):
    # chercher le texte “Kilométrage : X km”
    text = soup.get_text(separator=" ", strip=True)
    m = re.search(r"Kilométrage\s*[:\-]?\s*([0-9\u00A0\s\.,]+)\s*km", text, re.IGNORECASE)
    if m:
        raw = m.group(1)
        # supprimer espaces, points, etc.
        cleaned = re.sub(r"[^\d]", "", raw)
        if cleaned.isdigit():
            return int(cleaned)
    return None

def extract_section_data(soup, section_title):
    def normalize_key(text):
        text = text.lower()
        return ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )
    data = {}
    header = soup.find("h3", string=re.compile(section_title, re.IGNORECASE))
    if header:
        div = header.find_next("div", class_="pt-1")
        if div:
            rows = div.find_all("div", class_=re.compile("flex"))
            for row in rows:
                spans = row.find_all("span", recursive=False)
                if len(spans) >= 2:
                    label = normalize_key(spans[0].get_text(strip=True))
                    value = spans[1].get_text(strip=True)
                    data[label] = value
    return data

def get_soh(soup):
    p_tag = soup.find("p", class_="text-5xl font-bold text-primary-600")
    if p_tag:
        soh = p_tag.get_text(strip=True)
        return float(soh.replace('%', ''))
    return None

def get_price(soup):
    div_tag = soup.find("div", class_="text-3xl font-bold text-gray-900")

    if div_tag:
        price = div_tag.get_text(strip=True)
        price = price.replace(' ', '').replace('€', '').replace('.', '')
        return float(price)
    return None

def get_car_infos(url):
    resp = requests.get(url, timeout=2)
    soup = BeautifulSoup(resp.text, "lxml")
    wltp = extract_autonomy_wltp(soup)
    ident = extract_section_data(soup, "Identification")
    usage = extract_section_data(soup, "Usage")
    capacity = extract_capacity_useful(soup)
    mileage = extract_mileage(soup)
    soh = get_soh(soup)
    price = get_price(soup)
    try: 
        date = int(re.search(r'\d{4}', usage.get("premiere mise en circulation", None))[0]) 
    except Exception:
        date = None
        
    result = {
        "OEM": ident.get("marque", None),
        "Modèle": ident.get("modele", None),
        "Type": ident.get("version", None),
        "finition": ident.get("finition", None),
        "battery_capacity": capacity,
        "Odomètre (km)": mileage,
        "Année": date ,
        "WLTP": wltp,
        "SoH": soh,
        "price": price,
        "lien": url
    }

    return result
if __name__ == "__main__":

    all_car_link = scrape_all_cars(max_pages=100, delay=1)
    data_sheet = load_excel_data("Courbes de tendance", "Courbes OS")
    df_sheet = pd.DataFrame(columns=data_sheet[0,:7], data=data_sheet[1:,:7])
    try:
        with open(PATH_FILE, "r", encoding="utf-8") as f:
            link_already_try = {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        link_already_try = set()
    links_not_fetch = set(all_car_link) - set(df_sheet['lien']) - link_already_try
    
    print(f"Number of link to scrappe = {len(links_not_fetch)}")
    all_infos = {}
    for i, link in enumerate(all_car_link, 1):
        print(f"{i:2d}. {link}")
        try:
            all_infos[i] = get_car_infos(link)
        except Exception as e:
            print(f"[{i}] Erreur sur le lien {link} : {e}")
            with open(PATH_FILE, "a", encoding="utf-8") as f:
                f.write(f"{link}\n")
                f.flush()
        time.sleep(1)
        
    infos_clean = pd.DataFrame(all_infos).T.dropna(subset='SoH')[["OEM", "Modèle", "Type", "Année", "Odomètre (km)", "SoH", "lien", "battery_capacity", "price", "WLTP"]]
    # Mapping to database model
    with get_connection() as con:
        cursor = con.cursor()
        cursor.execute("""SELECT vm.model_name, vm.id, vm.type, vm.commissioning_date, vm.end_of_life_date, m.make_name, b.capacity FROM vehicle_model vm
                                                    join make m on vm.make_id=m.id
                                                    join battery b on b.id=vm.battery_id;""")
        model_existing = pd.DataFrame(cursor.fetchall(), columns=["model_name", "id", "type",  "commissioning_date", "vm.end_of_life_date", "make_name", "capacity"])
    infos_clean['id'] = infos_clean.apply(lambda row: mapping_vehicle_type(row['Type'], row['OEM'], row['Modèle'], model_existing, row['battery_capacity'], row['Année']), axis=1)
    type_mapping = infos_clean.merge(model_existing[['id', 'type']], on='id', how='left')['type']
    infos_clean['Type'] = [mapped if mapped != 'unknown' else old for old, mapped in zip(infos_clean['Type'], type_mapping) ]
    infos_clean.drop(columns='id', inplace=True)
    infos_clean['Odomètre (km)'] = infos_clean['Odomètre (km)'].astype(float)
    infos_clean['WLTP'] = infos_clean['WLTP'].astype(float)
    infos_clean['price'] = infos_clean['price'].astype(float)
    infos_clean = infos_clean.replace(np.nan, "unknown").replace(pd.NA, "unknown")
    export_to_excel(infos_clean, "Courbes de tendance", "Courbes OS")


