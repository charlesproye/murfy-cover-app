#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import re
from urllib.parse import unquote
from datetime import date
import requests
from bs4 import BeautifulSoup
import pandas as pd
from PyPDF2 import PdfReader
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from core.sql_utils import get_connection
from core.gsheet_utils import *
from activation.config.mappings import mapping_vehicle_type

BASE_URL = "https://www.autosphere.fr"
SEARCH_URL_TEMPLATE = "https://www.autosphere.fr/recherche?brand=Mercedes,Bmw,Nissan,Mini,Volkswagen,Tesla,Volvo,Ford,Ds,Opel,Audi,Kia,Toyota,Peugeot,Dacia,Renault,Hyundai,Lexus,Seat,Mitsubishi,Mg&fuel_type=Electrique&from={}"
STEP = 23
START_OFFSET = 0
STOP_OFFSET = 3000


def get_all_vehicle_links():
    all_links = set()
    offset = START_OFFSET

    while True:
        url = SEARCH_URL_TEMPLATE.format(offset)
        print(f"Scraping page with from={offset} ...")
        response = requests.get(url, timeout=10)
        if offset > STOP_OFFSET:
            break
        if response.status_code != 200:
            print(f"Page with from={offset} inaccessible. Stopping scraping.")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        new_links = set()
        for a_tag in soup.select('a[href*="/fiche"]'):
            href = a_tag.get('href')
            if href and "/auto-occasion" in href:
                full_url = BASE_URL + href
                new_links.add(full_url)

        if not new_links:
            print(f"No new links found with from={offset}. Stopping scraping.")
            break

        initial_count = len(all_links)
        all_links.update(new_links)
        added_count = len(all_links) - initial_count

        print(f"{added_count} new links found with from={offset}.")
        offset += STEP
        time.sleep(1)

    print(f"\nTotal number of unique vehicle pages retrieved: {len(all_links)}")
    return list(all_links)

def get_aviloo_pdf_url(driver, link):
    driver.get(link)
    wait = WebDriverWait(driver, 1)
    try:
        aviloo_elem = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "a[href*='certs.aviloo.com']")
            )
        )
        return aviloo_elem.get_attribute("href")
    except Exception as e:
        print(f"Erreur lors de la récupération du lien AVILOO : {e}")
        return None

def get_model_from_autosphere(driver):
    try:
        links = driver.find_elements(By.XPATH, "//a[contains(@href,'/voiture-occasion/')]")
        for link in links:
            href = link.get_attribute("href")
            if href and "/voiture-occasion/" in href:
                parts = href.split("/")
                if len(parts) >= 5:
                    modele = parts[-1].replace(".html", "")
                    modele = unquote(modele)  # décode Zo%c3%a9 → Zoé
                    return modele
    except Exception as e:
        print(f"Erreur récupération modèle générique: {e}")
    return None

def extract_aviloo_data_from_pdf(pdf_path):
    result = {
        "SoH": None,
        "odometer": None,
        "OEM": None,
        "battery_capacity": None,
        "WLTP": None,
    }
    try:
        with open(pdf_path, 'rb') as f:
            reader = PdfReader(f)
            full_text = ""
            for page in reader.pages:
                full_text += page.extract_text() + "\n"
        lines = full_text.splitlines()

        for line in lines:
            line_lower = line.lower()

            # ---- SoH ----
            if result["SoH"] is None and ("soh" in line_lower or "état de santé" in line_lower):
                match = re.search(r"(\d{1,3}[.,]?\d{0,2})\s*%", line)
                if match:
                    result["SoH"] = float(match.group(1).replace(",", "."))

            # ---- odometer ----
            if result["odometer"] is None and ("km" in line_lower or "mileage" in line_lower):
                match = re.search(r"(\d{1,3}(?:[ \u202f]?\d{3})+)\s*km", line_lower)
                if match:
                    result["odometer"] = int(match.group(1).replace(" ", "").replace("\u202f", ""))

            # ---- OEM in sheet <=> make ----
            if result["OEM"] is None and ("marque" in line_lower or "oem" in line_lower or "brand" in line_lower):
                result["OEM"] = line.split(":")[-1].replace('Marque', "").replace('Brand', "").strip()

            # ---- battery ----
            if result["battery_capacity"] is None and ("kwh" in line_lower or "capacité" in line_lower):
                match = re.search(r"(\d{1,2}[.,]?\d{0,2})\s*kwh", line_lower)
                if match:
                    result["battery_capacity"] = float(match.group(1).replace(",", "."))

            # ---- WLTP ----
            if result["WLTP"] is None and ("wltp" in line_lower or "autonomie" in line_lower):
                match = re.findall(r"\d{3}", line_lower)
                if match:
                    result["WLTP"] = match[-1]

    except Exception as e:
        print(f"[PDF] Error reading AVILOO report: {e}")

    return result

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    return webdriver.Chrome(options=options)


def extract_year_and_battery(all_li):
    year, battery_capacity = None, None
    for li in all_li:
        text = li.text.strip()

        # Yead
        if re.search(r"\b20\d{2}\b", text) and not year:
            match = re.search(r"\b(20(?:0[0-79]|0[9]|[1-9]\d))\b", text)
            if match:
                year = int(match.group(1))
            if year > date.today().year:
                year = date.today().year

        # battery capacity
        if not battery_capacity:
            match = re.search(r"\d+(?:[.,]\d+)?\s?kWh", text, re.IGNORECASE)
            if match:
                battery_capacity = match.group(0)

    return year, battery_capacity


def extract_price(wait, car_nbr):
    try:
        price_elem = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "p.text-black-main.font-semibold.text-desktop-lg")
            )
        )
        price_text = price_elem.text.strip()
        return int(re.sub(r"[^\d]", "", price_text))
    except Exception as e:
        print(f"[{car_nbr}] Price not found: {e}")
        return None


def extract_aviloo_data(wait, car_nbr):
    data = None
    try:
        aviloo_link_elem = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='certs.aviloo.com/pdf']"))
        )
        pdf_url = aviloo_link_elem.get_attribute("href")
        if pdf_url:
            pdf_response = requests.get(pdf_url, timeout=10)
            with open("aviloo_tmp.pdf", "wb") as f:
                f.write(pdf_response.content)
                data = extract_aviloo_data_from_pdf("aviloo_tmp.pdf")
                
    except Exception as e:
        print(f"[{car_nbr}] Aviloo not found: {e}")

    return data

def extract_version_complete(driver, modele, car_nbr):
    try:
        h1_title = driver.find_element(By.TAG_NAME, "h1")
        version_complete = h1_title.text.strip()
        if " - " in version_complete:
            version_complete = version_complete.split(" - ")[0]
        if modele != "2008":
            version_complete = re.sub(r"\b20\d{2}\b", "", version_complete)
        return (
            version_complete.replace("Achat Integral", "")
            .replace("Achat Intégral", "")
            .strip()
        )
    except Exception as e:
        print(f"[{car_nbr}] Error reading title: {e}")
        return None


def extract_vehicle_info(link, car_nbr):
    infos = {}
    driver = get_driver()
    try:
        driver.get(link)
        wait = WebDriverWait(driver, 1)
        all_li = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//li")))
        year, battery_capacity = extract_year_and_battery(all_li)
        price = extract_price(wait, car_nbr)
        modele = get_model_from_autosphere(driver)
        version_complete = extract_version_complete(driver, modele, car_nbr)
        data_aviloo = extract_aviloo_data(wait, car_nbr)
        infos.update({
            "lien": link,
            "Type": version_complete,
            "Modèle": modele,
            "Année": int(year) if year else None,
            "battery_capacity": data_aviloo.get("battery_capacity", battery_capacity) ,
            "price": price,
            "SoH": data_aviloo.get('SoH', None),
            "Odomètre (km)": data_aviloo.get("odometer", None),
            "OEM": data_aviloo.get("OEM", None),
            "WLTP": data_aviloo.get("WLTP", None),
        })
    finally:
        driver.quit()
    return infos

def main():
    all_links = get_all_vehicle_links()
    data_sheet = load_excel_data("Courbes de tendance", "Courbes OS")
    df_sheet = pd.DataFrame(columns=data_sheet[0,:7], data=data_sheet[1:,:7])
    links_not_fetch = set(all_links) - set(df_sheet['lien'])
    all_infos = {}

    for i, link in enumerate(tqdm(links_not_fetch, desc="Scraping", unit="link"), start=1):
        print(f"[{i}] Récupération des infos depuis : {link}")
        try:
            info = extract_vehicle_info(link, i)
            all_infos[i] = info
        except Exception as e:
            print(f"[{i}] Erreur sur le lien {link} : {e}")
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

if __name__ == "__main__":
    main()

