#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import time
import re
import pandas as pd
from PyPDF2 import PdfReader

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.gsheet_utils import *


BASE_URL = "https://www.autosphere.fr"
SEARCH_URL_TEMPLATE = "https://www.autosphere.fr/recherche?brand=Peugeot&fuel_type=Electrique&from={}"#"https://www.autosphere.fr/recherche?fuel_type=Electrique&from={}"
STEP = 23
START_OFFSET = 0
STOP_OFFSET = 10


def get_all_vehicle_links():
    all_links = set()
    offset = START_OFFSET

    while True:
        url = SEARCH_URL_TEMPLATE.format(offset)
        print(f"Scraping page avec from={offset} ...")
        response = requests.get(url)
        if offset > STOP_OFFSET:
            break
        if response.status_code != 200:
            print(f"Page avec from={offset} inaccessible. Fin du scraping.")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        new_links = set()
        for a_tag in soup.select('a[href*="/fiche"]'):
            href = a_tag.get('href')
            if href and "/auto-occasion" in href:
                full_url = BASE_URL + href
                new_links.add(full_url)

        if not new_links:
            print(f"Aucun nouveau lien trouvé avec from={offset}. Fin du scraping.")
            break

        initial_count = len(all_links)
        all_links.update(new_links)
        added_count = len(all_links) - initial_count

        print(f"{added_count} nouveaux liens trouvés avec from={offset}.")
        offset += STEP
        time.sleep(1)

    print(f"\nNombre total de fiches uniques récupérées : {len(all_links)}")
    return list(all_links)

def extract_soh_from_pdf(pdf_path):
    try:
        with open(pdf_path, 'rb') as f:
            reader = PdfReader(f)
            for page in reader.pages:
                text = page.extract_text()
                for line in text.splitlines():
                    if "SoH" in line or "état de santé" in line.lower():
                        match = re.search(r"(\d{1,3}[.,]?\d{0,2})\s*%", line)
                        if match:
                            return float(match.group(1).replace(',', '.'))
    except Exception as e:
        print(f"[PDF] Erreur lecture SoH : {e}")
    return None

def extract_aviloo_data_from_pdf(pdf_path):
    result = {
        "SoH": None,
        "kilometrage": None,
        "OEM": None,
        "Modèle": None
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

            if result["SoH"] is None and ("soh" in line_lower or "état de santé" in line_lower):
                match = re.search(r"(\d{1,3}[.,]?\d{0,2})\s*%", line)
                if match:
                    result["SoH"] = float(match.group(1).replace(",", "."))

            if result["kilometrage"] is None and ("km" in line_lower or "mileage" in line_lower):
                match = re.search(r"(\d{1,3}[ \u202f]?\d{3})\s*km", line_lower)
                if match:
                    result["kilometrage"] = int(match.group(1).replace(" ", "").replace("\u202f", ""))

            if result["OEM"] is None and ("marque" in line_lower or "oem" in line_lower or "brand" in line_lower):
                result["OEM"] = line.split(":")[-1].strip()

            if result["Modèle"] is None and ("modèle" in line_lower or "model" in line_lower):
                result["Modèle"] = line.split(":")[-1].strip()

    except Exception as e:
        print(f"[PDF] Erreur lecture rapport AVILOO : {e}")

    return result

def extract_vehicle_info(link, car_nbr):
    infos = {}

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Décommente pour exécuter sans interface
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(link)
        wait = WebDriverWait(driver, 2)
        all_li = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//li")))
        modele = None
        annee = None

        for li in all_li:
            text = li.text.strip()

            if re.search(r"\b20\d{2}\b", text) and not annee:
                annee = re.search(r"\b(20(?:0[0-79]|0[9]|[1-9]\d))\b", text).group(1)
        try:
            aviloo_link_elem = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='certs.aviloo.com/pdf']")))
            pdf_url = aviloo_link_elem.get_attribute("href")
            if pdf_url:
                pdf_response = requests.get(pdf_url)
                with open("aviloo_tmp.pdf", "wb") as f:
                    f.write(pdf_response.content)
                data = extract_aviloo_data_from_pdf("aviloo_tmp.pdf")
                if data["SoH"]:
                    infos["SoH"] = float(data["SoH"]) / 100
                if data["kilometrage"]:
                    infos["Odomètre (km)"] = int(data["kilometrage"])
                if data["OEM"]:
                    infos["OEM"] = data["OEM"]
                if data["Modèle"]:
                    modele = data["Modèle"]
        except:
            pass
        version_complete = None
        try:
            links_breadcrumb = wait.until(EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "li.inline-flex.items-center a.text-blue-700")
            ))
            if len(links_breadcrumb) >= 4:
                modele = links_breadcrumb[3].text.strip()
            version_span = driver.find_element(By.CSS_SELECTOR, "span.text-black-600")
            version_complete = version_span.text.strip()
            if ' - ' in version_complete:
                version_complete = version_complete[:version_complete.find(' - ')]
            if modele != "2008":
                version_complete = re.sub(r"\b20\d{2}\b", "", version_complete)
            version_complete = version_complete.replace('Achat Integral', "").replace('Achat Intégral', "").strip()
        except Exception as e:
            print(f"[{car_nbr}] Erreur fil d’Ariane : {e}")
        infos["lien"] = link
        infos["Type"] = version_complete
        infos["Modèle"] = modele
    finally:    
        driver.quit()
    return infos


def main():
    all_links = get_all_vehicle_links()
    all_infos = {}

    for i, link in enumerate(all_links, start=1):
        print(f"[{i}] Récupération des infos depuis : {link}")
        try:
            info = extract_vehicle_info(link, i)
            all_infos[i] = info
        except Exception as e:
            print(f"[{i}] Erreur sur le lien {link} : {e}")
        time.sleep(1)
    # Bien ordonner les colonnes par rapoort à la gsheet
    infos_clean = pd.DataFrame(all_infos).T.dropna(subset='SoH')[["OEM","Modèle","Type","Année","Odomètre (km)","SoH", "lien"]]
    print(infos_clean.shape)
    export_to_excel(infos_clean, "202505 - Courbes SoH", "Courbes OS")


if __name__ == "__main__":
    main()
