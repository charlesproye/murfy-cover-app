#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import time
import re
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.gsheet_utils import *


BASE_URL = "https://www.autosphere.fr"
SEARCH_URL_TEMPLATE = "https://www.autosphere.fr/recherche?fuel_type=Electrique&from={}"
STEP = 23
START_OFFSET = 115
STOP_OFFSET = 100


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


def extract_vehicle_info(link, car_nbr):
    infos = {}

    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")  # Décommente pour exécuter sans interface
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(link)
        wait = WebDriverWait(driver, 15)
        all_li = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//li")))

        score_sante = None
        kilometrage = None
        annee = None

        for li in all_li:
            text = li.text.strip()
            if "Santé" in text and not score_sante:
                match = re.search(r"(\d+\s?%)", text)
                if match:
                    score_sante = match.group(1)

            if "km" in text and not kilometrage:
                match = re.search(r"([\d\s]+km)", text)
                if match:
                    kilometrage = match.group(1).strip().replace("\u202f", "").replace(" ", "").replace("km", "").strip()

            if re.search(r"\b20\d{2}\b", text) and not annee:
                annee = re.search(r"\b(20\d{2})\b", text).group(1)

        titre_element = wait.until(EC.presence_of_element_located((By.XPATH, "//h1")))
        titre_text = titre_element.text.strip()

        match = re.search(r"\d+ch\s+\w+", titre_text)

        marque = None
        modele = None
        version_complete = None

        try:
            links_breadcrumb = wait.until(EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "li.inline-flex.items-center a.text-blue-700")
            ))
            if len(links_breadcrumb) >= 4:
                marque = links_breadcrumb[2].text.strip()
                modele = links_breadcrumb[3].text.strip()
            version_span = driver.find_element(By.CSS_SELECTOR, "span.text-black-600")
            version_complete = version_span.text.strip()
            if ' - ' in version_complete:
                version_complete = version_complete[:version_complete.find(' - ')]
            if modele != "2008":
                version_complete = re.sub(r"\b20\d{2}\b", "", version_complete)
            version_complete = version_complete.replace('Achat Integral', "").strip()
        except Exception as e:
            print(f"[{car_nbr}] Erreur fil d’Ariane : {e}")

        infos["lien"] = link
        infos["SoH"] = score_sante
        infos["Année"] = int(annee)
        infos["Odomètre (km)"] = int(kilometrage)
        infos["Type"] = version_complete
        infos["Modèle"] = modele
        infos["OEM"] = marque

    finally:
        driver.quit()


    return infos


def export_to_excel(df_to_write, gsheet):
    client = get_gspread_client()
    sheet_out = client.open("202505 - Courbes SoH")
    worksheet = sheet_out.worksheet(gsheet)
    worksheet.append_rows(df_to_write.values.tolist())
    print(f"Données écritent dans {gsheet}")


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
    
    infos_clean = pd.DataFrame(all_infos).T.dropna(subset='SoH')[["OEM","Modèle","Type","Année","Odomètre (km)","SoH", "lien"]]
    print(infos_clean.shape)
    export_to_excel(infos_clean, "Courbes OS")


if __name__ == "__main__":
    main()

