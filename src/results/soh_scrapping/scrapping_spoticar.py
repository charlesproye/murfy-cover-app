# -*- coding: utf-8 -*-
from __future__ import annotations
import re
from pathlib import Path
import time
from typing import Optional, Dict
import pandas as pd
import numpy as np
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from core.sql_utils import get_connection
from core.gsheet_utils import load_excel_data, export_to_excel
from activation.config.mappings import mapping_vehicle_type

PATH_FILE = Path(__file__).parent / "link_unsuable.txt"
BASE_URL = "https://www.spoticar.fr"
SEARCH_URL = "https://www.spoticar.fr/voitures-occasion?page={page}&filters[0][energy]=electrique"

_DEF_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)
    
def make_driver():
    options = webdriver.ChromeOptions()
    options.add_argument(f"--user-agent={_DEF_UA}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")
    return webdriver.Chrome(options=options)


def get_car_links(url):
    driver = make_driver()
    try:
        driver.get(url)

        # Scroll progressif pour charger toutes les annonces
        last_height = 0
        while True:
            driver.execute_script("window.scrollBy(0, 1200);")
            time.sleep(1.2)
            new_height = driver.execute_script("return window.scrollY;")
            if new_height == last_height:
                break
            last_height = new_height

        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "a[href^='/voitures-occasion/']")
            )
        )

        elements = driver.find_elements(By.CSS_SELECTOR, "a[href^='/voitures-occasion/']")
        links = []
        for el in elements:
            href = el.get_attribute("href")
            if re.search(r"\d{5,1000}+$", href) is not None:
                links.append(href)

        return list(set(links))
    finally:
        driver.quit()


def get_all_car_links(max_pages=10):
    all_links = set()
    for page in range(1, max_pages + 1):
        url = SEARCH_URL.format(page=page)
        print(f"Page {page} : {url}")
        links = get_car_links(url)
        if not links:
            print("No car for the page")
            break
        before = len(all_links)
        all_links.update(links)
        after = len(all_links)
        print(f"{len(links)} link found")
        if after == before:
            break
    return list(all_links)



def _text_of(element) -> str:
    return re.sub(r"\s+", " ", element.text).strip()


def find_value_by_label(driver: webdriver.Chrome, label: str) -> Optional[str]:
    label_norm = label.lower()
    xpath_candidates = [
        f"//dt[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÉÈÀÙÂÊÎÔÛÄËÏÖÜÇ', 'abcdefghijklmnopqrstuvwxyzéèàùâêîôûäëïöüç'), '{label_norm}')]/following-sibling::*[1]",
        f"//*[self::div or self::span][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÉÈÀÙÂÊÎÔÛÄËÏÖÜÇ', 'abcdefghijklmnopqrstuvwxyzéèàùâêîôûäëïöüç'), '{label_norm}')]/following-sibling::*[1]",
        f"//th[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÉÈÀÙÂÊÎÔÛÄËÏÖÜÇ', 'abcdefghijklmnopqrstuvwxyzéèàùâêîôûäëïöüç'), '{label_norm}')]/following-sibling::td[1]",
    ]
    for xp in xpath_candidates:
        try:
            el = WebDriverWait(driver, 1).until(
                EC.presence_of_element_located((By.XPATH, xp))
            )
            txt = _text_of(el)
            if txt:
                return txt
        except TimeoutException:
            continue
    try:
        label_el = driver.find_element(
            By.XPATH,
            f"//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÉÈÀÙÂÊÎÔÛÄËÏÖÜÇ', 'abcdefghijklmnopqrstuvwxyzéèàùâêîôûäëïöüç'), '{label_norm}')]",
        )
        parent = label_el.find_element(By.XPATH, "..")
        sibs = parent.find_elements(By.XPATH, ".//*")
        if len(sibs) >= 2:
            for s in sibs[1:]:
                txt = _text_of(s)
                if txt and label_norm not in txt.lower():
                    return txt
    except Exception:
        pass
    return None


def extract_price(driver: webdriver.Chrome) -> Optional[str]:
    try:
        price_el = driver.find_element(By.CSS_SELECTOR, "div.total-price-value span")
        return _text_of(price_el)
    except Exception as e:
        return None


def extract_title_parts(driver: webdriver.Chrome) -> Dict[str, Optional[str]]:
    out = {"make": None, "modele": None, "version": None}
    try:
        h1 = driver.find_element(By.TAG_NAME, "h1")
        title = _text_of(h1)
    except Exception as e:
        title = driver.title or ""
    if title:
        parts = title.split()
        if parts:
            out["make"] = parts[0]
            if len(parts) > 1:
                out["modele"] = parts[1]
    return out


def scroll_slowly(driver: webdriver.Chrome, steps: int = 6, pause: float = 0.5):
    last = 0
    for i in range(steps):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * arguments[0]);", (i+1)/steps)
        time.sleep(pause)
        curr = driver.execute_script("return window.scrollY;")
        if curr == last:
            break
        last = curr

def add_link_to_unused_list(link):
    with open(PATH_FILE, "a", encoding="utf-8") as f:
        f.write(f"{link}\n")
        f.flush()
    

def get_spoticar_data(url: str, timeout: int = 20) -> Dict[str, Optional[str]]:
    driver = make_driver()
    try:
        driver.get(url)
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        scroll_slowly(driver)

        page_text = _text_of(driver.find_element(By.TAG_NAME, 'body'))

        soh = None
        soh_match = re.search(r"(?i)\bsoh\b\s*[:\-]?\s*(\d{1,3})\s*%", page_text)
        if soh_match:
            soh = soh_match.group(1)
        else:
            soh_match2 = re.search(r"(?i)(sant[eé] de (la )?batterie|[ée]tat de sant[eé]).{0,20}?(\d{1,3})\s*%", page_text)
            if soh_match2:
                soh = soh_match2.group(3)

        if not soh:
            print(url, "SoH not found")
            add_link_to_unused_list(url)

        try:
            odometer_el = driver.find_element(By.CSS_SELECTOR, "div.tag.field_vo_mileage")
            odometer = _text_of(odometer_el)
            odometer = re.sub(r"[^\d]", "", odometer)
        except Exception:
            odometer = find_value_by_label(driver, "Kilométrage")
            odometer = re.sub(r"[^\d]", "", odometer) if odometer else None
            if not odometer:
                print(url, "Odometer not found")

        try:
            wltp = find_value_by_label(driver, "Autonomie électrique en cycle combiné")
            wltp = re.sub(r"[^\d]", "", wltp) if wltp else None
            if not wltp:
                print(url, "WLTP not found")
                add_link_to_unused_list(url)
        except Exception:
            wltp = None
            print(url, "WLTP not found")

        try:
            price = extract_price(driver)
            price = re.sub(r"[^\d]", "", price) if price else None
            if not price:
                print(url, "Price not found")
        except Exception:
            price = None
            print(url, "Price not found")

        return {
            "lien": url,
            "SoH": soh,
            "Odomètre (km)": odometer,
            "WLTP": wltp,
            "price": price,
            "OEM": find_value_by_label(driver, "make"),
            "Modèle": find_value_by_label(driver, "Modèle"),
            "Type": find_value_by_label(driver, "Version"),
            "Année": find_value_by_label(driver, "Année"),
            "battery_capacity": None
        }

    except Exception as e:
        print(url, f"Erreur fatale : {e}")
        return None

    finally:
        driver.quit()



def main():
    car_links = get_all_car_links(max_pages=1)
    data_sheet = load_excel_data("Courbes de tendance", "Courbes OS")
    df_sheet = pd.DataFrame(columns=data_sheet[0,:7], data=data_sheet[1:,:7])
    try:
        with open(PATH_FILE, "r", encoding="utf-8") as f:
            link_already_try = {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        link_already_try = set()
    links_not_fetch = set(car_links) - set(df_sheet['lien']) - link_already_try

    print(f"Number of link to scrappe = {len(links_not_fetch)}")
    infos = {}

    for i, link in enumerate(tqdm(links_not_fetch, desc="Scraping", unit="link"), start=1):
        try:
            infos[i] = get_spoticar_data(link)
        except Exception as e:
            print(f"[{i}] Erreur sur le lien {link} : {e}")
    
    infos_clean = pd.DataFrame(infos).T.dropna(subset='SoH')[["OEM", "Modèle", "Type", "Année", "Odomètre (km)", "SoH", "lien", "battery_capacity", "price", "WLTP"]]
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
