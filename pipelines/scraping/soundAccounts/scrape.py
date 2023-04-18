from tqdm import tqdm
import logging
from ..helpers import Scraper
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import *
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import lxml
import tkinter as tk
import time

DEBUG = os.environ.get("DEBUG", False)


class SoundScraper(Scraper):
    def __init__(self, bucket_name="sound-accounts"):
        super().__init__(bucket_name)
        self.proxy_host = "proxy.crawlera.com"
        self.proxy_port = "8011"
        self.proxy_auth = os.environ.get("PROXY", "") + ":"
        self.root_url = "https://www.sound.xyz"
        self.scroll_pause_time = 1
        options = Options()
        options.add_argument("start-maximized")
        options.add_argument("disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument('--no-sandbox')
        options.add_argument('--headless')
        options.proxy = Proxy(
            {
                "proxyType": ProxyType.MANUAL,
                "httpProxy": "http://{}@{}:{}/".format(self.proxy_auth, self.proxy_host, self.proxy_port),
                "sslProxy": "http://{}@{}:{}/".format(self.proxy_auth, self.proxy_host, self.proxy_port),
            }
        )
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def get_sound_feed(self):
        self.driver.get(self.root_url)
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        idx = 0

        with tqdm(desc="Scrolling feed") as pbar:
            while True:
                idx += 1
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                try:
                    self.driver.find_element(by=By.XPATH, value="//button[contains(text(), 'Load more')]").click()
                except:
                    pass
                time.sleep(self.scroll_pause_time)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
                pbar.update(1)

        s = self.driver.page_source
        soup = BeautifulSoup(s, "lxml")
        return soup

    def parse(self, soup):
        tiles = soup.find_all("div", {"class": "c-dTraXF"})
        artists = set()
        for tile in tiles:
            artists.add(tile.find("div", {"class": "c-hECfPj"}).find("a")["href"])
        logging.info(f"Found {len(artists)} artists")

        results = []
        for artist in tqdm(artists, desc="Scraping artists"):
            url = self.root_url + artist
            self.driver.get(url)
            time.sleep(self.scroll_pause_time)
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            profile = soup.find("div", {"class": "c-bMUoQk"})
            if not profile:
                continue
            entry = {}
            entry["url"] = url
            entry["name"] = profile.find("h1").text
            links = profile.find("div", {"class": "c-fvtaoQ"}).find_all("a")
            for link in links:
                if "twitter" in link["href"]:
                    entry["twitter"] = link["href"]

            try:
                button = self.driver.find_element(By.CLASS_NAME, value="copy-address-button")
                button.click()
                root = tk.Tk()
                entry["address"] = root.clipboard_get()
            except:
                pass
            results.append(entry)

        logging.info(f"Scraped {len(results)} artists")
        return results

    def run(self):
        soup = self.get_sound_feed()

        artists = self.parse(soup)
        self.data["artists"] = artists
        self.save_data()
        self.save_metadata()


if __name__ == "__main__":
    S = SoundScraper()
    S.run()
