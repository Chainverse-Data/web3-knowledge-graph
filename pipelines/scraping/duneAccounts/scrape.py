from tqdm import tqdm
import logging
from ..helpers import Scraper
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import *
from selenium.webdriver.common.by import By
import functools
import operator
import pandas as pd
from bs4 import BeautifulSoup
import lxml
import tkinter as tk
import time

DEBUG = os.environ.get("DEBUG", False)


class DuneScraper(Scraper):
    def __init__(self, bucket_name="dune-accounts"):
        super().__init__(bucket_name)
        self.proxy_host = "proxy.crawlera.com"
        self.proxy_port = "8011"
        self.proxy_auth = os.environ.get("PROXY") + ":"
        self.root_url = "https://dune.com/"
        self.wizard_url = "https://dune.com/browse/wizards"
        self.CHROMEDRIVER_PATH = "/Users/rohan/documents/Diamond/chainverse/account-ingest/chromedriver"
        self.scroll_pause_time = 10
        options = Options()
        options.proxy = Proxy(
            {
                "proxyType": ProxyType.MANUAL,
                "httpProxy": "http://{}@{}:{}/".format(self.proxy_auth, self.proxy_host, self.proxy_port),
                "sslProxy": "http://{}@{}:{}/".format(self.proxy_auth, self.proxy_host, self.proxy_port),
            }
        )
        self.driver = webdriver.Chrome(self.CHROMEDRIVER_PATH, options=options)

    def get_wizards(self, pages=1000):
        results = []
        self.driver.get(self.wizard_url)
        time.sleep(self.scroll_pause_time)

        for i in range(1, pages + 1):
            s = self.driver.page_source
            soup = BeautifulSoup(s, "lxml")
            wizards = soup.find_all("li", {"class": "entries_entry__ZszyW"})
            for wizard in wizards:
                results.append(wizard.find("a")["href"][1:])
            logging.info(f"page {i} done, {len(results)} wizards found")
            try:
                self.driver.find_element_by_xpath(f"//button[contains(text(), '{i+1}')]").click()
            except:
                logging.info("no more pages")
            time.sleep(self.scroll_pause_time)

        return results

    def get_users(self, wizards):
        data = []

        for user in tqdm(wizards):
            url = user["url"]
            self.driver.get(url)
            time.sleep(0.25)
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            profile = soup.find("div", {"class": "UserProfile_profile-content__YQuA5"})
            if not profile:
                continue
            left = profile.find("div", {"class": "UserDetails_user-details__PmbFa"})
            right = profile.find("ul", {"class": "UserProfile_rightList__I0xqZ"})

            entry = {}
            entry["name"] = user["name"]
            entry["url"] = url

            if right:
                right_li = right.find_all("li")
                for i in range(len(right_li)):
                    if i == 0:
                        entry["stars"] = right_li[i].find("span").text.split(" ")[0]
                    elif i == 1:
                        entry["queries"] = right_li[i].find("span").text.split(" ")[0]
                    elif i == 2:
                        entry["dashboards"] = right_li[i].find("span").text.split(" ")[0]
                    elif i == 3:
                        entry["contracts"] = right_li[i].find("span").text.split(" ")[0]

            if left:
                if left.find("div", {"class": "UserDetails_bio__s_u8r"}):
                    entry["description"] = left.find("div", {"class": "UserDetails_bio__s_u8r"}).find("span").text

                left_div = left.find_all("div", {"class": "row_flex__5rrKF"})
                for i in range(len(left_div)):
                    if left_div[i].find("div", {"class": "UserDetails_teams__2Dwos"}):
                        teams = left_div[i].find_all("a")
                        entry["teams"] = [team["href"][1:] for team in teams]
                    elif left_div[i].find("a") and left_div[i].find("a").has_attr("aria-label"):
                        entry[left_div[i].find("a")["aria-label"]] = left_div[i].find("a")["href"]
                    elif left_div[i].find("span") and left_div[i].find("span").has_attr("aria-label"):
                        entry[left_div[i].find("span")["aria-label"]] = left_div[i].find("span").text

            try:
                button = self.driver.find_element(By.CLASS_NAME, value="CopyToClipboardButton_button__z6LLS")
                button.click()
                root = tk.Tk()
                entry["address"] = root.clipboard_get()
            except:
                pass

            data.append(entry)

        logging.info(f"found {len(data)} users")
        return data

    def get_teams(self, teams):
        data = []

        for team in tqdm(teams):
            url = team["url"]
            self.driver.get(url)
            time.sleep(0.25)
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            profile = soup.find("div", {"class": "TeamProfile_profile-content__grkZj"})
            if not profile:
                continue
            left = profile.find("div", {"class": "TeamProfile_team-details__wqkuy"})
            right = profile.find("ul", {"class": "TeamStats_rightList__IJrmS"})

            entry = {}

            if right:
                right_li = right.find_all("li")
                if len(right_li) > 0:
                    entry["stars"] = right_li[0].find("span").text.split(" ")[0]
                if len(right_li) > 1:
                    entry["queries"] = right_li[1].find("span").text.split(" ")[0]
                if len(right_li) > 2:
                    entry["dashboards"] = right_li[2].find("span").text.split(" ")[0]
                if len(right_li) > 3:
                    entry["contracts"] = right_li[3].find("span").text.split(" ")[0]

            if left:
                if left.find("div", {"class": "TeamProfile_bio__gBhI6"}):
                    entry["description"] = left.find("div", {"class": "TeamProfile_bio__gBhI6"}).find("span").text

                left_div = left.find_all("div", {"class": "row_flex__5rrKF"})
                for i in range(len(left_div)):
                    if left_div[i].find("a") and left_div[i].find("a").has_attr("aria-label"):
                        entry[left_div[i].find("a")["aria-label"]] = left_div[i].find("a")["href"]
                    elif left_div[i].find("span") and left_div[i].find("span").has_attr("aria-label"):
                        entry[left_div[i].find("span")["aria-label"]] = left_div[i].find("span").text

            entry["name"] = team["name"]
            entry["url"] = url
            data.append(entry)

        logging.info(f"found {len(data)} teams")
        return data

    def run(self):
        wizards = self.get_wizards()
        wizards = [{"name": wizard, "url": f"https://dune.com/{wizard}"} for wizard in set(wizards)]
        self.save_json(self.bucket_name, f"wizards-{self.asOf}", wizards)
        users = self.get_users(wizards)

        team_list = functools.reduce(operator.iconcat, [x.get("teams", []) for x in users], [])
        team_list = [{"name": team, "url": f"https://dune.com/{team}"} for team in set(team_list)]
        teams = self.get_teams(team_list)

        self.data["users"] = users
        self.data["teams"] = teams
        self.save_data()
        self.save_metadata()


if __name__ == "__main__":
    S = DuneScraper()
    S.run()
