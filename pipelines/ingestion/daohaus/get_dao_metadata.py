import selenium
import time
import pandas as pd
import os
import sys
from pathlib import Path
from selenium import webdriver
from dotenv import load_dotenv
from webdriver_manager.chrome import ChromeDriverManager

sys.path.append(str(Path(__file__).resolve().parents[2]))
from ingestion.helpers.s3 import *
from ingestion.helpers.graph import ChainverseGraph

load_dotenv()

uri = os.getenv("NEO_URI")
username = os.getenv("NEO_USERNAME")
password = os.getenv("NEO_PASSWORD")
conn = ChainverseGraph(uri, username, password)


def get_daos(conn):

    query = """
            MATCH (d:DaoHaus:Dao) WHERE d.name IS NULL RETURN d
            """

    daos = conn.query(query)
    daos = [i["d"] for i in daos]
    return daos


daos = get_daos(conn)
daos = [dict(dao) for dao in daos]

chain_dict = {1: "0x1", 100: "0x64"}

driver = webdriver.Chrome(ChromeDriverManager().install())
browser = driver


def grab_dao_metadata(daos):
    # this is for me tho
    # nav to home
    browser.get("https://app.daohaus.club/explore")
    time.sleep(1)
    # deal with bullshit wallet connect popup
    browser.find_element_by_xpath("//*[@id='walletconnect-qrcode-modal']/div/div[2]/a[2]").click()
    browser.find_element_by_xpath("//*[@id='walletconnect-qrcode-modal']/div/div[1]/div").click()
    time.sleep(1)
    list_dicts = list()
    for idx, row in enumerate(daos):
        address = row["daohausId"]
        url = "https://app.daohaus.club/dao/" + chain_dict[row["chainId"]] + "/" + address + "/settings"
        print(url)
        try:
            browser.get(url)
            time.sleep(2)
            browser.find_element_by_xpath('//*[@id="walletconnect-qrcode-modal"]/div/div[2]/a[2]').click()
            time.sleep(1)
            browser.find_element_by_xpath('//*[@id="walletconnect-qrcode-close"]/div[2]').click()
            dao_dict = {}
            name = browser.find_element_by_xpath('//*[@id="root"]/div/div[3]/div[2]/div/div/div[4]/div[1]/div').text
            print(name)
            elems = browser.find_elements_by_xpath("//a[@href]")
            socials = [i.get_attribute("href") for i in elems]
            socials = [i for i in socials if not "daohaus" in i]
            socials = [i for i in socials if not "blockscout" in i]
            socials = [i for i in socials if not "bridge.walletconnect" in i]
            socials = [i for i in socials if not "tokenary" in i]
            socials = [i for i in socials if not "wallet3://wc?uri=wc%3Af24ae9cc" in i]
            socials = [i for i in socials if not "wallet.ambire" in i]
            socials = [i for i in socials if not "etherscan" in i]
            socials = [i for i in socials if not "encrypted" in i]
            socials = [i for i in socials if not "ledger" in i]
            socials = [i for i in socials if not "infinitywallet" in i]
            socials = [i for i in socials if not "nowdaoit" in i]
            dao_dict["contract"] = address
            dao_dict["identifiers"] = socials
            dao_dict["name"] = name
            dao_dict["twitter"] = [x for x in socials if "twitter" in x]
            dao_dict["forum"] = [x for x in socials if "forum" in x]
            dao_dict["discord"] = [x for x in socials if "discord" in x]
            dao_dict["blog"] = [x for x in socials if "medium" in x or "blog" in x or "mirror" in x or "substack" in x]
            dao_dict["documentation"] = [x for x in socials if "docs" in x or "gitbook" in x]
            dao_dict["opensea"] = [x for x in socials if "opensea" in x]
            dao_dict["gallery"] = [x for x in socials if "gallery" in x]
            dao_dict["github"] = [x for x in socials if "github" in x]
            list_dicts.append(dao_dict)
            daof = pd.DataFrame(list_dicts)
            daof.to_csv("ingestion/daohaus/daohaus_checkpoint_three.csv")
        except Exception as e:
            print(e)
            pass


if __name__ == "__main__":
    grab_dao_metadata(daos)
