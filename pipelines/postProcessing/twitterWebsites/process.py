import logging
from ..helpers import Processor
from .cyphers import TwitterWebsiteCyphers
from datetime import datetime, timedelta
import re
import tqdm


class TwitterWebsitePostProcess(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = TwitterWebsiteCyphers()
        super().__init__("twitter-websites")
        self.cutoff = datetime.now() - timedelta(days=30)

    @staticmethod
    def extract_urls(text):
        url_pattern = re.compile(r"(https?://\S+)")
        return re.findall(url_pattern, text)

    def get_twitter_data(self):
        twitter_handles = self.cyphers.get_all_twitter()
        logging.info(f"Found {len(twitter_handles)} twitter handles")
        return twitter_handles

    def get_websites(self, twitter_handles):
        logging.info("Getting websites...")
        websites = []
        for handle, bio in tqdm.tqdm(twitter_handles):
            x = self.extract_urls(bio)
            for url in x:
                websites.append({"handle": handle, "url": url.lower()})

        logging.info(f"Found {len(websites)} websites")
        return websites

    def handle_ingestion(self, websites):
        urls = self.save_json_as_csv(websites, f"websites_{self.asOf}")
        self.cyphers.set_website(urls)

    def run(self):
        twitter_handles = self.get_twitter_data()
        websites = self.get_websites(twitter_handles)
        self.handle_ingestion(websites)


if __name__ == "__main__":
    processor = TwitterWebsitePostProcess()
    processor.run()
