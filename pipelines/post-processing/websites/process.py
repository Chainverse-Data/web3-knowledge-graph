import logging
from ..helpers import Processor
from .cyphers import WebsiteCyphers
from datetime import datetime, timedelta
import os
import re
import tqdm


class WebsitePostProcess(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = WebsiteCyphers()
        super().__init__("websites")
        self.cutoff = datetime.now() - timedelta(days=30)
        self.full_job = bool(os.environ.get("FULL_WEBSITE_JOB", False))

    @staticmethod
    def extract_urls(text):
        url_pattern = re.compile(r"(https?://\S+)")
        return re.findall(url_pattern, text)

    def get_twitter_data(self):
        if self.full_job:
            twitter_handles = self.cyphers.get_all_twitter()
        else:
            twitter_handles = self.cyphers.get_recent_twitter(self.cutoff)
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
        urls = self.s3.save_json_as_csv(websites, self.bucket_name, f"websites_{self.asOf}")
        self.cyphers.create_website(urls)
        self.cyphers.link_twitter_website(urls)

    def run(self):
        twitter_handles = self.get_twitter_data()
        websites = self.get_websites(twitter_handles)
        self.handle_ingestion(websites)


if __name__ == "__main__":
    processor = WebsitePostProcess()
    processor.run()
