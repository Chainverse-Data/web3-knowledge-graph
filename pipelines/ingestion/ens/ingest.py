import logging
from ..helpers import Ingestor
from .cyphers import EnsCyphers
import datetime


class EnsIngestor(Ingestor):
    def __init__(self):
        self.cyphers = EnsCyphers()
        super().__init__("ens")
        self.metadata["last_date_ingested"] = self.end_date
        if isinstance(self.start_date, datetime.datetime):
            self.metadata["last_date_ingested"] = self.end_date.strftime("%Y-%m-%d")

    def ingest_ens(self):
        logging.info("Ingesting ens...")
        urls = self.s3.save_json_as_csv(self.scraper_data["ens"], self.bucket_name, f"ingestor_ens_{self.asOf}")
        self.cyphers.create_or_merge_ens(urls)
        self.cyphers.link_ens(urls)

    def run(self):
        self.ingest_ens()
        self.save_metadata()


if __name__ == "__main__":
    ens_ingestor = EnsIngestor()
    ens_ingestor.run()
