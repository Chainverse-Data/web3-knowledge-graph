import logging
from ..helpers import Ingestor
from .cyphers import EnsCyphers
import datetime
import pandas


class EnsIngestor(Ingestor):
    def __init__(self):
        self.cyphers = EnsCyphers()
        super().__init__("ens")
        self.metadata["last_date_ingested"] = self.end_date
        if isinstance(self.end_date, datetime.datetime):
            self.metadata["last_date_ingested"] = self.end_date.strftime("%Y-%m-%d")

    def ingest_ens(self):
        logging.info("Ingesting ens...")

        df = pandas.DataFrame(self.scraper_data["ens"]).drop_duplicates(subset=["address", "name"])
        df = df[
            (df["name"].notna())
            & (df["address"].notna())
            & (df["token_id"].notna())
            & (df["name"] != "")
            & (df["address"] != "")
            & (df["token_id"] != "")
        ]
        df = df[df["name"].apply(self.utf8len)].rename(columns={"token_id": "tokenId"})
        self.scraper_data["ens"] = df.to_dict("records")
        urls = self.s3.save_json_as_csv(self.scraper_data["ens"], self.bucket_name, f"ingestor_ens_{self.asOf}")
        self.cyphers.create_or_merge_ens_items(urls)
        self.cyphers.link_ens(urls)

        df = pandas.DataFrame(self.scraper_data["primary"]).drop_duplicates(subset=["address", "name"])
        df = df[(df["name"].notna()) & (df["address"].notna()) & (df["name"] != "") & (df["address"] != "")]
        df = df[df["name"].apply(self.utf8len)]
        self.scraper_data["primary"] = df.to_dict("records")
        urls = self.s3.save_json_as_csv(self.scraper_data["primary"], self.bucket_name, f"ingestor_primary_{self.asOf}")
        self.cyphers.add_primary_property(urls)

    @staticmethod
    def utf8len(s):
        return len(s.encode("utf-8")) <= 8164

    def run(self):
        self.ingest_ens()
        self.save_metadata()


if __name__ == "__main__":
    ens_ingestor = EnsIngestor()
    ens_ingestor.run()
