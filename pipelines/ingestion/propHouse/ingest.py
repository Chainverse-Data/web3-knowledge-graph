from ..helpers import Ingestor
from .cyphers import PropHouseCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging


class PropHouseIngestor(Ingestor):
    def __init__(self):
        self.cyphers = PropHouseCyphers()
        super().__init__("prop-house")

    @staticmethod
    def clean_date(x):
        if x is None:
            return None
        return int(datetime.datetime.strptime(x[:10], "%Y-%m-%d").timestamp())

    def process_communities(self):
        communities = pd.DataFrame(self.scraper_data["communities"])
        communities.drop(columns=["auctions"], inplace=True)
        communities["createdDate"] = communities["createdDate"].apply(self.clean_date)
        communities["contractAddress"] = communities["contractAddress"].apply(lambda x: x.lower())
        communities["symbol"] = communities["name"]
        communities["decimal"] = [1]*len(communities)
        return communities

    def ingest_communities(self):
        communities = self.process_communities()
        urls = self.save_df_as_csv(communities, f"ingestor_communities_{self.asOf}")
        self.cyphers.create_communities(urls)
        self.cyphers.create_tokens(urls)
        self.cyphers.link_communities_tokens(urls)

    def process_auctions(self):
        auctions = pd.DataFrame(self.scraper_data["auctions"])
        auctions["communityId"] = auctions["community"].apply(lambda x: x["id"])
        auctions.drop(columns=["community"], inplace=True)
        auctions["startDt"] = auctions["startTime"].apply(self.clean_date)
        auctions["endDt"] = auctions["proposalEndTime"].apply(self.clean_date)
        auctions["createdDate"] = auctions["createdDate"].apply(self.clean_date)
        return auctions

    def ingest_auctions(self):
        auctions = self.process_auctions()
        urls = self.save_df_as_csv(auctions, f"ingestor_auctions_{self.asOf}")
        self.cyphers.create_auctions(urls)
        self.cyphers.link_auctions_communities(urls)

    def process_proposals(self):
        proposals = pd.DataFrame(self.scraper_data["proposals"])
        proposals["createdDate"] = proposals["createdDate"].apply(self.clean_date)
        proposals["address"] = proposals["address"].apply(lambda x: x.lower())
        return proposals

    def ingest_proposals(self):
        proposals = self.process_proposals()

        addresses = [{"address": x} for x in proposals["address"].unique()]
        urls = self.save_json_as_csv(addresses, f"ingestor_proposal_addresses_{self.asOf}")
        self.cyphers.create_wallets(urls)

        urls = self.save_df_as_csv(proposals, f"ingestor_proposals_{self.asOf}")
        self.cyphers.create_proposals(urls)
        self.cyphers.link_auction_proposals(urls)
        self.cyphers.link_proposal_wallets(urls)
        self.cyphers.link_proposals_entities(urls)

        proposals = proposals[proposals["winner"] == True]
        urls = self.save_df_as_csv(proposals, f"ingestor_proposals_winner_{self.asOf}")
        self.cyphers.add_winner_labels(urls)

    def process_votes(self):
        votes = pd.DataFrame(self.scraper_data["votes"])
        votes["createdDate"] = votes["createdDate"].apply(self.clean_date)
        votes["address"] = votes["address"].apply(lambda x: x.lower())
        return votes

    def ingest_votes(self):
        votes = self.process_votes()

        addresses = [{"address": x} for x in votes["address"].unique()]
        urls = self.save_json_as_csv(addresses, f"ingestor_vote_addresses_{self.asOf}")
        self.cyphers.create_wallets(urls)

        urls = self.save_df_as_csv(votes, f"ingestor_votes_{self.asOf}")
        self.cyphers.create_votes(urls)

    def run(self):
        self.ingest_communities()
        self.ingest_auctions()
        self.ingest_proposals()
        self.ingest_votes()

if __name__ == "__main__":
    ingestor = PropHouseIngestor()
    ingestor.run()
