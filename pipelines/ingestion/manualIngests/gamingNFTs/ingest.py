
import pandas as pd
from .cypher import GamingNFTCyphers
from ...helpers import Ingestor

class GamingNFTIngestor(Ingestor):
    def __init__(self, bucket_name="manual-gaming-nfts"):
        self.cyphers = GamingNFTCyphers()
        super().__init__(bucket_name, load_data=False)

    def clean_chain_name(self, x):
            if x == "mainnet":
                return 1
            elif x == "polygon":
                return 137
            else:
                return 1

    def prepare_data(self):
        data = pd.read_csv("pipelines/ingestion/manual-ingests/gamingNFTs/data/gaming.csv")
        wallets = [{"address": wallet} for wallet in data["address"].unique()]
        data["network"] = data["network"].apply(self.clean_chain_name)
        eth_nfts = [{"contractAddress": address} for address in data[data["network"] == 1]["contract"].unique()]
        poly_nfts = [{"contractAddress": address} for address in data[data["network"] == 137]["contract"].unique()]
        return data, wallets, eth_nfts, poly_nfts

    def ingest_nft_data(self):
        data, wallets, eth_nfts, poly_nfts = self.prepare_data()
        urls = self.save_json_as_csv(wallets, self.bucket_name, f"ingestor_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)

        urls = self.save_json_as_csv(eth_nfts, self.bucket_name, f"ingestor_eth_tokens_{self.asOf}")
        self.cyphers.queries.create_or_merge_tokens(urls, "ERC721")

        urls = self.save_json_as_csv(poly_nfts, self.bucket_name, f"ingestor_poly_tokens_{self.asOf}")
        self.cyphers.queries.create_or_merge_tokens(urls, "ERC721", chain_id=137)

        urls = self.save_json_as_csv(data, self.bucket_name, f"ingestor_holdings_{self.asOf}")
        self.cyphers.link_or_merge_holdings(urls)

    def run(self):
        self.ingest_nft_data()

if __name__ == "__main__":
    I = GamingNFTIngestor()
    I.run()
