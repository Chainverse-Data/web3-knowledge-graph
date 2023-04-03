
import pandas as pd
from .cypher import ENSTwitterCyphers
from ...helpers import Ingestor

class ENSTwitterIngestor(Ingestor):
    def __init__(self, bucket_name="manual-ens-twitter"):
        self.cyphers = ENSTwitterCyphers()
        super().__init__(bucket_name, load_data=False)

    def prepare_twitter_ens_data(self):
        data = pd.read_csv("pipelines/ingestion/manual-ingests/ens-twitter/data/ens_names.csv")
        ens_names = data[~data["ens_name"].isna()]
        ens_names["name"] = ens_names["ens_name"]
        wallets = data[~data["wallet"].isna()]
        wallets["address"] = wallets["wallet"]
        return ens_names, wallets

    def ingest_twitter_ens_data(self):
        ens_names, wallets = self.prepare_twitter_ens_data()
        print(wallets)
        urls = self.save_df_as_csv(ens_names, f"ingestor_ens_names_{self.asOf}")
        self.cyphers.queries.create_or_merge_twitter(urls)
        self.cyphers.queries.create_or_merge_ens_alias(urls)
        self.cyphers.link_or_merge_ens_names(urls)

        urls = self.save_df_as_csv(wallets, f"ingestor_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)
        self.cyphers.link_or_merge_wallets(urls)

    def run(self):
        self.ingest_twitter_ens_data()

if __name__ == "__main__":
    I = ENSTwitterIngestor()
    I.run()
