from ..helpers import Ingestor
from .cyphers import DaoHausCyphers
import pandas as pd

class DaoHausIngestor(Ingestor):
    def __init__(self, bucket_name="daohaus"):
        self.cyphers = DaoHausCyphers()
        super().__init__(bucket_name)
        self.chainIds = {
            "mainnet": 1,
            "xdai": 100,
            "optimism":10
        }

    def prepare_daos_data(self):
        data = {"daos": [], "tokens": [], "summoners":[]}
        for chain in self.scraper_data:
            dao_meta = {}
            token_balances = {}
            for dao in self.scraper_data[chain]["daoMetas"]:
                dao_meta[dao["id"]] = dao
            
            for balance in self.scraper_data["tokenBalances"]:
                daoId = balance["moloch"]["id"]
                token_address = balance["token"]["tokenAddress"]
                if daoId not in token_balances:
                    token_balances[daoId] = {}
                token_balances[daoId][token_address] = balance["tokenBalance"]
            
            for dao in self.scraper_data[chain]["moloches"]:
                tmp = dao
                tmp["chain"] = chain
                tmp["chainId"] = self.chainIds[chain]
                tmp["title"] = dao_meta[dao["id"]]["title"]
                tmp["version"] = dao_meta[dao["id"]]["version"]
                del tmp["approvedTokens"] 
                del tmp["tokens"]
                data["daos"].append(tmp)
                data["summoners"].append({"daoId": dao["id"], "address": dao["summoner"]})
                for token in dao["tokens"]:
                    tmp = {
                        "daoId": dao["id"],
                        "contractAddress": token["tokenAddress"],
                        "symbol": token["symbol"],
                        "decimal": token["decimals"],
                        "balance": token_balances[dao["id"]][token["tokenAddress"]],
                        "balanceNumber": float(token_balances[dao["id"]][token["tokenAddress"]])/float(token["decimals"])
                    }
                    data["tokens"].append(tmp)
        daos_df = pd.DataFrame().from_dict(data["daos"])
        tokens_df = pd.DataFrame().from_dict(data["tokens"])
        summoners_df = pd.DataFrame().from_dict(data["summoners"])
        return daos_df, tokens_df, summoners_df

    def prepare_members_data(self):
        data = {"members":[], "wallets":[]}
        for chain in self.scraper_data:
            for member in self.scraper_data[chain]["members"]:
                data["members"].append(member)
                data["wallets"].append({"address": member["memberAddress"]})
        members_df = pd.DataFrame().from_dict(data["members"])
        wallets_df = pd.DataFrame().from_dict(data["members"])
        wallets_df["address"].drop_duplicates(inplace=True, ignore_index=True)
        return members_df, wallets_df

    def prepare_proposals_data(self):
        data = {"proposals":[], "wallets": []}
        for chain in self.scraper_data:
            for proposal in self.scraper_data[chain]["proposals"]:
                tmp = proposal
                tmp["id"] = f"{proposal['molochAddress']}-proposal-{proposal['proposalId']}"
                data["proposals"].append(tmp)
                data["wallets"].append({"address": proposal["applicant"]})
                data["wallets"].append({"address": proposal["proposer"]})
                data["wallets"].append({"address": proposal["sponsor"]})
                data["wallets"].append({"address": proposal["processor"]})
        proposals_df = pd.DataFrame().from_dict(data["proposals"]).drop_duplicates(inplace=True, ignore_index=True)
        wallets_df = pd.DataFrame().from_dict(data["wallets"]).drop_duplicates(inplace=True, ignore_index=True)
        return proposals_df, wallets_df
                
    def prepare_votes_data(self):
        data = []
        voteMap = {1: "yes", 0: "no"}
        for chain in self.scraper_data:
            for vote in self.scraper_data[chain]["votes"]:
                tmp = vote
                tmp["proposalId"] = f"{vote['molochAddress']}-proposal-{vote['proposal']['proposalId']}"
                tmp["vote"] = voteMap.get(tmp["uintVote"], "ukn")
                del tmp["proposal"]
                data.append(tmp)
        votes_df = pd.DataFrame().from_dict(data).drop_duplicates(inplace=True, ignore_index=True)
        return votes_df

    def ingest_daos(self):
        daos_df, tokens_df, summoners_df = self.prepare_daos_data()
        self.s3.sa


    def ingest_members(self):

    def ingest_votes(self):

    def ingest_tokens(self):

    def ingest_proposals(self):

    def run(self):
        self.ingest_daos()
        self.ingest_tokens()
        self.ingest_members()
        self.ingest_proposals()
        self.ingest_votes()
        self.save_metadata()
