import json
import logging

from tqdm import tqdm
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
            "optimism":10,
            "polygon":137
        }

    def prepare_daos_and_tokens_data(self):
        logging.info("Preparing Daos and tokens data")
        data = {"daos": [], "tokens": [], "summoners":[]}
        for chain in tqdm(self.scraper_data, position=0):
            dao_meta = {}
            token_balances = {}
            for dao in tqdm(self.scraper_data[chain]["daoMetas"], position=1):
                dao_meta[dao["id"]] = dao
            
            for balance in tqdm(self.scraper_data[chain]["tokenBalances"], position=1):
                daoId = balance["moloch"]["id"]
                token_address = balance["token"]["tokenAddress"]
                if daoId not in token_balances:
                    token_balances[daoId] = {}
                token_balances[daoId][token_address] = balance["tokenBalance"]
            
            for dao in tqdm(self.scraper_data[chain]["moloches"], position=1):
                tmp = dao.copy()
                tmp["chain"] = chain
                tmp["chainId"] = self.chainIds[chain]
                tmp["title"] = dao_meta.get(dao["id"], {"title": ""})["title"]
                tmp["version"] = dao_meta.get(dao["id"], {"version": None})["version"]
                del tmp["approvedTokens"] 
                del tmp["tokens"]
                data["daos"].append(tmp)
                data["summoners"].append({"daoId": dao["id"], "address": dao["summoner"]})
                for token in dao["tokens"]:
                    balance = token_balances.get(dao["id"], {token["tokenAddress"]: None})
                    if token["tokenAddress"] in balance:
                        balance = balance[token["tokenAddress"]]
                    else:
                        balance = None
                    tmp = {
                        "daoId": dao["id"],
                        "contractAddress": token["tokenAddress"],
                        "symbol": token["symbol"],
                        "decimal": token["decimals"],
                        "balance": balance,
                    }
                    if tmp["balance"] and token["decimals"]:
                        if float(token["decimals"]) != 0:
                            tmp["balanceNumber"] = float(token_balances[dao["id"]][token["tokenAddress"]])/float(token["decimals"])
                        else:
                            tmp["balanceNumber"] = float(token_balances[dao["id"]][token["tokenAddress"]])
                    else:
                        tmp["balanceNumber"] = None

                    data["tokens"].append(tmp)
        daos_df = pd.DataFrame().from_dict(data["daos"])
        tokens_df = pd.DataFrame().from_dict(data["tokens"])
        summoners_df = pd.DataFrame().from_dict(data["summoners"])
        logging.info(f"Data prepared: \ndaos:{len(daos_df)}\ntokens: {len(tokens_df)}\nsummoners: {len(summoners_df)}")
        return daos_df, tokens_df, summoners_df

    def prepare_members_data(self):
        logging.info("Preparing members data")
        data = {"members":[], "wallets":[]}
        for chain in tqdm(self.scraper_data, position=0):
            for member in tqdm(self.scraper_data[chain]["members"], position=1):
                data["members"].append(member)
                data["wallets"].append({"address": member["memberAddress"]})
        members_df = pd.DataFrame().from_dict(data["members"]).drop_duplicates(ignore_index=True)
        wallets_df = pd.DataFrame().from_dict(data["wallets"]).drop_duplicates(ignore_index=True)
        logging.info(f"Data prepared: \nmembers:{len(members_df)}\nwallets: {len(wallets_df)}")
        return members_df, wallets_df

    def prepare_proposals_data(self):
        logging.info("Preparing proposals data")
        data = {"proposals":[], "wallets": []}
        for chain in tqdm(self.scraper_data, position=0):
            for proposal in tqdm(self.scraper_data[chain]["proposals"], position=1):
                tmp = proposal
                tmp["id"] = f"{proposal['molochAddress']}-proposal-{proposal['proposalId']}"
                del tmp["details"]
                del tmp["minionExecuteActionTx"]
                if proposal["tributeTokenDecimals"]:
                    if float(proposal["tributeTokenDecimals"]):
                        tmp["tributeAmount"] = int(proposal["tributeOffered"])/float(proposal["tributeTokenDecimals"])
                    else:
                        tmp["tributeAmount"] = int(proposal["tributeOffered"])
                else:
                    tmp["tributeAmount"] = None

                if proposal["paymentTokenDecimals"]:
                    if float(proposal["paymentTokenDecimals"]) != 0:
                        tmp["payementAmount"] = int(proposal["paymentRequested"])/float(proposal["paymentTokenDecimals"])
                    else:
                        tmp["payementAmount"] = int(proposal["paymentRequested"])
                else:
                    tmp["payementAmount"] = None
                
                data["proposals"].append(tmp)

                data["wallets"].append({"address": proposal["applicant"]})
                data["wallets"].append({"address": proposal["proposer"]})
                data["wallets"].append({"address": proposal["sponsor"]})
                data["wallets"].append({"address": proposal["processor"]})
        proposals_df = pd.DataFrame().from_dict(data["proposals"]).drop_duplicates(ignore_index=True)
        wallets_df = pd.DataFrame().from_dict(data["wallets"]).drop_duplicates(ignore_index=True)
        logging.info(f"Data prepared: \nproposals:{len(proposals_df)}\nwallets: {len(wallets_df)}")
        return proposals_df, wallets_df
                
    def prepare_votes_data(self):
        logging.info("Preparing votes data")
        data = []
        voteMap = {1: "yes", 0: "no"}
        for chain in tqdm(self.scraper_data, position=0):
            for vote in tqdm(self.scraper_data[chain]["votes"], position=1):
                tmp = vote
                tmp["proposalId"] = f"{vote['molochAddress']}-proposal-{vote['proposal']['proposalId']}"
                tmp["vote"] = voteMap.get(tmp["uintVote"], "ukn")
                del tmp["proposal"]
                data.append(tmp)
        votes_df = pd.DataFrame().from_dict(data).drop_duplicates(ignore_index=True)
        logging.info(f"Data prepared: \nvotes:{len(votes_df)}")
        return votes_df

    def ingest_daos_and_tokens(self):
        logging.info("Ingesting daos and tokens...")
        daos_df, tokens_df, summoners_df = self.prepare_daos_and_tokens_data()
        
        urls = self.save_df_as_csv(daos_df, self.bucket_name, f"ingestor_daos_{self.asOf}")
        self.cyphers.create_or_merge_daos(urls)
        
        urls = self.save_df_as_csv(summoners_df, self.bucket_name, f"ingestor_summoners_{self.asOf}")
        self.cyphers.create_or_merge_wallets(urls)
        self.cyphers.link_or_merge_summoners(urls)

        urls = self.save_df_as_csv(tokens_df, self.bucket_name, f"ingestor_tokens_{self.asOf}")
        self.cyphers.create_or_merge_tokens(urls)
        self.cyphers.link_or_merge_tokens(urls)

    def ingest_members(self):
        logging.info("Ingesting members...")
        members_df, wallets_df = self.prepare_members_data()

        urls = self.save_df_as_csv(wallets_df, self.bucket_name, f"ingestor_members_wallets_{self.asOf}")
        self.cyphers.create_or_merge_wallets(urls)

        urls = self.save_df_as_csv(members_df, self.bucket_name, f"ingestor_members_{self.asOf}")
        self.cyphers.link_or_merge_members(urls)

    def ingest_proposals(self):
        logging.info("Ingesting proposals...")
        proposals_df, wallets_df = self.prepare_proposals_data()

        urls = self.save_df_as_csv(wallets_df, self.bucket_name, f"ingestor_proposals_wallets_{self.asOf}")
        self.cyphers.create_or_merge_wallets(urls)

        urls = self.save_df_as_csv(proposals_df, self.bucket_name, f"ingestor_proposals_{self.asOf}")
        self.cyphers.create_or_merge_proposals(urls)
        self.cyphers.link_or_merge_proposals(urls)
        self.cyphers.link_or_merge_applicants(urls)
        self.cyphers.link_or_merge_proposers(urls)
        self.cyphers.link_or_merge_processors(urls)
        self.cyphers.link_or_merge_sponsors(urls)

        tmp_df = proposals_df[proposals_df["tributeOffered"] != "0"]
        urls = self.save_df_as_csv(tmp_df, self.bucket_name, f"ingestor_tributes_{self.asOf}")
        self.cyphers.link_or_merge_tributes(urls)

        tmp_df = proposals_df[proposals_df["paymentRequested"] != "0"]
        urls = self.save_df_as_csv(tmp_df, self.bucket_name, f"ingestor_payments_{self.asOf}")
        self.cyphers.link_or_merge_payments(urls)

    def ingest_votes(self):
        logging.info("Ingesting votes...")
        votes_df = self.prepare_votes_data()

        urls = self.save_df_as_csv(votes_df, self.bucket_name, f"ingestor_votes_{self.asOf}")
        self.cyphers.link_or_merge_voters(urls)
        self.cyphers.link_or_merge_votes(urls)

    def run(self):
        self.ingest_daos_and_tokens()
        self.ingest_members()
        self.ingest_proposals()
        self.ingest_votes()
        self.save_metadata()

if __name__ == '__main__':
    ingestor = DaoHausIngestor()
    ingestor.run()
