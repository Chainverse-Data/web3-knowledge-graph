

import logging
from ...helpers import Etherscan
from ..helpers import Scraper


class NFTfiScraper(Scraper):
    def __init__(self, bucket_name="nftfi"):
        super().__init__(bucket_name)
        self.NFTfi_contract = "0x88341d1a8F672D2780C8dC725902AAe72F143B0c"
        self.topics = {
            "LoanStarted": "0x7a9a95acdaec2726663658ce76ffb4b960ce244a001b3252a5bb3db586e28c1b",
            "LoanRepaid": "0x61de14a46baf4e24c466c80f5d823c5925a65ffce385bf401fef42afd8f3a993", 
            "LoanLiquidated": "0xf62f4578c92288f0f2dd8009bb1fbae39796c677d6e0b12d28f0ad83b30af6c9"
        }
        self.start_block = self.metadata.get("start_block", 10073259)
        self.etherscan = Etherscan()
        
    def save_last_block(self):
        last_block = 0
        for eventName in self.data:
            for log in self.data[eventName]:
                if log["blockNumber"] > last_block:
                    last_block = log["blockNumber"]
        self.metadata["start_block"] = last_block
    
    def get_LoanStarted(self):
        logging.info("Getting the LoanStarted events")
        logs = self.etherscan.get_decoded_event_logs(self.NFTfi_contract, "LoanStarted", fromBlock=self.start_block, topic0=self.topics["LoanStarted"])
        results = []
        for log in logs:
            tmp = dict(log["args"])
            tmp["event"] = log["event"]
            tmp["transactionHash"] = log["transactionHash"].hex()
            tmp["contractAddress"] = log["address"]
            tmp["blockNumber"] = log["blockNumber"]
            results.append(tmp)
        self.data["LoanStarted"] = results

    def get_LoanRepaid(self):
        logging.info("Getting the LoanRepaid events")
        logs = self.etherscan.get_decoded_event_logs(self.NFTfi_contract, "LoanRepaid", fromBlock=self.start_block, topic0=self.topics["LoanRepaid"])
        results = []
        for log in logs:
            tmp = dict(log["args"])
            tmp["event"] = log["event"]
            tmp["transactionHash"] = log["transactionHash"].hex()
            tmp["contractAddress"] = log["address"]
            tmp["blockNumber"] = log["blockNumber"]
            results.append(tmp)
        self.data["LoanRepaid"] = results

    def get_LoanLiquidated(self):
        logging.info("Getting the LoanLiquidated events")
        logs = self.etherscan.get_decoded_event_logs(self.NFTfi_contract, "LoanLiquidated", fromBlock=self.start_block, topic0=self.topics["LoanLiquidated"])
        results = []
        for log in logs:
            tmp = dict(log["args"])
            tmp["event"] = log["event"]
            tmp["transactionHash"] = log["transactionHash"].hex()
            tmp["contractAddress"] = log["address"]
            tmp["blockNumber"] = log["blockNumber"]
            results.append(tmp)
        self.data["LoanLiquidated"] = results

    def run(self):
        self.get_LoanStarted()
        self.get_LoanRepaid()
        self.get_LoanLiquidated()
        self.save_data()
        self.save_last_block()
        self.save_metadata()

if __name__ == '__main__':
    S = NFTfiScraper()
    S.run()