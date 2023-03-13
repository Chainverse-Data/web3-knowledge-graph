import os

from ...helpers import Etherscan
from .cyphers import ContractDeployersCyphers
from ..helpers import Processor
from tqdm import tqdm

class ContractDeployersProcessor(Processor):
    def __init__(self, bucket_name="contract-deployers"):
        self.cyphers = ContractDeployersCyphers()
        self.etherscan = Etherscan()
        self.chunk_size = 1000
        super().__init__(bucket_name)
    
    def process_multisigs(self):
        multisigs = self.cyphers.get_ens_multisigs()
        for i in tqdm(range(0, len(multisigs), self.chunk_size), desc="Getting Multisig deployers address"):
            current_multisigs = multisigs[i: i+self.chunk_size]
            results = []
            for j in range(0, len(current_multisigs), 5):
                data = self.etherscan.get_contract_deployer(current_multisigs[j: j+5])
                if data:
                    results.extend(data)
            results = [
                {
                    "contractAddress": result["contractAddress"],
                    "address": result["contractCreator"],
                    "txHash": result["txHash"]
                } 
                for result in results
            ]
            urls = self.save_json_as_csv(results, self.bucket_name, f"process_multisig_{self.asOf}_{i}")
            self.cyphers.queries.create_wallets(urls)
            self.cyphers.link_or_merge_deployers(urls)

    def run(self):
        self.process_multisigs()

if __name__ == "__main__":
    P = ContractDeployersProcessor()
    P.run()



