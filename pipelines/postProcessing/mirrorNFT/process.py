
import logging
from .cypher import MirrorNFTCyphers
from ..helpers import Processor
from ...helpers import Alchemy, Etherscan
import os


class MirrorNFTProcessor(Processor):
    def __init__(self, bucket_name="citizen-token-holding"):
        self.cyphers = MirrorNFTCyphers()
        super().__init__(bucket_name)
        self.NFT_chunk_size = 10
        self.ERC20_chunk_size = 10
        self.ERC20_last_block = self.metadata.get("ERC20_last_block", {})
        self.alchemy = Alchemy() 
        self.etherscan = Etherscan() 

    def get_mirror_NFTs_tokens(self):
        tokens = self.cyphers.get_mirror_ERC721_tokens()
        logging.info(f"{len(tokens)} tokens retrieved for processing!")
        return tokens

    def get_holders(self, address):
        return self.alchemy.getOwnersForCollection(address, chain="optimism")

    def get_holders_for_mirror_NFT_tokens(self):
        tokens = self.get_mirror_NFTs_tokens()
        for i in range(0, len(tokens), self.NFT_chunk_size):
            data = self.parallel_process(self.get_holders, tokens[i: i+self.NFT_chunk_size], description="Getting NFT token Holders")
            results = []
            for token, holders in zip(tokens[i: i+self.NFT_chunk_size], data):
                for element in holders:
                    for balance in element["tokenBalances"]:
                        tmp = {
                            "contractAddress": token,
                            "address": element["ownerAddress"],
                            "tokenId": balance["tokenId"],
                            "balance": balance["balance"]
                        }
                        results.append(tmp)
            urls = self.save_json_as_csv(results, self.bucket_name, f"process_nft_tokens_{self.asOf}_{i}")
            self.cyphers.queries.create_wallets(urls)
            self.cyphers.link_or_merge_NFT_token_holding(urls)
    
    def run(self):
        self.get_holders_for_mirror_NFT_tokens()

if __name__ == "__main__":
    P = MirrorNFTProcessor()
    P.run()