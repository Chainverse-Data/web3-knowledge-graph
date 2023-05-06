import argparse
from datetime import datetime, timedelta, timezone
import logging
from .cypher import TokenHoldersCyphers
from ..helpers import Processor
from ...helpers import Alchemy, Etherscan
import os

DEBUG = os.environ.get("DEBUG", False)
DEBUG = False

class TokenHoldersProcessor(Processor):
    def __init__(self, address, chain, tokenType, bucket_name="token-holders"):
        self.address = address
        self.chain = chain
        self.tokenType = tokenType
        self.cyphers = TokenHoldersCyphers()
        super().__init__(bucket_name)
        self.alchemy = Alchemy() 
        self.etherscan = Etherscan() 

    def get_alchemy_NFT_metadata(self):
        response_data = self.alchemy.getNFTMetadata(self.address)
        if type(response_data) != dict:
            result = {}
        else:
            result = response_data
        node = {"address": self.address}
        node['title'] = result.get("title", None)
        node['description'] = result.get("description", None)
        node['tokenUri_gateway'] = result.get("tokenUri", {}).get("gateway", None)
        node['tokenUri_raw'] = result.get("tokenUri", {}).get("raw", None)
        if type(result.get("metadata", None)) == dict:
            node['image'] = result.get("metadata", {}).get("image", None)
        else:
            node['image'] = None
        node['timeLastUpdated'] = result.get("timeLastUpdated", None)
        node['symbol'] = result.get("contractMetadata", {}).get("symbol", None)
        node['totalSupply'] = result.get("contractMetadata", {}).get("totalSupply", None)
        node['contractDeployer'] = result.get("contractMetadata", {}).get("contractDeployer", None)
        node['deployedBlockNumber'] = result.get("contractMetadata", {}).get("deployedBlockNumber", None)
        node["floorPrice"] = result.get("contractMetadata", {}).get("openSea", {}).get("floorPrice", None)
        node["collectionName"] = result.get("contractMetadata", {}).get("openSea", {}).get("collectionName", None)
        node["safelistRequestStatus"] = result.get("contractMetadata", {}).get("openSea", {}).get("safelistRequestStatus", None)
        node["imageUrl"] = result.get("contractMetadata", {}).get("openSea", {}).get("imageUrl", None)
        node["openSeaName"] = result.get("contractMetadata", {}).get("openSea", {}).get("collectionName", None)
        node["openSeaDescription"] = result.get("contractMetadata", {}).get("openSea", {}).get("description", None)
        node["externalUrl"] = result.get("contractMetadata", {}).get("openSea", {}).get("externalUrl", None)
        node["twitterUsername"] = result.get("contractMetadata", {}).get("openSea", {}).get("twitterUsername", None)
        return node

    def get_holders_for_NFT_tokens(self):
        data = [self.alchemy.getOwnersForCollection(self.address)]
        results = []
        for holders in data:
            for element in holders:
                for balance in element["tokenBalances"]:
                    tmp = {
                        "contractAddress": self.address,
                        "address": element["ownerAddress"],
                        "tokenId": balance["tokenId"],
                        "balance": balance["balance"]
                    }
                    results.append(tmp)
        metadata = self.get_alchemy_NFT_metadata()
        print(metadata)
        self.cyphers.mark_current_hold_edges([self.address])
        urls = self.save_json_as_csv(results, f"process_nft_tokens_{self.asOf}_{self.address}")
        self.cyphers.queries.create_wallets(urls)
        self.cyphers.clean_NFT_token_holding(urls)
        self.cyphers.link_or_merge_NFT_token_holding(urls)
        self.cyphers.move_old_hold_edges_to_held([self.address])
        self.cyphers.update_tokens([self.address])
        self.cyphers.add_NFT_token_node_metadata(metadata)
    
    def get_holders_for_ERC20_token(self):
        logging.info("Getting ERC20 token holders from Etherscan")
        data = self.etherscan.get_token_holders(self.address, chain=self.chain)
        logging.info("Getting ERC20 token metadata from Etherscan")
        metadata = self.etherscan.get_token_information(self.address, chain=self.chain)
        if metadata and data:
            results = []
            divisor = metadata["divisor"]
            for holder in data:
                numericBalance = None
                if divisor:
                    try:
                        numericBalance = int(holder["TokenHolderQuantity"]) / 10**int(divisor)
                    except:
                        pass
                tmp = {
                    "contractAddress": self.address,
                    "address": holder["TokenHolderAddress"],
                    "balance": holder["TokenHolderQuantity"],
                    "numericBalance": numericBalance,

                }
                results.append(tmp)
            logging.info("Saving the data")
            self.cyphers.mark_current_hold_edges([self.address])
            urls = self.save_json_as_csv(results, f"process_erc20_tokens_{self.asOf}_{self.address}")
            self.cyphers.queries.create_wallets(urls)
            self.cyphers.link_or_merge_ERC20_token_holding(urls)
            self.cyphers.move_old_hold_edges_to_held([self.address])
            self.cyphers.update_tokens([self.address])
            self.cyphers.add_ERC20_token_node_metadata(metadata)

    def run(self):
        self.cyphers.set_pipeline_status(self.address)
        if self.tokenType == "ERC20":
            self.get_holders_for_ERC20_token()
        else:
            self.get_holders_for_NFT_tokens()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TokenHolders arguments.')
    parser.add_argument('-t', '--tokenAddress', type=str, help='the token address')
    parser.add_argument('-e', '--erc', type=str, help='the token contract type: ERC20 | ERC721 | ERC1155')
    parser.add_argument('-c', '--chain' ,type=str, help='the chain the token is on: ethereum | polygon | arbitrum | optimism (ERC20 only: binance)')
    args = parser.parse_args()
    print(args)
    P = TokenHoldersProcessor(args.tokenAddress, args.chain, args.erc)
    P.run()