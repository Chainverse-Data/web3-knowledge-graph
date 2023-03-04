import logging

from tqdm import tqdm
from ..helpers import Processor
from .cyphers import TokenMetadataCyphers
import os

class TokenMetadataPostProcess(Processor):
    """This class reads from the Neo4J instance for ERC20 tokens nodes to call the Alchemy getTokenMetadata endpoint to retreive the metadata"""
    def __init__(self):
        self.cyphers = TokenMetadataCyphers()
        super().__init__("token-metadata")
        self.alchemy_api_url = f"https://eth-mainnet.g.alchemy.com/v2/{os.environ['ALCHEMY_API_KEY']}"
        self.alchemy_nft_api_url = "https://eth-mainnet.g.alchemy.com/nft/v2/{}/getNFTMetadata?contractAddress={}&tokenId=0"
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        self.chunk_size = 10000

    def get_tokens_ERC721_metadata(self):
        logging.info("Starting ERC721 Metadata extraction")
        tokens = self.cyphers.get_empty_ERC721_tokens()
        for i in tqdm(range(0, len(tokens), self.chunk_size)):
            results = self.parallel_process(self.get_alchemy_ERC721_metadata, tokens[i: i+self.chunk_size], description="Getting all ERC721 Metadata")
            metadata_urls = self.save_json_as_csv(results, self.bucket_name, f"token_ERC721_metadata_{self.asOf}")
            self.cyphers.add_ERC721_token_node_metadata(metadata_urls)
            deployers = [{"address": result["address"], "contractDeployer": result["contractDeployer"]} for result in results if result["contractDeployer"]]
            deployers_wallets = [{"address": result["contractDeployer"]} for result in results if result["contractDeployer"]]
            deployers_urls = self.save_json_as_csv(deployers, self.bucket_name, f"token_ERC721_deployers_{self.asOf}")
            deployers_wallets_urls = self.save_json_as_csv(deployers_wallets, self.bucket_name, f"token_ERC721_deployers_wallets_{self.asOf}")
            self.cyphers.queries.create_wallets(deployers_wallets_urls)
            self.cyphers.add_ERC721_deployers(deployers_urls)

    def get_tokens_ERC20_metadata(self):
        logging.info("Starting ERC20 Metadata extraction")
        tokens = self.cyphers.get_empty_ERC20_tokens()
        for i in tqdm(range(0, len(tokens), self.chunk_size)):
            results = self.parallel_process(self.get_alchemy_ERC20_metadata, tokens[i: i+self.chunk_size], description="Getting all ERC20 metadata")
            metadata_urls = self.save_json_as_csv(results, self.bucket_name, f"token_ERC20_metadata_{self.asOf}")
            self.cyphers.add_ERC20_token_node_metadata(metadata_urls)

    def get_alchemy_ERC721_metadata(self, node):
        url = self.alchemy_nft_api_url.format(os.environ['ALCHEMY_API_KEY'], node["address"])
        response_data = self.get_request(url, headers=self.headers, json=True)
        if type(response_data) != dict:
            result = {}
        else:
            result = response_data
            result["metadataScraped"] = True
        node = {"address": node["address"]}
        node['metadataScraped'] = result.get("metadataScraped", None)
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

    def get_alchemy_ERC20_metadata(self, node):
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getTokenMetadata",
            "params": [node['address']]
        }
        response_data = self.post_request(self.alchemy_api_url, json=payload, headers=self.headers, return_json=True)
        if type(response_data) != dict:
            result = {}
        else:
            result = response_data.get("result", {})
            node['metadataScraped'] = True
        node['metadataScraped'] = result.get("metadataScraped", None)
        node['name'] = result.get("name", None)
        node['symbol'] = result.get("symbol", None)
        node['decimals'] = result.get("decimals", None)
        node['logo'] = result.get("logo", None)
        return node

    def run(self):
        self.get_tokens_ERC20_metadata()
        self.get_tokens_ERC721_metadata()

if __name__ == '__main__':
    processor = TokenMetadataPostProcess()
    processor.run()