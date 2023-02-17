import logging
from ..helpers import Processor
from .cyphers import TokenMetadataCyphers
import os

class TokenMetadataPostProcess(Processor):
    """This class reads from the Neo4J instance for ERC20 tokens nodes to call the Alchemy getTokenMetadata endpoint to retreive the metadata"""
    def __init__(self):
        self.cyphers = TokenMetadataCyphers()
        super().__init__("token-metadata")
        self.alchemy_api_url = "https://eth-mainnet.g.alchemy.com/v2/{}".format(os.environ["ALCHEMY_API_KEY"])
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

    def get_tokens(self):
        tokens = self.cyphers.get_empty_tokens()
        return tokens

    def get_tokens_ERC20_metadata(self):
        logging.info("Starting ERC20 Metadata extraction")
        tokens = self.get_tokens()
        results = self.parallel_process(self.get_alchemy_ERC20_metadata, tokens, description="Getting all ERC20 metadata")
        metadata_urls = self.save_json_as_csv(results, self.bucket_name, f"token_metadata_{self.asOf}")
        self.cyphers.add_token_node_metadata(metadata_urls)

    def get_alchemy_ERC20_metadata(self, node):
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getTokenMetadata",
            "params": [node['address']]
        }
        response_data = self.post_request(self.alchemy_api_url, json=payload, headers=self.headers, return_json=True)
        if not response_data:
            result = {}
        else:
            result = response_data.get("result", {})
        node['name'] = result.get("name", "")
        node['symbol'] = result.get("symbol", "")
        node['decimals'] = result.get("decimals", "")
        node['logo'] = result.get("logo", "")
        return node

    def run(self):
        self.get_tokens_ERC20_metadata()

if __name__ == '__main__':
    processor = TokenMetadataPostProcess()
    processor.run()