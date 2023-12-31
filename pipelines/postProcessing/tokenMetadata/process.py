import logging
import pandas as pd

from tqdm import tqdm
from ...helpers import Alchemy, Etherscan
from ..helpers import Processor
from .cyphers import TokenMetadataCyphers
import os
import re

class TokenMetadataPostProcess(Processor):
    """This class reads from the Neo4J instance for ERC20 tokens nodes to call the Alchemy getTokenMetadata endpoint to retreive the metadata"""
    def __init__(self):
        self.cyphers = TokenMetadataCyphers()
        super().__init__("token-metadata")
        self.alchemy = Alchemy()
        self.etherscan = Etherscan()
        self.chunk_size = 10000

    def upsert(self, node, result, key):
        if result.get(key, None):
            return result.get(key)
        else:
            return node.get(key, None)

    def get_tokens_ERC721_metadata(self):
        logging.info("Starting ERC721 Metadata extraction")
        tokens = self.cyphers.get_empty_ERC721_tokens()
        for i in tqdm(range(0, len(tokens), self.chunk_size)):
            results = self.parallel_process(self.get_alchemy_ERC721_metadata, tokens[i: i+self.chunk_size], description="Getting all ERC721 Metadata")
            
            twitter = [{"handle": result["twitterUsername"], "address": result["address"], "citation": "OpenSea Metadata"} for result in results if result["twitterUsername"]]
            urls = self.save_json_as_csv(twitter, f"token_ERC721_twitters_{self.asOf}")
            logging.info(f"Adding {len(twitter)} twitters")
            self.cyphers.create_or_merge_socials(urls, ["Twitter", "Account"], "handle", "handle", "HAS_ACCOUNT", "citation")
            
            websites = [{"url": result["externalUrl"], "address": result["address"], "citation": "OpenSea Metadata"} for result in results if result["externalUrl"]]
            urls = self.save_json_as_csv(websites, f"token_ERC721_websites_{self.asOf}")
            logging.info(f"Adding {len(websites)} websites")
            self.cyphers.create_or_merge_socials(urls, ["Website", "Account"], "url", "url", "HAS_ACCOUNT", "citation")

            discords = [{"url": result["discordUrl"], "address": result["address"], "citation": "OpenSea Metadata"} for result in results if "discordUrl" in result and result["discordUrl"]]
            urls = self.save_json_as_csv(discords, f"token_ERC721_websites_{self.asOf}")
            logging.info(f"Adding {len(discords)} discords")
            self.cyphers.create_or_merge_socials(urls, ["Discord", "Hub"], "url", "url", "HAS_HUB", "citation")

            deployers = [{"address": result["address"], "contractDeployer": result["contractDeployer"]} for result in results if result["contractDeployer"]]
            deployers_wallets = [{"address": result["contractDeployer"]} for result in results if result["contractDeployer"]]
            deployers_urls = self.save_json_as_csv(deployers, f"token_ERC721_deployers_{self.asOf}")
            deployers_wallets_urls = self.save_json_as_csv(deployers_wallets, f"token_ERC721_deployers_wallets_{self.asOf}")
            self.cyphers.queries.create_wallets(deployers_wallets_urls)
            self.cyphers.add_ERC721_deployers(deployers_urls)

            metadata_urls = self.save_json_as_csv(results, f"token_ERC721_metadata_{self.asOf}")
            self.cyphers.add_ERC721_token_node_metadata(metadata_urls)

    def get_alchemy_ERC721_metadata(self, node):
        response_data = self.alchemy.getNFTMetadata(node["address"])
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

    def ingest_socials(self, metadata):
        metadata = pd.DataFrame(metadata)
        social_keys = {
            "website": {"label": "Website", "property": "website" , "call": self.handle_external_links},
            "email": {"label": "Email", "property": "email" , "call": self.handle_accounts},
            "blog": {"label": "Website", "property": "blog" , "call": self.handle_external_links},
            "reddit": {"label": "Reddit", "property": "reddit" , "call": self.handle_reddits},
            "slack": {"label": "Slack", "property": "slack" , "call": self.handle_hubs},
            "facebook": {"label": "Facebook", "property": "facebook" , "call": self.handle_accounts},
            "twitter": {"label": "Twitter", "property": "twitter" , "call": self.handle_accounts},
            "bitcointalk": {"label": "Bitcointalk", "property": "bitcointalk" , "call": self.handle_accounts},
            "github": {"label": "Github", "property": "github" , "call": self.handle_githubs},
            "telegram": {"label": "Telegram", "property": "telegram" , "call": self.handle_accounts},
            "wechat": {"label": "Wechat", "property": "wechat" , "call": self.handle_accounts},
            "linkedin": {"label": "Linkedin", "property": "linkedin" , "call": self.handle_accounts},
            "discord": {"label": "Discord", "property": "discord" , "call": self.handle_hubs},
            "whitepaper": {"label": "Whitepaper", "property": "whitepaper" , "call": self.handle_white_paper}
        }
        for key, social in social_keys.items():
            data = metadata[metadata[key] != ""][["address", key]].drop_duplicates()
            social["call"](data, key, social["label"], social["property"])

    def handle_reddits(self, data, key, label, property):
        def get_infos(url):
            matches = re.match("https:\/\/www\.reddit\.com\/(\w*)?\/?(\w*)?", url)
            is_user, handle = None, None
            if matches:
                is_user = matches.groups()[0]
                if is_user == "r": is_user = False
                elif is_user == "u": is_user = True
                else: is_user = None
                handle = matches.groups()[1]
            return is_user, handle
        data = data[~data[key].isna()]
        tmp = data[key].apply(get_infos)
        data["is_user"] = tmp.apply(lambda element: element[0])
        data["handle"] = tmp.apply(lambda element: element[1])
        users = data[data["is_user"] == True][["address", "handle", property]]
        subreddits = data[data["is_user"] == False][["address", "handle", property]]
        urls = self.save_df_as_csv(users, f"process_reddits_users_{self.asOf}")
        logging.info(f"Adding {len(users)} reddits accounts")
        self.cyphers.create_or_merge_socials(urls, [label, "Account"], "handle", "handle", "HAS_ACCOUNT", property)

        urls = self.save_df_as_csv(subreddits, f"process_subreddits_{self.asOf}")
        logging.info(f"Adding {len(subreddits)} subreddits")
        self.cyphers.create_or_merge_socials(urls, [label, "Hub"], "handle", "handle", "HAS_HUB", property)

    def handle_githubs(self, data, key, label, property):
        def get_accounts(url):
            matches = re.match("https:\/\/github.com\/(\w*)?\/?(\w*)?", url)
            account, repository = None, None
            if matches:
                account = matches.groups()[0]
                repository = matches.groups()[1]
            return account, repository
        data = data[~data[key].isna()]
        tmp = data[key].apply(get_accounts)
        data["account"] = tmp.apply(lambda element: element[0])
        data["repository"] = tmp.apply(lambda element: element[1])
        tmp_data = data[~data["repository"].isna()]
        tmp_data["full_name"] = tmp_data["account"] + "/" + tmp_data["repository"]
        urls = self.save_df_as_csv(tmp_data, f"process_githubs_repos_{self.asOf}")
        logging.info(f"Adding {len(tmp_data)} github repos")
        self.cyphers.create_or_merge_socials(urls, [label, "Repository"], "full_name", "full_name", "HAS_REPOSITORY", property)

    def handle_twitter(self, data, key, label, property):
        def get_handles(url):
            matches = re.match("https:\/\/twitter\.com\/(\w*)", url)
            handle =  None
            if matches:
                handle = matches.groups()[00]
            else:
                handle = url
            return handle
        tmp_data = data[~data[key].isna()]
        tmp_data["handle"] = data[key].apply(get_handles)
        tmp_data["handle"] = tmp_data[~tmp_data["handle"].isna()]
        urls = self.save_df_as_csv(data, f"process_{key}_{self.asOf}")
        logging.info(f"Adding {len(data)} {key}")
        self.cyphers.create_or_merge_socials(urls, [label, "Account"], "handle", property, "HAS_ACCOUNT", property)

    def handle_accounts(self, data, key, label, property):
        data = data[~data[key].isna()]
        urls = self.save_df_as_csv(data, f"process_{key}_{self.asOf}")
        logging.info(f"Adding {len(data)} {key}")
        self.cyphers.create_or_merge_socials(urls, [label, "Account"], "handle", property, "HAS_ACCOUNT", property)

    def handle_external_links(self, data, key, label, property):
        data = data[~data[key].isna()]
        data = data.drop_duplicates()
        urls = self.save_df_as_csv(data, f"process_{key}_{self.asOf}")
        logging.info(f"Adding {len(data)} {key}")
        self.cyphers.create_or_merge_socials(urls, [label, "Account"], "url", property, "HAS_ACCOUNT", property)

    def handle_hubs(self, data, key, label, property):
        data = data[~data[key].isna()]
        urls = self.save_df_as_csv(data, f"process_{key}_{self.asOf}")
        logging.info(f"Adding {len(data)} {key}")
        self.cyphers.create_or_merge_socials(urls, [label, "Hub"], "url", property, "HAS_HUB", property)

    def handle_white_paper(self, data, key, label, property):
        data = data[~data[key].isna()]
        urls = self.save_df_as_csv(data, f"process_{key}_{self.asOf}")
        logging.info(f"Adding {len(data)} {key}")
        self.cyphers.create_or_merge_socials(urls, [label, "Whitepaper"], "url", property, "HAS_WHITEPAPER", property)

    def get_tokens_ERC20_metadata(self):
        logging.info("Starting ERC20 Metadata extraction")
        tokens = self.cyphers.get_empty_ERC20_tokens()
        for i in tqdm(range(0, len(tokens), self.chunk_size)):
            results = self.parallel_process(self.get_ERC20_metadata, tokens[i: i+self.chunk_size], description="Getting all ERC20 metadata")
            results = [result for result in results if result.get("metadataScraped", None)]
            metadata_urls = self.save_json_as_csv(results, f"token_ERC20_metadata_{self.asOf}")
            self.cyphers.add_ERC20_token_node_metadata(metadata_urls)
            self.ingest_socials(results)

    def get_ERC20_metadata(self, node):
        node = self.get_alchemy_ERC20_metadata(node)
        node = self.get_etherscan_ERC20_metadata(node)
        return node
    
    def get_alchemy_ERC20_metadata(self, node):
        response_data = self.alchemy.getTokenMetadata(node["address"])
        print(response_data)
        if type(response_data) != dict:
            result = {}
        else:
            result = response_data
            node['metadataScraped'] = True
        node['name'] = self.upsert(node, result, "name")
        node['symbol'] = self.upsert(node, result, "symbol")
        node['decimals'] = self.upsert(node, result, "decimals")
        node['logo'] = self.upsert(node, result, "logo")
        return node
    
    def get_etherscan_ERC20_metadata(self, node):
        response_data = self.etherscan.get_token_information(node["address"])
        print(response_data)
        if type(response_data) != dict:
            result = {}
        else:
            result = response_data
            node['metadataScraped'] = True
        node['name'] = self.upsert(node, result, "tokenName")
        node['symbol'] = self.upsert(node, result, "symbol")
        node['decimals'] = self.upsert(node, result, "divisor")
        node['totalSupply'] = self.upsert(node, result, "totalSupply")
        node['blueCheckmark'] = self.upsert(node, result, "blueCheckmark")
        node['description'] = self.upsert(node, result, "description")
        node['tokenPriceUSD'] = self.upsert(node, result, "tokenPriceUSD")
        social_keys = ["website", "email", "blog", "reddit", "slack", "facebook", "twitter", "bitcointalk", "github", "telegram", "wechat", "linkedin", "discord", "whitepaper"]
        for social_key in social_keys:
            node[social_key] = self.upsert(node, result, social_key)
        return node

    def run(self):
        self.get_tokens_ERC721_metadata()
        self.get_tokens_ERC20_metadata()

if __name__ == '__main__':
    processor = TokenMetadataPostProcess()
    processor.run()