import logging
import time
import os
from . import Requests

DEBUG = os.environ.get("DEBUG", False)

class Alchemy(Requests):
    def __init__(self, max_retries=5):
        self.chains = ["ethereum", "optimism", "arbitrum", "polygon"]
        self.alchemy_api_url = {
            "ethereum": f"https://eth-mainnet.g.alchemy.com/v2/{os.environ['ALCHEMY_API_KEY']}",
            "optimism": f"https://opt-mainnet.g.alchemy.com/v2/{os.environ['ALCHEMY_API_KEY_OPTIMISM']}",
            "arbitrum": f"https://arb-mainnet.g.alchemy.com/v2/{os.environ['ALCHEMY_API_KEY_ARBITRUM']}",
            "polygon": f"https://polygon-mainnet.g.alchemy.com/v2/{os.environ['ALCHEMY_API_KEY_POLYGON']}"
        }
        self.alchemy_nft_url = {
            "ethereum": f"https://eth-mainnet.g.alchemy.com/nft/v2/{os.environ['ALCHEMY_API_KEY']}",
            "optimism": f"https://opt-mainnet.g.alchemy.com/nft/v2/{os.environ['ALCHEMY_API_KEY_OPTIMISM']}",
            "arbitrum": f"https://arb-mainnet.g.alchemy.com/nft/v2/{os.environ['ALCHEMY_API_KEY_ARBITRUM']}",
            "polygon": f"https://polygon-mainnet.g.alchemy.com/nft/v2/{os.environ['ALCHEMY_API_KEY_POLYGON']}"
        }
        self.headers = {"Content-Type": "application/json"}
        self.max_retries = max_retries
    
    def getNFTMetadata(self, tokenAddress, chain="ethereum", tokenId=0, tokenType=None, counter=0):
        """
            Helper function to get a ERC721 or ERC1155 token Metadata from alchemy.
            Parameters are:
                - token: (address) token contract address
                - tokenId: (int) tokenId, defaults to 0 to get the overall contract metadata
                - tokenType: (None|ERC721|ERC1155) Makes query faster if the contract type is specified 
                - chain: (ethereum|arbitrum|polygon|optimism) which chain to get this data from
        """
        if counter > self.max_retries:
            time.sleep(counter)
            return None
        
        params = {
            "contractAddress": tokenAddress,
            "tokenId": tokenId,
        }
        if tokenType: params["tokenType"] = tokenType

        url = self.alchemy_nft_url[chain] + "/getNFTMetadata"
        if DEBUG: logging.debug(f"Calling url: {url}")
        result = self.get_request(url, params=params, headers=self.headers, json=True)
        if type(result) != dict:
            return self.getNFTMetadata(tokenAddress, chain=chain, tokenId=tokenId, tokenType=tokenType, counter=counter+1)
        return result

    def getTokenMetadata(self, tokenAddress, chain="ethereum", counter=0):
        """
            Helper function to get a token Metadata from alchemy.
            Parameters are:
                - token: (address) token contract address
                - chain: (ethereum|arbitrum|polygon|optimism) which chain to get this data from
        """
        if counter > self.max_retries:
            time.sleep(counter)
            return None
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getTokenMetadata",
            "params": [tokenAddress]
        }
        if DEBUG: logging.debug(f"Calling url: {self.alchemy_api_url[chain]} with payload: {payload}")
        response_data = self.post_request(self.alchemy_api_url[chain], json=payload, headers=self.headers, return_json=True)
        if response_data and "result" in response_data:
            result = response_data.get("result", {})
            return result
        else:
            return self.getTokenMetadata(tokenAddress, chain=chain, counter=counter+1)

    def getOwnersForCollection(self, 
                               token,
                               block=None,
                               withTokenBalances=True,
                               chain="ethereum",
                               pageKey=None, 
                               counter=0):
        """
            Helper function to automate getting the balance and holders data from Alchemy for NFT tokens (ERC721 and ERC1155).
            Parameters are:
                - token: (address) token contract address
                - block: (int) the owners and balance at a particular block number, defaults to latest
                - withTokenBalance: (boolean) returns the token balance with the token Id
                - chain: (ethereum|arbitrum|polygon|optimism) which chain to get this data from
        """
        if counter > self.max_retries:
            time.sleep(counter)
            return None
        results=[]
        params = {
            "contractAddress": token,
            "withTokenBalances": withTokenBalances
        }
        if block:
            params["block"] = block
        if pageKey:
            params["pageKey"] = pageKey
        url = self.alchemy_nft_url[chain] + "/getOwnersForCollection"
        if DEBUG: logging.debug(f"Calling url: {url}")
        content = self.get_request(url, params=params, headers=self.headers, json=True)
        if not content or not "ownerAddresses" in content:
            return self.getOwnersForCollection(token, pageKey=pageKey, counter=counter+1)
        results.extend(content["ownerAddresses"])
        pageKey = content.get("pageKey", None)
        if pageKey:
            next_results = self.getOwnersForCollection(token, pageKey=pageKey, counter=0)
            if next_results:
                results.extend(next_results)
        return results

    def getAssetTransfers(self, 
                          tokens=None, 
                          fromBlock=None, 
                          toBlock=None, 
                          fromAddress=None, 
                          toAddress=None, 
                          maxCount=None, 
                          excludeZeroValue=True, 
                          external=True,
                          internal=True,
                          erc20=True,
                          erc721=True,
                          erc1155=True,
                          specialnft=True,
                          order="asc",
                          chain="ethereum",
                          pageKey=None,
                          pageKeyIterate=True,
                          counter=0):
        """
            Helper function to automate getting the transfers data from Alchemy for any tokens.
            Parameters are:
                - tokens: [(address)] token contract addresses as an array
                - fromBlock: (int) starting block 
                - toBlock: (int) ending block
                - fromAddress: (address) filter transactions from this address
                - toAddress: (address) filter transactions to this address
                - maxCount: (int) max number of transactions to return 
                - excludeZeroValue: (boolean) wether or not to return zero value transactions
                - external: (boolean) Wether or not to include external transactions 
                - internal: (boolean) Wether or not to include internal transactions (only for ethereum)
                - erc20: (boolean) Wether or not to include erc20 
                - erc721: (boolean) Wether or not to include erc721 
                - erc1155: (boolean) Wether or not to include erc1155 
                - specialnft: (boolean) Wether or not to include specialnft 
                - chain: (ethereum|arbitrum|polygon|optimism) which chain to get this data from
        """
        results = []
        if counter > self.max_retries:
            time.sleep(counter)
            return None

        params = {
            "order": order,
            "excludeZeroValue": excludeZeroValue
        }
        if tokens: params["contractAddresses"] = tokens
        if fromBlock: params["fromBlock"] = fromBlock
        if toBlock: params["toBlock"] = toBlock
        if fromAddress: params["fromAddress"] = fromAddress
        if toAddress: params["toAddress"] = toAddress
        if maxCount: params["maxCount"] = hex(maxCount)
        if pageKey: params["pageKey"] = pageKey

        categories = []
        if external: categories.append("external")
        if internal and chain in ["ethereum", "polygon"]: categories.append("internal")
        if erc20: categories.append("erc20")
        if erc721: categories.append("erc721")
        if erc1155: categories.append("erc1155")
        if specialnft: categories.append("specialnft")
        params["category"] = categories

        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "alchemy_getAssetTransfers",
            "params": [
                params
            ]
        }

        content = self.post_request(self.alchemy_api_url[chain], json=payload, headers=self.headers, return_json=True)
        if DEBUG: logging.debug(f"Calling url: {self.alchemy_api_url[chain]} with payload: {payload}")
        if content and "result" in content:
            result = content["result"].get("transfers", [])
            results.extend(result)
            pageKey = content["result"].get("pageKey", None)
            if pageKeyIterate and pageKey:
                newResults = self.getAssetTransfers(tokens, fromBlock=fromBlock, toBlock=toBlock, fromAddress=fromAddress, toAddress=toAddress, maxCount=maxCount, excludeZeroValue=excludeZeroValue, external=external, internal=internal, erc20=erc20, erc721=erc721, erc1155=erc1155, specialnft=specialnft, pageKey=pageKey, order=order, chain=chain)
                if newResults:
                    results.extend(newResults)
        else:
            return self.getAssetTransfers(tokens, fromBlock=fromBlock, toBlock=toBlock, fromAddress=fromAddress, toAddress=toAddress, maxCount=maxCount, excludeZeroValue=excludeZeroValue, external=external, internal=internal, erc20=erc20, erc721=erc721, erc1155=erc1155, specialnft=specialnft, pageKey=pageKey, order=order, chain=chain, counter=counter+1)
        return results
    
    def getTokenBalances(self, 
                                  tokens, 
                                  address, 
                                  chain="ethereum",
                                  pageKey=None,
                                  counter=0):
        """
            Helper function to automate getting the transfers data from Alchemy for any tokens.
            Parameters are:
                - tokens: [(address)] token contract addresses as an array
                - address: (address) get balance for this address
                - chain: (ethereum|arbitrum|polygon|optimism) which chain to get this data from
        """
        results = []
        if counter > self.max_retries:
            time.sleep(counter)
            return None

        params = [
            address,
            tokens
        ]
        if pageKey: params.append({"pageKey": pageKey})

        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "alchemy_getTokenBalances",
            "params": params
        }

        if DEBUG: logging.debug(f"Calling url: {self.alchemy_api_url[chain]} with payload: {payload}")
        content = self.post_request(self.alchemy_api_url[chain], json=payload, headers=self.headers, return_json=True)
        if content and "result" in content:
            result = content["result"]
            results.extend(result)
            pageKey = result.get("pageKey", None)
            if pageKey:
                newResults = self.getTokenBalances(tokens, address, chain=chain, pageKey=pageKey)
                if newResults:
                    results.extend(newResults)
        else:
                return self.getTokenBalances(tokens, address, chain=chain, pageKey=pageKey, counter=counter+1)
        return results
    
    def getBlockByNumber(self, block, full_transaction=False, chain="ethereum", counter=0):
        """
            Helper function to automate getting the transfers data from Alchemy for any tokens.
            Parameters are:
                - block: (hex) block number
                - full_transaction: (boolean) Wether to return the full block information
                - chain: (ethereum|arbitrum|polygon|optimism) which chain to get this data from
        """
        if counter > self.max_retries:
            time.sleep(counter)
            return None
        
        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "eth_getBlockByNumber",
            "params": [
                block,
                full_transaction
            ]
        }

        if DEBUG: logging.debug(f"Calling url: {self.alchemy_api_url[chain]} with payload: {payload}")
        response_data = self.post_request(self.alchemy_api_url[chain], json=payload, headers=self.headers, return_json=True)
        if response_data and "result" in response_data:
            result = response_data.get("result", {})
            return result
        else:
            return self.getTokenMetadata(block, full_transaction=full_transaction, chain=chain, counter=counter+1)

    def getSpamContracts(self, chain="ethereum", counter=0):
        """
            Helper function to get the list of Alchemy defines spam contracts.
                - chain: (ethereum|arbitrum|polygon|optimism) which chain to get this data from
        """
        assert chain in ["ethereum", "polygon"], "Supported chains are only ethereum and polygon"
        if counter > self.max_retries:
            time.sleep(counter)
            return None
        
        url = self.alchemy_nft_url[chain] + "/getSpamContracts"
        if DEBUG: logging.debug(f"Calling url: {url}")
        result = self.get_request(url, headers=self.headers, json=True)
        if type(result) != list:
            return self.getSpamContracts(chain=chain, counter=counter+1)
        return result

    def getLogs(self, contractAddress, fromBlock=0, toBlock="latest", topics=None, blockHash=None, chain="ethereum", counter=0):
        """
            Helper function to automate getting the log data for a contract, filtered by block time, block hash and topics.
            Parameters are:
                - contractAddress: (address) The address of the contract
                - fromBlock: (int|hex|string) start block
                - toBlock: (int|hex|string) end block
                - topics: [(topicHash)] An array of topic hashes
                - blockHash: (blockHash) A block hash 
                - chain: (ethereum|arbitrum|polygon|optimism) which chain to get this data from
        """
        if counter > self.max_retries:
            time.sleep(counter)
            return None
        
        params = {
            "address": contractAddress
        }
        if fromBlock: params["fromBlock"] = hex(fromBlock) 
        if toBlock: params["toBlock"] = hex(toBlock)
        if topics: params["topics"] = topics
        if blockHash: params["blockHash"] = blockHash

        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "eth_getLogs",
            "params": [params]
        }

        if DEBUG: logging.debug(f"Calling url: {self.alchemy_api_url[chain]} with payload: {payload}")
        response_data = self.post_request(self.alchemy_api_url[chain], json=payload, headers=self.headers, return_json=True)
        if response_data and "result" in response_data:
            result = response_data.get("result", {})
            return result
        else:
            return self.getLogs(contractAddress, fromBlock=fromBlock, toBlock=toBlock, topics=topics, blockHash=blockHash, chain=chain, counter=counter+1)