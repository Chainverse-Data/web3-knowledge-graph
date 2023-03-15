import os
import time
from . import Requests

class Etherscan(Requests):
    def __init__(self, max_retries=5) -> None:
        self.chains = ["ethereum", "optimism", "polygon"]
        self.etherscan_api_url = {
            "ethereum": "https://api.etherscan.io/api",
            "optimism": "https://api-optimistic.etherscan.io/api",
            "polygon": "https://api.polygonscan.com/api",
            "arbitrum": "https://api.arbiscan.io/api",
            "binance": "https://api.bscscan.com/api",
        }
        self.etherscan_api_keys = {
            "ethereum": os.environ.get('ETHERSCAN_API_KEY', ""),
            "optimism": os.environ.get('ETHERSCAN_API_KEY_OPTIMISM', ""),
            "polygon": os.environ.get('ETHERSCAN_API_KEY_POLYGON', ""),
            "arbitrum": os.environ.get('ETHERSCAN_API_KEY_ARBITRUM', ""),
            "binance": os.environ.get('ETHERSCAN_API_KEY_BINANCE', ""),
        }
        self.headers = {"Content-Type": "application/json"}
        self.pagination_count = 1000
        self.max_retries = max_retries
        super().__init__()


    def get_last_block_number(self, chain="ethereum", counter=0):
        """
            Helper method to get the latests block number.
            parameters:
                - chain: (ethereum|optimism|polygon) the chain of interest
        """
        time.sleep(counter)
        if counter > self.max_retries:
            return None

        params = {
            "module": "proxy",
            "action": "eth_blockNumber",
            "apikey": self.etherscan_api_keys[chain]
        }

        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if content and type(content) == dict and "result" in content:
            block_number = int(content["result"], 16)
        else:
            return self.get_last_block_number(chain=chain, counter=counter+1)
        return block_number

    def get_token_holders(self, tokenAddress, page=1, offset=1000, chain="ethereum", counter = 0):
        """
            Helper method to get the token holders of any token from Etherscan
            parameters:
                - tokenAddress: (address) The contract address that is of interest
                - offset: (int) To change the number of results returned by each query, max 1000. You should probably not touch this.
                - chain: (ethereum|optimism|polygon) the chain of interest
        """
        results = []
        time.sleep(counter)
        if counter > self.max_retries:
            return None

        params = {
            "apikey": self.etherscan_api_keys[chain],
            "module":"token",
            "action":"tokenholderlist",
            "contractaddress": tokenAddress,
            "page": page,
            "offset": offset
        }
        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if content and type(content) == dict and "result" in content:
            result = content["result"]
            if len(result) > 0:
                nextResult = self.get_token_holders(tokenAddress, page=page+1, offset=offset)
                if nextResult:
                    results.extend(nextResult)
            results.extend(result)
        else:
            return self.get_token_holders(tokenAddress, page=page, offset=offset, counter=counter+1)
        return results
        
    def get_token_information(self, tokenAddress, chain="ethereum", counter = 0):
        """
            Helper method to get the token metadata of any token from Etherscan
            parameters:
                - tokenAddress: (address) The contract address that is of interest
                - chain: (ethereum|optimism|polygon) the chain of interest
        """
        time.sleep(counter)
        if counter > self.max_retries:
            return None
        
        params = {
            "module":"token",
            "action":"tokeninfo",
            "contractaddress":tokenAddress,
            "apikey": self.etherscan_api_keys[chain]
        }

        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if content and type(content) == dict and "result" in content and type(content["result"]) == list and len(content["result"]) > 0:
            result = content["result"]
            return result[0]
        else:
            self.get_token_information(tokenAddress, counter=counter+1)

    def get_contract_deployer(self, contractAddresses, chain="ethereum", counter=0):
        """
            Helper method to get the address of the deployer of a contract.
            parameters:
                - contractAddresses: ([address]) An array of contract addresses, up to 5 address!
                - chain: (ethereum|optimism|polygon) the chain of interest
        """

        assert len(contractAddresses) <= 5, "contractAddress cannot be more than 5 addresses"

        time.sleep(counter)
        if counter > self.max_retries:
            return None
        
        params = {
            "module":"contract",
            "action":"getcontractcreation",
            "contractaddresses": ",".join(contractAddresses),
            "apikey": self.etherscan_api_keys[chain]
        }
        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if content and type(content) == dict and "result" in content and type(content["result"]) == list and len(content["result"])>0:
            result = content["result"]
            return result
        else:
            self.get_contract_deployer(contractAddresses, counter=counter+1)

    def get_internal_transactions(self, address, startBlock, endBlock, page=1, offset=10000, sort="asc", chain="ethereum", counter=0):
        """
            Helper method to get the internal transactions of a smart contract.
            parameters:
                - address: (address) A contract addresses.
                - startBlock: (int) The starting block to get transactions from
                - endBlock: (int) The end block to get transactions from
                - sort: (asc|desc) The sorting order.
                - chain: (ethereum|optimism|polygon) the chain of interest
        """
        results = []
        time.sleep(counter)
        if counter > self.max_retries:
            return None

        params = {
            "module":"account",
            "action":"txlistinternal",
            "address": address,
            "startblock":startBlock,
            "endblock":endBlock,
            "page":page,
            "offset":offset,
            "sort": sort,
            "apikey": self.etherscan_api_keys[chain]
        }

        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if content["message"] == "No transactions found":
            return results
        if content and type(content) == dict and "result" in content and type(content["result"]) == list:
            result = content["result"]
            if len(result) > 0:
                nextResult = self.get_internal_transactions(address, startBlock, endBlock, sort=sort, page=page+1, offset=offset)
                if nextResult:
                    results.extend(nextResult)
            results.extend(result)
        else:
            return self.get_internal_transactions(address, startBlock, endBlock, sort=sort, page=page+1, offset=offset, counter=counter+1)
        return results

    def get_smart_contract_ABI(self, address, chain="ethereum", counter=0):
        """
            Helper method to get the ABI of a published smart contract. The smart contract needs to have verified its ABI.
            parameters:
                - address: (address) A contract addresses.
                - chain: (ethereum|optimism|polygon) the chain of interest
        """
        time.sleep(counter)
        if counter > self.max_retries:
            return None
        
        params = {
            "module":"contract",
            "action":"getabi",
            "address": address,
            "apikey": self.etherscan_api_keys[chain]
        }
        
        content = self.get_request(self.etherscan_api_url[chain], params=params, headers=self.headers, json=True)
        if content and type(content) == dict and "result" in content:
            result = content["result"]
            return result
        else:
            self.get_smart_contract_ABI(address, counter=counter+1)