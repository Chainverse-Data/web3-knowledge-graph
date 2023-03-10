import os
import time
from . import Requests

class Etherscan(Requests):
    def __init__(self) -> None:
        self.etherescan_api_url = f"https://api.etherscan.io/api"
        self.headers = {"Content-Type": "application/json"}
        self.pagination_count = 1000
        super().__init__()

    def get_token_holders(self, tokenAddress, page=1, offset=1000, counter = 0):
        """
            Helper method to get the token holders of any token from Etherscan
            parameters:
                - tokenAddress: (address) The contract address that is of interest
                - offset: (int) To change the number of results returned by each query, max 1000. You should probably not touch this.
        """
        results = []
        time.sleep(counter)
        if counter > 5:
            return None

        params = {
            "apikey": os.environ['ETHERSCAN_API_KEY'],
            "module":"token",
            "action":"tokenholderlist",
            "contractaddress": tokenAddress,
            "page": page,
            "offset": offset
        }
        content = self.get_request(self.etherescan_api_url, params=params, headers=self.headers, json=True)
        if content and "result" in content:
            result = content["result"]
            if len(result) > 0:
                nextResult = self.get_token_holders(tokenAddress, page=page+1, offset=offset)
                if nextResult:
                    results.extend(nextResult)
            results.extend(result)
        else:
            self.get_token_holders(tokenAddress, page=page, offset=offset, counter=counter+1)
        return results
        
    def get_token_information(self, tokenAddress, counter = 0):
        time.sleep(counter)
        if counter > 5:
            return None
        
        params = {
            "module":"token",
            "action":"tokeninfo",
            "contractaddress":tokenAddress,
            "apikey": os.environ['ETHERSCAN_API_KEY']
        }

        content = self.get_request(self.etherescan_api_url, params=params, headers=self.headers, json=True)
        if content and "result" in content and len(content["result"])>0:
            result = content["result"]
            return result[0]
        else:
            self.get_token_information(tokenAddress, counter=counter+1)