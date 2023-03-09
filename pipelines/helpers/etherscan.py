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
        