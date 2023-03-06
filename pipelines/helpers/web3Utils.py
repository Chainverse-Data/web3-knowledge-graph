import json
import logging
import os
import time
import warnings
import requests
from ens import ENS
from web3 import Web3
from web3.logs import DISCARD
import eth_utils

class Web3Utils:
    def __init__(self) -> None:
        self.alchemy_url = f"https://eth-mainnet.g.alchemy.com/v2/{os.environ['ALCHEMY_API_KEY']}"
        self.w3 = Web3(Web3.HTTPProvider(self.alchemy_url))
        self.ns = ENS.fromWeb3(self.w3)
        self.text_records = ["avatar", "description", "display", "email", "keywords", "mail", "notice", "location", "phone", "url", "com.github", "com.peepeth", "com.linkedin", "com.twitter", "io.keybase", "org.telegram"]
        if self.w3.isConnected():
            logging.info(f"Web3 is connected!")
        else:
            raise Exception("Error connecting to Web3")

    def get_smart_contract(self, address):
        abi_endpoint = f"https://api.etherscan.io/api?module=contract&action=getabi&address={address}&apikey={os.environ['ETHERSCAN_API_KEY']}"
        abi = json.loads(requests.get(abi_endpoint).text)
        contract = self.w3.eth.contract(address, abi=abi["result"])
        return contract

    def parse_logs(self, contract, tx_hash, name):
        receipt = self.w3.eth.get_transaction_receipt(tx_hash)
        event = [abi for abi in contract.abi if abi["type"] == "event" and abi["name"] == name][0]
        decoded_logs = contract.events[event["name"]]().processReceipt(receipt, errors=DISCARD)
        return decoded_logs

    def get_ens_name(self, address, counter=0, max_retry=10):
        if counter > max_retry:
            time.sleep(counter * 10)
            return None
        try:
            domain = self.ns.name(address)
        except Exception as e:
            logging.error(f"An error occured getting the ens name for address {address}\n{e}")
            self.get_ens_name(address, counter=counter+1)
        return domain

    def get_text_record(self, name, record, counter=0, max_retry=10):
        if counter > max_retry:
            time.sleep(counter * 10)
            return None
        try:
            domain = self.ns.get_text(name, record)
        except Exception as e:
            logging.error(f"An error occured getting the record {record} for ens name {name}\n{e}")
            self.get_text_record(name, record, counter=counter+1)
        return domain

    def get_text_records(self, name, max_retry=10):
        records = {"name": name}
        for record in self.text_records:
            records[record] = self.get_text_record(name, record, max_retry=max_retry)
        return records

    def get_ens_address(self, name, counter=0, max_retry=10):
        if counter > max_retry:
            time.sleep(counter * 10)
            return None
        try:
            address = self.ns.address(name)
        except Exception as e:
            logging.error(f"An error occured getting the ens name {name}\n{e}")
            self.get_ens_address(name, counter=counter+1)
        return address

    def get_ens_info(self, name):
        warnings.filterwarnings("ignore", category=FutureWarning)

        try:
            owner = self.w3.ens.address(name=name).lower()

            x = eth_utils.crypto.keccak(eth_utils.conversions.to_bytes(text=name.replace(".eth", "")))
            token_id = eth_utils.conversions.to_int(x)

            payload = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [
                    {
                        "fromBlock": "0x0",
                        "toBlock": "latest",
                        "contractAddresses": ["0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85"],
                        "category": ["erc721"],
                        "withMetadata": True,
                        "excludeZeroValue": True,
                        "maxCount": "0x3e8",
                        "order": "desc",
                        "toAddress": owner,
                    }
                ],
            }

            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }

            response = requests.post(self.alchemy_url, json=payload, headers=headers, verify=False)
            x = json.loads(response.text)
            transfers = x["result"]["transfers"]
            flag = False
            trans = None
            for trans in transfers:
                if str(eth_utils.conversions.to_int(hexstr=trans["erc721TokenId"])) == str(token_id):
                    flag = True
                    trans = trans
                    break

            if flag:
                ens_dict = {"name": name, "address": owner, "token_id": str(token_id), "trans": trans}
                return ens_dict
            else:
                return None
        except:
            return None
