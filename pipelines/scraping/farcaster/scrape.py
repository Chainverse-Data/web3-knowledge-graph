from ..helpers import Scraper
from ...helpers import Etherscan
import logging
import json


class FarcasterScraper(Scraper):
    def __init__(self, bucket_name="farcaster"):
        super().__init__(bucket_name)

        self.register = {
            "log": "0x3cd6a0ffcc37406d9958e09bba79ff19d8237819eb2e1911f9edbce656499c87",
            "contract": "0xDA107A1CAf36d198B12c16c7B6a1d1C795978C42",
        }

        self.invite = {
            "log": "0xed994b8dfbd359de8b535931832fe533e23820fbb73ce69d8dde9bd677989f39",
            "contract": "0xe3Be01D99bAa8dB9905b33a3cA391238234B79D1",
            "implementation": "0xF73Bc3Fa2f6F774D4b6791414B1092a40Cd83831",
        }
        self.headers = {"accept": "application/json", "content-type": "application/json"}
        self.api_url = "https://fardrop.xyz/api/followers-by-username?username="
        self.start_block = 0
        self.etherscan = Etherscan()

    def save_last_block(self):
        last_block = 0
        for log in self.data["register"]:
            if log["blockNumber"] > last_block:
                last_block = log["blockNumber"]
        self.metadata["start_block"] = last_block

    def get_owners(self):
        logging.info("Getting the owners")
        logs = self.etherscan.get_decoded_event_logs(
            self.register["contract"],
            "Register",
            fromBlock=self.start_block,
            topic0=self.register["log"],
            chain="goerli",
        )
        results = []
        for log in logs:
            tmp = dict(log["args"])
            tmp["event"] = log["event"]
            tmp["transactionHash"] = log["transactionHash"].hex()
            tmp["contractAddress"] = log["address"]
            tmp["blockNumber"] = log["blockNumber"]
            results.append(tmp)

        results.sort(key=lambda x: x["blockNumber"])
        fid_map = {}
        for log in results:
            fid_map[log["id"]] = log["to"]

        self.data["register"] = results
        logging.info(f"Found {len(fid_map)} unique fIDs")
        return fid_map

    def get_names(self, fid_map):
        logging.info("Getting the names")
        abi = self.etherscan.get_smart_contract_ABI(self.invite["implementation"], chain="goerli")
        logs = self.etherscan.get_decoded_event_logs(
            self.invite["contract"],
            "Invite",
            fromBlock=self.start_block,
            topic0=self.invite["log"],
            abi=abi,
            chain="goerli",
        )
        results = []
        for log in logs:
            tmp = dict(log["args"])
            tmp["fname"] = tmp["fname"].decode()
            tmp["event"] = log["event"]
            tmp["transactionHash"] = log["transactionHash"].hex()
            tmp["contractAddress"] = log["address"]
            tmp["blockNumber"] = log["blockNumber"]
            results.append(tmp)

        results.sort(key=lambda x: x["blockNumber"])
        id_to_name = {}
        for log in results:
            id_to_name[log["inviteeId"]] = log["fname"]

        users = []
        for fid, name in id_to_name.items():
            if fid in fid_map:
                tmp = {
                    "fid": fid,
                    "name": name,
                    "address": fid_map[fid],
                }
                users.append(tmp)

        self.data["users"] = users
        self.data["invite"] = results

        logging.info(f"Found {len(users)} users")

    def get_followers(self):
        logging.info("Getting the followers")
        usernames = set(x["name"] for x in self.data["users"])
        results = []
        for username in usernames:
            url = self.api_url + username
            data = self.get_request(url, headers=self.headers, json=True, max_retries=2)
            if not data:  # pragma: no cover
                logging.warning(f"Failed to get followers for {username}")
                continue
            user_id = data["fid"]
            for entry in data["followers"]:
                tmp = {"fid": user_id, "follower": entry}
                results.append(tmp)

        self.data["followers"] = results
        logging.info(f"Found {len(results)} follower relationships")

    def run(self):
        fid_map = self.get_owners()
        self.get_names(fid_map)
        self.get_followers()
        self.save_last_block()
        self.save_data()
        self.save_metadata()


if __name__ == "__main__":
    scraper = FarcasterScraper()
    scraper.run()
    logging.info("Run complete!")
