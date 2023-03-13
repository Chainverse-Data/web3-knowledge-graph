import os

from .cyphers import ENSLooupCyphers
from ..helpers import Processor
from ...helpers import Web3Utils
from tqdm import tqdm

class ENSLookupProcessor(Processor):
    def __init__(self, bucket_name="ens-lookup"):
        self.cyphers = ENSLooupCyphers()
        self.web3utils = Web3Utils()
        self.chunk_size = 1000
        super().__init__(bucket_name)
    
    def process_ens_names(self):
        names = [result["name"] for result in self.cyphers.get_ens_names()]
        for i in tqdm(range(0, len(names), self.chunk_size)):
            records = self.parallel_process(self.web3utils.get_text_records, names[i: i+self.chunk_size], description="Getting ENS address associated with the names")
            results = []
            for record in records:
                record["twitter"] = record.pop("com.twitter")
                record["github"] = record.pop("com.github")
                record["peepeth"] = record.pop("com.peepeth")
                record["linkedin"] = record.pop("com.linkedin")
                record["keybase"] = record.pop("io.keybase")
                record["telegram"] = record.pop("org.telegram")
                results.append(record)
            
            urls = self.save_json_as_csv(results, self.bucket_name, f"processing_ens_records_{self.asOf}_{i}")
            self.cyphers.update_ens(urls)

            emails = [{"name": record["name"], "email": record["email"]} for record in results if record["email"]]
            urls = self.save_json_as_csv(emails, self.bucket_name, f"processing_ens_records_emails_{self.asOf}_{i}")
            self.cyphers.create_or_merge_records(urls, "Email", "email")
            self.cyphers.link_or_merge_records(urls, "Email", "email")

            twitters = [{"name": record["name"], "handle": record["twitter"]} for record in results if record["twitter"]]
            urls = self.save_json_as_csv(twitters, self.bucket_name, f"processing_ens_records_twitters_{self.asOf}_{i}")
            self.cyphers.create_or_merge_records(urls, "Twitter", "handle")
            self.cyphers.link_or_merge_records(urls, "Twitter", "handle")
            
            githubs = [{"name": record["name"], "handle": record["github"]} for record in results if record["github"]]
            urls = self.save_json_as_csv(githubs, self.bucket_name, f"processing_ens_records_githubs_{self.asOf}_{i}")
            self.cyphers.create_or_merge_records(urls, "Github", "handle")
            self.cyphers.link_or_merge_records(urls, "Github", "handle")
            
            peepeths = [{"name": record["name"], "handle": record["peepeth"]} for record in results if record["peepeth"]]
            urls = self.save_json_as_csv(peepeths, self.bucket_name, f"processing_ens_records_peepeths_{self.asOf}_{i}")
            self.cyphers.create_or_merge_records(urls, "Peepeth", "handle")
            self.cyphers.link_or_merge_records(urls, "Peepeth", "handle")
            
            linkedins = [{"name": record["name"], "handle": record["linkedin"]} for record in results if record["linkedin"]]
            urls = self.save_json_as_csv(linkedins, self.bucket_name, f"processing_ens_records_linkedins_{self.asOf}_{i}")
            self.cyphers.create_or_merge_records(urls, "Linkedin", "handle")
            self.cyphers.link_or_merge_records(urls, "Linkedin", "handle")
            
            keybases = [{"name": record["name"], "handle": record["keybase"]} for record in results if record["keybase"]]
            urls = self.save_json_as_csv(keybases, self.bucket_name, f"processing_ens_records_keybases_{self.asOf}_{i}")
            self.cyphers.create_or_merge_records(urls, "Keybase", "handle")
            self.cyphers.link_or_merge_records(urls, "Keybase", "handle")
            
            telegrams = [{"name": record["name"], "handle": record["telegram"]} for record in results if record["telegram"]]
            urls = self.save_json_as_csv(telegrams, self.bucket_name, f"processing_ens_records_telegrams_{self.asOf}_{i}")
            self.cyphers.create_or_merge_records(urls, "Telegram", "handle")
            self.cyphers.link_or_merge_records(urls, "Telegram", "handle")
        
    def run(self):
        self.process_ens_names()

if __name__ == "__main__":
    P = ENSLookupProcessor()
    P.run()



