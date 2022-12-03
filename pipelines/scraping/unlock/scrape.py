import time
from ..helpers import Scraper
from ..helpers import str2bool
import os
import argparse
import argparse
import gql 
import logging 
from dotenv import load_dotenv
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log

gql_log.setLevel(logging.WARNING)

## keys
load_dotenv()
graph_api_key = os.getenv('GRAPH_API_KEY')

class LockScraper(Scraper):
    def __init__(self, bucket_name='unlock-test', allow_override=True):
        super().__init__(bucket_name, allow_override=allow_override)
        self.mainnet_url = f"https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/8u7KcVRxjtTDRgEJup3UuPJk6YoRDTHNpSMk5BEpdw42" ## what is this pattern
        self.metadata['cutoff_block'] = self.metadata.get("cutoff_block", 9227851) ## doublecheck this
        self.interval = 1000 ## default for The Graph
        self.data['locks'] = ['init'] ## lock data
        self.data['keys'] = ['init'] ## key data
        self.mainnet_url
         
    
    def call_the_graph_api(self, query, variables, counter=0):
        time.sleep(counter)
        transport = AIOHTTPTransport(url = self.mainnet_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)
        
        try: 
            logging.info(f" here  are the variables {variables}")
            logging.info(f"here is the query: {query}")
            result = client.execute(query, variables)
            countLocks = len(result['locks'])
            logging.info(f"the graph API returned this many locks: {countLocks}")
            if result.get('locks', None) == None: 
                logging.error(f"The Graph API did not return locks, counter: {counter}")
                return self.call_the_graph_api(query, variables, counter=counter+1)
            if result.get('keys', None) == None: 
                logging.error(f"The Graph API did not return  keys counter: {counter}")
                return self.call_the_graph_api(query, variables, counter=counter+1)
        except Exception as e:
            logging.error(f'An exception occured getting The Graph API {e} counter: {counter} client: {client}')
        return(result)

    def get_locks(self):
        skip = 0 
        cutoff_block = self.metadata['cutoff_block']
        retry = 0
        req = 0 
        max_req = 5
        
        while len(self.data['locks']) > 0:
            locks_before = len(self.data['locks'])
            keys_before = len(self.data['keys'])
            variables = {
                "first": self.interval, 
                "skip": skip, 
                "cutoff": cutoff_block
            }
            query = gql.gql(
            """query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
            locks(first: $first, skip: $skip, orderBy: createdAtBlock, orderDirection: desc, where: {createdAtBlock_gt: $cutoff}) {
                address
                name
                tokenAddress
                createdAtBlock
                price
                expirationDuration
                lockManagers
                keys {
                    id
                    owner
                    expiration
                    tokenURI
                }
                version
            }
            }
            """)
            result = self.call_the_graph_api(query, variables)
            locks = result['locks']
            for lock in locks:
                tmp = {
                    'address': lock['address'].lower(),
                    'name': lock['name'],
                    'blockNumber': lock['createdAtBlock'],
                    'price': lock['price'],
                    'expirationDuration': lock['expirationDuration'],
                    'lockManagers': lock['lockManagers']
                }
                self.data['locks'].append(tmp)
                skip += self.interval
                logging.info(f"Query success, skip is at: {skip}")
            for lock in locks:
                lockAddress = lock['address'].lower()
                for key in lock['keys']:
                    tmp = {
                        'lock': lockAddress,
                        'key': key['tokenAddress'].lower(),
                        'tokenId': key['id'],
                        'owner': key['owner'],
                        'expiration': key['expiration'],
                        'tokenUri': key['tokenUri']
                    }
                    self.data['keys'].append(tmp)
                retry = 0 
            logging.info(f"Query success, skip is at: {skip}")
                    
        else:
            retry += 1
            if retry > 10:
                skip += self.interval
                logging.error(f"Query success, skip is at: {skip}")
        
            
    def run(self):
        self.get_locks()
        self.save_metadata()
        self.save_data()

if __name__ == "__main__":
    scraper = LockScraper()
    scraper.run()
    logging.info("Run complete!")
