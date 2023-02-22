import time
from ..helpers import Scraper
import os
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

        ## graph urls
        self.mainnet_url = f"https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/8u7KcVRxjtTDRgEJup3UuPJk6YoRDTHNpSMk5BEpdw42" ## what is this pattern
        self.mainnet_url_updated = f"https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/QmRiwYpBoPESYDf3UhneJc1veUWp1JHUc1rVuT8RWdndCW" ## what is this pattern
        self.polygon_url = f"https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/QmSdGt5e3nymtWY6EhMxfkHCjF9Asr4vvjmtaiKuy6qwAi"
        self.xdai_url = f"https://gateway.thegraph.com/{graph_api_key}/subgraphs/id/QmUa26Qj9MZn2fmD67SzegPiHSHvFfgYytjFAmxK7au6MY"
        self.optimism_url = f"https://gateway.thegraph.com/{graph_api_key}/subgraphs/id/QmYWbG6fFM9uaq5zYLwJABSnycGU6s9D9FuqHLDREXhCWP"

        self.metadata['cutoff_block'] = self.metadata.get("cutoff_block", 9227851) ## doublecheck this
        self.interval = 1000 ## default for The Graph
        self.data['locks'] = ['init'] ## lock data
        self.data['keys'] = ['init'] ## key data
        self.mainnet_url
        
        ## strategy
        ### the non mainnet locks have a slightly different ontology. 
        ### so i can extend `call_the_graph_api` to take a url as an argument and then just call it with the appropriate url, and then rewrite ingestors
        ### orrr i can extend `call_the_graph_api` to get the new data, and rewrite other functions for the other networks. 
        ### probably should rewrite everything. but i guess this will work in the meantime
    
    def call_the_graph_api(self, query, variables, counter=0):
        time.sleep(counter)
        transport = AIOHTTPTransport(url = self.mainnet_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)
        
        try: 
            logging.info(f"here  are the variables {variables}")
            logging.info(f"here is the query: {query}")
            result = client.execute(query, variable_values=variables)
            countLocks = len(result['locks'])
            countKeys = len(result['keys'])
            logging.info(f"the graph API returned this many locks: {countLocks}")
            logging.info(f"the graph API returned this many keys: {countKeys}")
            if result.get('locks', None) == None: 
                logging.error(f"The Graph API did not return locks, counter: {counter}")
                return self.call_the_graph_api(query, variables, counter=counter+1)
            elif result.get('keys', None) == None: 
                logging.info(f"Not receiving any more results, ending scrape: here are the results {result['locks']}")
        except Exception as e:
            logging.error(f'An exception occured getting The Graph API {e} counter: {counter} client: {client}')
        return(result)

    def get_locks(self):
        skip = 0 
        cutoff_block = self.metadata['cutoff_block']
        retry = 0
        req = 0 
        max_req = 5
        locks_before = len(self.data['locks'])
        
        while len(self.data['locks']) > 0:
            
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
            time.sleep(2) ## need to add actual logic here for when to sleep 
            locks = result['locks']
            if len(locks) > 0:
                for lock in locks:
                    tmp = {
                        'address': lock['address'].lower(),
                        'name': lock['name'],
                        'blockNumber': lock['createdAtBlock'],
                        'price': lock['price'],
                        'expirationDuration': lock['expirationDuration'],
                        'lockManagers': lock['lockManagers'],
                        'keys': lock['keys']
                    }
                    self.data['locks'].append(tmp)
                    print(tmp)
                for lock in locks:
                    address = lock['address'].lower()
                    for key in lock['keys']:
                        tmp = {
                            'lockAddress': address,
                            'keyId': key['id'],
                            'owner': key['owner'],
                            'expiration': key['expiration'],
                            'tokenURI': key['tokenURI']
                        }
                        self.data['keys'].append(tmp)
                skip += self.interval
                logging.info(f"Query success, skip is at: {skip}")                    
            else: ## changing
                retry += 1
                if retry > 5:
                    skip += self.interval
                    break ## trying to break if we don't get results

    ## def get_key_transactions(self): 
        ## skip = 0 ###

            
    def run(self):
        self.get_locks()
        self.save_metadata()
        self.save_data()

if __name__ == "__main__":
    scraper = LockScraper()
    scraper.run()
    logging.info("Run complete!")
