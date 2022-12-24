import time
from ..helpers import Scraper
from ..helpers import str2bool
import os
import argparse
import gql 
import pandas as pd
import logging 
from dotenv import load_dotenv
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log

gql_log.setLevel(logging.WARNING)

## keys
load_dotenv()
graph_api_key = os.getenv('GRAPH_API_KEY')

class LockScraper(Scraper):
    def __init__(self, bucket_name='unlock-demo', allow_override=True):
        super().__init__(bucket_name, allow_override=allow_override)

        ## graph urls
        self.graph_urls = [
            {
                "network": "mainnet",
                "url": "https://api.thegraph.com/subgraphs/name/unlock-protocol/unlock"
            },
            {
                "network": "mainnet", 
                "url": "https://thegraph.com/hosted-service/subgraph/unlock-protocol/polygon-v2"
            },
            {
                "network": "xdai",
                "url": "https://thegraph.com/hosted-service/subgraph/unlock-protocol/xdai"
            },
            {
                "network": "optimism",
                "url": "https://api.thegraph.com/subgraphs/name/unlock-protocol/optimism"
            },
            {
                "network": "arbitrum",
                "url": "https://api.thegraph.com/subgraphs/name/unlock-protocol/arbitrum-v2"
            }
        ]

        self.metadata['cutoff_block'] = self.metadata.get("cutoff_block", 9227851) ## doublecheck this
        self.interval = 1000 ## default for The Graph
        self.data['locks'] = ['init'] ## lock data
        self.data['managers'] = ['init']
        self.data['keys'] = ['init'] ## key data
        self.data['holders'] = ['init']
    
    def call_the_graph_api(self, graph_url, network, query, variables, counter=0):
        time.sleep(counter)
        transport = AIOHTTPTransport(url = graph_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)
        
        try: 
            logging.info(f"here  are the variables {variables}")
            logging.info(f"here is the query: {query}")
            result = client.execute(query, variables)
            countLocks = len(result['locks'])
            countKeys = len(result['keys'])
            logging.info(f"the graph API returned this many locks: {countLocks}")
            logging.info(f"the graph API returned this many locks: {countKeys}")
            if result.get('locks', None) == None: 
                logging.error(f"The Graph API did not return locks, counter: {counter}")
                return self.call_the_graph_api(query, variables, counter=counter+1)
            elif result.get('locks', None) == None: 
                logging.info(f"Not receiving any more results, ending scrape: here are the results {result['locks']}")
        except Exception as e:
            logging.error(f'An exception occured getting The Graph API {e} counter: {counter} client: {client}')
        return(result)

    def get_locks(self):
        for url in self.graph_urls:
            graph_url = url['url']
            logging.info(graph_url)
            network = url['network']
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

                locks_query = gql.gql(
                    """
                    query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
                    locks(first: $first, skip: $skip, orderBy: creationBlock, orderDirection: desc, where: {creationBlock_gt: $cutoff}) {
                            id
                            address
                            name
                            tokenAddress
                            creationBlock
                            price
                            expirationDuration
                            totalSupply
                            LockManagers {
                                id
                                address
                            }
                            keys {
                                id
                                keyId
                                owner { 
                                    id
                                    address
                                }
                                expiration
                                tokenURI
                                createdAt
                            }
                        }
                        }
                        """)

                result = self.call_the_graph_api(query=locks_query, variables=variables, graph_url=graph_url, network=network)
                time.sleep(2)
                locks = result['locks']
                for l in locks:
                    locks_tmp = {
                        "address": l['address'].lower(),
                        "id": l['id'],
                        "name": l['name'],
                        "tokenAddress": l['tokenAddress'],
                        "creationBlock": l["creationBlock"],
                        "price": l["price"],
                        "expirationDuration": l["expirationDuration"],
                        "totalSupply": l["totalSupply"],
                        "network": network
                    }
                    self.data['locks'].append(locks_tmp)
                    df = pd.DataFrame(self.data['locks'])
                    df.to_csv('locksss.csv')
                for lc in locks:
                    for manager in lc['LockManagers']:
                        managers_tmp = {
                            "id": manager['id'],
                            "lock": lc['tokenAddress'],
                            "address": manager['address']
                        }
                        self.data['managers'].append(managers_tmp)
                        df = pd.DataFrame(self.data['managers'])
                        df.to_csv('managerss.csv')
                for l in locks:
                    address = l['tokenAddress']
                    for k in l['keys']:
                        keyId = k['keyId']
                        keys_tmp = {
                            "id": k['id'],
                            "address": address,
                            "keyId": k['keyId'],
                            "expiration": k['expiration'],
                            "tokenURI": k['tokenURI'],
                            "createdAt": k['createdAt'],
                            "network": network
                        }
                        self.data['keys'].append(keys_tmp)
                        df = pd.DataFrame(self.data['keys'])
                        df.to_csv('keysss.csv') 
                        
                        holders_tmp = {
                            "id": k['owner']['id'],
                            "address": k['owner']['address'],
                            "keyId":  keyId, 
                            "address": address
                        }

                        self.data['holders'].append(holders_tmp)
                        df = pd.DataFrame(self.data['holders'])
                        df.to_csv("holdeerss.csv")
                if skip < 5000:
                    skip += self.interval 
                    logging.info(f"Query success, skip is at: {skip}")
                else: 
                    logging.info("Next URL")
                    break                
            
    def run(self):
        self.get_locks()
        self.save_metadata()
        self.save_data()

if __name__ == "__main__":
    scraper = LockScraper()
    scraper.run()
    logging.info("Run complete!")
