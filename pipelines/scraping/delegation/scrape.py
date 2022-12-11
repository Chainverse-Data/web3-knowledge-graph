import time
from ..helpers import Scraper
from ..helpers import str2bool
import os
import argparse
import gql 
import logging 
from dotenv import load_dotenv
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log

GRAPH_API_KEY = os.getenv('GRAPH_API_KEY')

gql_log.setLevel(logging.WARNING)

class DelegationScraper(Scraper):
    def __init__(self, bucket_name='gitcoin-delegation-test', allow_override=True):
        super().__init__(bucket_name, allow_override=allow_override)
        self.gitcoin_url =   "https://api.thegraph.com/subgraphs/name/messari/gitcoin-governance"
        self.metadata['cutoff_timestamp'] = self.metadata.get("cutoff_timestamp", 1620718124)
        self.data['delegateChanges'] = ['init']
        self.interval = 1000
    
    def call_the_graph_api(self, query, variables, counter=1):
        time.sleep(counter)
        transport = AIOHTTPTransport(url = self.gitcoin_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)

        try: 
            logging.info(f"here are the variables {variables}")
            result =  client.execute(query, variable_values=variables)
            countDelegations = len(result['delegateChanges'])
            if result.get('delegateChanges', None) == None: 
                logging.error(f"The Graph API did not return delegateChanges, counter {counter}")
                return self.call_the_graph_api(query, variables, counter=counter+1)
            elif result.get('delegateChanges', None) == None: 
                logging.info(f"Did not receive any results, ending scrape, here are the results {result['delegateChanges']}")
                return self.call_the_graph_api(query, variables, counter=counter+1)
        except Exception as e:
            str_exc = str(e)
            logging.error(f'An exception occured querying The Graph API {e} counter: {counter} client: {client}')
            logging.error(f"Here is the exception: {str_exc}") 
            return self.call_the_graph_api(query, variables, counter=counter+1)
        return result 
    
    def get_delegation_events(self):
        skip = 0 
        cutoff_timestamp = self.metadata['cutoff_timestamp']
        retry = 0
        req = 0 
        max_req = 5
        delegationEventsBefore = len(self.data['delegateChanges'])
        while len(self.data['delegateChanges']) > 0:
            variables = {
                "first": self.interval, 
                "skip": skip,
                "cutoff_timestamp": cutoff_timestamp
            }
            query = gql.gql("""
             query($first: Int!, $skip: Int!, $cutoff_timestamp: BigInt!) {
                delegateChanges(first: $first, skip: $skip, orderBy: blockTimestamp, orderDirection: desc, where: {blockTimestamp_gt: $cutoff_timestamp}) {
                    id
                    blockTimestamp
                    blockNumber
                    delegate
                    delegator
                }
            }      
            """)
            result = self.call_the_graph_api(query, variables=variables)
            records = len(result['delegateChanges'])
            time.sleep(2)
            skip += self.interval
            req += 1
            delegationEventsAfter = len(self.data['delegateChanges'])
            logging.info(f"there are {delegationEventsAfter} delegationEvents events")
            logging.info(f"there are {records} delegationEvents identified")
            if req > max_req:
                break
            delegationEvents = result['delegateChanges']
            if len(delegationEvents) > 0:
                logging.info("logging delegation events")
                lilCounter = 0
                for delEvent in delegationEvents: 
                    logging.info(f"logging delegation event {lilCounter}")
                    tmp = {
                        'id': delEvent['id'],
                        'blockTimestamp': delEvent['blockTimestamp'],
                        'delegate': delEvent['delegate'],
                        'delegator': delEvent['delegator'],
                    }
                    lilCounter += 1
                    self.data['delegateChanges'].append(tmp)
                skip += self.interval
                logging.info(f"Query success, skip is at {skip}.")
            else:
                retry += 1
                if retry >= 5:
                    break 
    def run(self):
        self.get_delegation_events()
        self.save_metadata()
        self.save_data()
    
if __name__ == '__main__':
    scraper = DelegationScraper()
    scraper.run()
    logging.info("Run complete. Good job dev.")

                    
            
            
        
