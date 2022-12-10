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
        self.gitcoin_url =   f'https://gateway.thegraph.com/api/{GRAPH_API_KEY}/subgraphs/id/QmQXuWMr5zgDWwHtc4MzzJkkWZinTZQjGEQe5ES9BtQ5Bf'
        self.metadata['cutoff_block'] = self.metadata.get("cutoff_block", 13018595)
        self.data['delegationValues'] = ['init']
        self.data['delegationEvents'] = ['init']
        self.data['delegates'] = ['init']
        self.data['tokenHolders'] = ['init']
        self.interval = 1000
    
    def call_the_graph_api(self, query, variables, counter=0):
        time.sleep(counter)
        transport = AIOHTTPTransport(url = self.gitcoin_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)

        try: 
            logging.info(f"here are the variables {variables}")
            logging.info(f"here is the query {query}")
            logging.info(f"self.gitcoin_url {self.gitcoin_url}")
            result = client.execute(query, variables)
            countDelegations = len(result['delegateValues'])
            if result.get('delegationEvents', None) == None: 
                logging.error(f"The Graph API did not return delegationEvents, counter {counter}")
                return self.call_the_graph_api(query, variables, counter=counter+1)
            elif result.get('delegationEvents', None) == None: 
                logging.info(f"Did not receive any results, ending scrape, here are the results {result['delegationEvents']}")
                return self.call_the_graph_api(query, variables, counter=counter+1)
        except Exception as e:
            str_exc = str(e)
            logging.error(f'An exception occured querying The Graph API {e} counter: {counter} client: {client}')
            logging.error(f"Here is the exception: {str_exc}")
            return self.call_the_graph_api(query, variables, counter=counter+1)
        return result 
    
    def get_delegation_events(self):
        skip = 0 
        cutoff_block = self.metadata['cutoff_block']
        retry = 0
        req = 0 
        max_req = 20
        delegationEventsBefore = len(self.data['delegationEvents'])
        while len(self.data['delegationEvents']) > 0:
            variables = {
                "first": self.interval, 
                "skip": skip, 
                "cutoff_block": self.metadata['cutoff_block']
            }
            query = """
                query getDelegationEvents($first: Int!, $skip: Int!, $cutoff_block: Int!) {
                delegationEvents(first: $first, skip: $skip, orderByDirection: "desc", where: {blockNumber_gt: $cutoff_block}) {
                    id
                    blockNumber
                    timestamp
                    delegate
                    delegator
                    amount
                    }
                }
            """
            result = self.call_the_graph_api(query, variables)
            time.sleep(2)
            self.data['delegationEvents'] = result['delegationEvents']
            skip += self.interval
            req += 1
            delegationEventsAfter = len(self.data['delegationEvents'])
            if req > max_req:
                break
            delegationEvents = result['delegationEvents']
            if len(delegationEvents) > 0:
                for delEvent in delegationEvents: 
                    tmp = {
                        'id': delEvent['id'],
                        'blockNumber': delEvent['blockNumber'],
                        'timestamp': delEvent['timestamp'],
                        'delegate': delEvent['delegate'],
                        'delegator': delEvent['delegator'],
                        'amount': delEvent['amount'],
                        'type': delEvent['type']
                    }
                    self.data['delegationEvents'].append(tmp)
                skip += self.interval
                logging.info(f"Query success, skip is at {skip}.")
            else:
                retry += 1
                if retry >= 5:
                    skip += self.interval
                    break 
    def run(self):
        self.get_delegation_events()
        self.save_metadata()
        self.save_data()
    
if __name__ == '__main__':
    scraper = DelegationScraper()
    scraper.run()
    logging.info("Run complete. Good job dev.")

                    
            
            
        
