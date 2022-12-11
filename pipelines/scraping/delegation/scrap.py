import time
from ..helpers import Scraper
from ..helpers import str2bool
import os
import argparse
import gql 
import logging 
from dotenv import load_dotenv
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log

gql_log.setLevel(logging.WARNING)

load_dotenv()
graph_api_key = os.getenv('GRAPH_API_KEY')


class DelegationScraper(Scraper):
  def __init__(self, bucket_name='gitcoin-delegation-test', allow_override=True):
    super().__init__(bucket_name, allow_override=allow_override)
    self.gitcoin_url =   f'https://gateway.thegraph.com/api/{graph_api_key}/subgraphs/id/QmQXuWMr5zgDWwHtc4MzzJkkWZinTZQjGEQe5ES9BtQ5Bf'
    self.metadata['cutoff_block'] = self.metadata.get("cutoff_block", 13018595)
    self.data['delegations'] = ['init']
    self.interval = 1000

  def call_the_graph_api(self, query, variables):
    counter = 2
    time.sleep(counter)
    logging.info("sleeping for {counter} seconds")
    transport = AIOHTTPTransport(url = self.gitcoin_url)
    client = gql.Client(transport=transport, fetch_schema_from_transport=True)
    logging.info("client created")
    try: 
      logging.info(f"here  are the variables {variables}")
      logging.info(f"here is the query: {query}")
      result = client.execute(query, variables)
      countDelegations = len(result['delegations'])
      logging.info(f"the graph API returned this many delegations: {countDelegations}")
      if result.get('degates', None) == None: 
        logging.error(f"The Graph API did not return delegates, counter: {counter}")
        return self.call_the_graph_api(query, variables, counter=counter+1)
      elif result.get('delegations', None) == None: 
        logging.info(f"Not receiving any more results, ending scrape: here are the results {result['delegates']}")
    except Exception as e:
        logging.error(f'An exception occured getting The Graph API {e} counter: {counter} client: {client}')
    return result 
    

  def get_delegations(self):
    print('starting')
    skip = 0 
    cutoff_block = self.metadata['cutoff_block']
    retry = 0
    req = 0 
    max_req = 5
    print(max_req)
    delegationsBefore = len(self.data['delegations'])
    while len(self.data['delegations']) > 0:
        variables = {
          "first": self.interval, 
          "skip": skip, 
          "cutoff": cutoff_block
        }
        print(variables)
        query = gql.gql(
        """query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
  delegation(first: $first, skip: $skip, orderBy: createdAtBlock, orderDirection: desc, where: {createdAtBlock_gt: $cutoff}) {
    delegateChanges {
      id
      tokenAddress
      delegator
      delegate
      previousDelegate
      blockTimestamp
      blockNumber
      txnHash
    }
    delegateVotingPowerChanges {
      id
      tokenAddress
      previousBalance
      newBalance
      blockTimestamp
      txnHash
      blockNumber
    }
    tokenHolders {
      id
      delegate {
        id
      }
      tokenBalance
      tokenBalanceRaw
    }
    delegates {
      id
      delegatedVotesRaw
      delegatedVotes
      tokenHoldersRepresentedAmount
    }
  }
}""")
    logging.info("Starting to query...")
    result = self.call_the_graph_api(query, variables)
    if result != None: 
      delegations = result['delegation']
      for delegation in delegations['delegateChanges']:
        tmp = {
          'id': delegation['id'],
          'tokenAddress': delegation['tokenAddress']
          }
        self.data['delegations'].append(tmp)
        skip += self.interval
        retry = 0
        logging.info(f"Query success, skip is at: {skip}")
    else:
      retry += 1
      if retry > 10:
        skip += self.interval


  def run(self):
    self.get_delegations()
    self.save_metadata()
    self.save_data()

if __name__ == "__main__":
    scraper = DelegationScraper()
    logging.info("Fetching delegations...")
    scraper.run()
    logging.info("Run complete!")
