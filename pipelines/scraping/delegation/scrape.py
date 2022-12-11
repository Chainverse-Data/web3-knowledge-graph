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
        self.gitcoin_url =  "https://api.thegraph.com/subgraphs/name/messari/gitcoin-governance"
        self.metadata['cutoff_timestamp'] = self.metadata.get("cutoff_timestamp", 1620718124)
        self.data['delegateChanges'] = ['init']
        self.data['delegateVotingPowerChanges'] = ['init']
        self.data['delegates'] = ['init']
        self.data['tokenHolders'] = ['init']
        self.interval = 1000
    
    def call_the_graph_api(self, query, variables, resultType=None, counter=0):
        time.sleep(counter)
        transport = AIOHTTPTransport(url = self.gitcoin_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)

        try: 
            logging.info(f"here are the variables {variables}")
            result =  client.execute(query, variable_values=variables)
            countDelegations = len(result[resultType])
            if result.get(resultType, None) == None: 
                logging.error(f"The Graph API did not return {resultType}, counter {counter}")
                return self.call_the_graph_api(query, variables, resultType)
            elif result.get(resultType, None) == None: 
                logging.info(f"Did not receive any results, ending scrape, here are the results {result[resultType]}")
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
                    tokenAddress
                    delegate
                    delegator
                    previousDelegate
                    txnHash
                    logIndex
                }
            }      
            """)

            req += 1
            if req >= max_req:
                break
            result = self.call_the_graph_api(query, variables=variables, resultType='delegateChanges')
            logging.info(f"there have been {req} requests")
            records = len(result['delegateChanges'])
            time.sleep(2)
            skip += self.interval
            logging.info(f"there are {records} delegationEvents identified")
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
                        'blockNumber': delEvent['blockNumber'],
                        'previousDelegate': delEvent['previousDelegate'],
                        'txnHash': delEvent['txnHash']
                    }
                    lilCounter += 1
                    self.data['delegateChanges'].append(tmp)
                logging.info(f"Query success, skip is at {skip}.")
            else:
                break
    def get_delegation_value_changes(self):
        skip = 0 
        cutoff_timestamp = self.metadata['cutoff_timestamp']
        retry = 0
        req = 0 
        max_req = 6
        while len(self.data['delegateVotingPowerChanges']) > 0:
            variables = {
                "first": self.interval, 
                "skip": skip,
                "cutoff_timestamp": cutoff_timestamp
            }
            query = gql.gql("""
             query($first: Int!, $skip: Int!, $cutoff_timestamp: BigInt!) {
                delegateVotingPowerChanges(first: $first, skip: $skip, orderBy: blockTimestamp, orderDirection: desc, where: {blockTimestamp_gt: $cutoff_timestamp}) {
                    id
                    blockTimestamp
                    blockNumber 
                    delegate
                    tokenAddress
                    previousBalance
                    newBalance
                    blockTimestamp
                    logIndex
                    txnHash
                }
            }      
            """)

            req += 1
            if req >= max_req:
                break
            result = self.call_the_graph_api(query, variables=variables, resultType='delegateVotingPowerChanges')
            records = len(result['delegateVotingPowerChanges'])
            time.sleep(2)
            skip += self.interval
            logging.info(f"there are {records} delegateVotingPowerChanges identified")
            delegationValueChanges = result['delegateVotingPowerChanges']
            if len(delegationValueChanges) > 0:
                logging.info("logging delegateVotingPowerChanges events")
                lilCounter = 0
                for delEvent in delegationValueChanges: 
                    logging.info(f"logging delegateVotingPowerChanges event {lilCounter}")
                    tmp = {
                        'id': delEvent['id'],
                        'blockTimestamp': delEvent['blockTimestamp'],
                        'delegate': delEvent['delegate'],
                        'previousBalance': delEvent['previousBalance'],
                        'newBalance': delEvent['newBalance'],
                        'txnHash': delEvent['txnHash'],
                        'blockNumber': delEvent['blockNumber']
                    }
                    lilCounter += 1
                    self.data['delegateVotingPowerChanges'].append(tmp)
                logging.info(f"Query success, skip is at {skip}.")
            else:
                break


        return None 

    def get_delegates(self):
        skip = 0 
        cutoff_timestamp = self.metadata['cutoff_timestamp']
        retry = 0
        req = 0 
        max_req = 6
        delegates = len(self.data['delegates'])
        while len(self.data['delegates']) > 0:
            variables = {
                "first": self.interval, 
                "skip": skip
            }
            query = gql.gql("""
             query($first: Int!, $skip: Int!) {
                delegates(first: $first, skip: $skip, orderBy: delegatedVotes, orderDirection: desc) {
                    id
                    delegatedVotesRaw
                    delegatedVotes
                    tokenHoldersRepresented
                        {
                            id
                        }
                    numberVotes
                }
            }      
            """)

            req += 1
            if req >= max_req:
                break
            result = self.call_the_graph_api(query, variables=variables, resultType='delegates')
            records = len(result['delegates'])
            time.sleep(2)
            skip += self.interval
            logging.info(f"there are {records} delegates identified")
            delegates = result['delegates']
            if len(delegates) > 0:
                logging.info("logging delegates events")
                lilCounter = 0
                for delEvent in delegates: 
                    logging.info(f"logging delegates event {lilCounter}")
                    tmp = {
                        'delegateAddress': delEvent['id'],
                        'delegatedVotesRaw': delEvent['delegatedVotesRaw'],
                        'delegatedVotes': delEvent['delegatedVotes'],
                        'tokenHoldersRepresented': delEvent['tokenHoldersRepresented'],
                        'numberOfVotes': delEvent['numberVotes'],
                       ## 'proposals': delEvent['proposals']
                    }
                    lilCounter += 1
                    self.data['delegates'].append(tmp)
                logging.info(f"Query success, skip is at {skip}.")
            else:
                break




    def get_token_holders(self):
        skip = 0
        cutoff_timestamp = self.metadata['cutoff_timestamp']
        retry = 0
        req = 0
        max_req = 6
        tokenHolders = len(self.data['tokenHolders'])
        while len(self.data['tokenHolders']) > 0:
            variables = {
                "first": self.interval,
                "skip": skip
            }
            query = gql.gql("""
                query($first: Int!, $skip: Int!) {
                    tokenHolders(first: $first, skip: $skip, orderBy: tokenBalance, orderDirection: desc) {
                        id
                        tokenBalance
                        totalTokensHeld
                        }
                    }
            """)
            req += 1
            if req >= max_req:
                break 
            result = self.call_the_graph_api(query, variables=variables, resultType='tokenHolders')
            records = len(result['tokenHolders'])
            time.sleep(2)
            skip += self.interval
            logging.info(f"there are {records} tokenHolders identified")
            tokenHolders = result['tokenHolders']
            if len(tokenHolders) > 0:
                for holder in tokenHolders:
                    lilCounter = 0
                    tmp = {
                        'tokenHolderAddress': holder['id'],
                        'tokenBalance': holder['tokenBalance'],
                        'totalTokensHeld': holder['totalTokensHeld']
                    }
                    self.data['tokenHolders'].append(tmp)
                    lilCounter += 1
                    self.data['tokenHolders'].append(tmp)
                logging.info(f"Query success, skip is at {skip}.")
            else:
                break


    def run(self):
        self.get_delegation_events()
        self.get_delegation_value_changes()
        self.get_delegates()
        self.get_token_holders()
        self.save_metadata()
        self.save_data()
    
if __name__ == '__main__':
    scraper = DelegationScraper()
    scraper.run()
    logging.info("Run complete. Good job dev.")

                    
            
            
        
