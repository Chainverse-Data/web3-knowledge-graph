import time
from ..helpers import Scraper
import os
import gql
import logging
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log

gql_log.setLevel(logging.WARNING)


class DelegationScraper(Scraper):
    def __init__(self, bucket_name="gab", allow_override=True):
        super().__init__(bucket_name, allow_override=allow_override)
        self.graph_urls = [
            {
                "protocol": "ampleforth",
                "url": "https://thegraph.com/hosted-service/subgraph/messari/ampleforth-governance",
            },
            {
                "protocol": "compound",
                "url": "https://thegraph.com/hosted-service/subgraph/messari/compound-governance",
            },
            {
                "protocol": "cryptex",
                "url": "https://thegraph.com/hosted-service/subgraph/messari/cryptex-governance",
            },
            {
                "protocol": "gitcoin",
                "url": "https://thegraph.com/hosted-service/subgraph/messari/gitcoin-governance",
            },
            {
                "protocol": "idle",
                "url": "https://thegraph.com/hosted-service/subgraph/messari/idle-governance",
            },
            {
                "protocol": "indexed",
                "url": "https://thegraph.com/hosted-service/subgraph/messari/indexed-governance",
            },
            {
                "protocol": "pooltogether",
                "url": "https://thegraph.com/hosted-service/subgraph/messari/pooltogether-governance",
            },
            {
                "protocol": "radicle",
                "url": "https://thegraph.com/hosted-service/subgraph/messari/radicle-governance",
            },
            {
                "protocol": "reflexer",
                "url": "https://thegraph.com/hosted-service/subgraph/messari/reflexer-governance",
            },
            {
                "protocol": "uniswap",
                "url": "https://thegraph.com/hosted-service/subgraph/messari/uniswap-governance",
            },
            {
                "protocol": "unslashed",
                "url": "https://thegraph.com/hosted-service/subgraph/messari/unslashed-governance",
            },
        ]
        self.data["delegateChanges"] = []
        self.data["delegateVotingPowerChanges"] = []
        self.data["delegates"] = []
        self.data["tokenHolders"] = []
        self.interval = 1000

    def call_the_graph_api(self, graph_url, query, variables, result_name, counter=0):
        time.sleep(counter)
        if counter > 20:
            return None

        transport = AIOHTTPTransport(url=graph_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)
        try:
            result = client.execute(query, variables)
            if result.get(result_name, None) == None:
                logging.error(f"The Graph API did not return {result_name}, counter: {counter}")
                return self.call_the_graph_api(graph_url, query, variables, result_name, counter=counter + 1)
        except Exception as e:
            logging.error(f"An exception occured getting The Graph API {e} counter: {counter} client: {client}")
            return self.call_the_graph_api(graph_url, query, variables, result_name, counter=counter + 1)
        return result

    def get_delegation_events(self):
        logging.info("Getting delegation events")
        for entry in self.graph_urls:
            protocol = entry["protocol"]
            graph_url = entry["url"]
            skip = 0
            cutoff_block = self.metadata.get(f"{protocol}_delegate_changes_cutoff_block", 0)
            entries = []
            while True:
                if skip > 5000:
                    skip = 0
                    cutoff_block = entries[-1]["blockNumber"]

                variables = {"first": self.interval, "skip": skip, "cutoff": cutoff_block}
                query = gql.gql(
                    """
                query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
                    delegateChanges(first: $first, skip: $skip, orderBy: blockNumber, orderDirection: asc, where: {blockNumber_gt: $cutoff}) {
                        id
                        tokenAddress
                        delegator
                        delegate
                        previousDelegate
                        txnHash
                        blockNumber
                        blockTimestamp
                    }
                }      
                """
                )
                result = self.call_the_graph_api(graph_url, query, variables, "delegateChanges")
                if result["delegateChanges"] == []:
                    break
                entries.extend(result["delegateChanges"])
                skip += self.interval

            for entry in entries:
                entry["protocol"] = protocol
                self.data["delegateChanges"].append(entry)

            logging.info(f"Found {len(entries)} delegateChanges for {protocol}")
            self.metadata[f"{protocol}_delegate_changes_cutoff_block"] = cutoff_block

    def get_delegation_voting_changes(self):
        logging.info("Getting delegate voting power changes")
        for entry in self.graph_urls:
            protocol = entry["protocol"]
            graph_url = entry["url"]
            skip = 0
            cutoff_block = self.metadata.get(f"{protocol}_delegate_voting_changes_cutoff_block", 0)
            entries = []
            while True:
                if skip > 5000:
                    skip = 0
                    cutoff_block = entries[-1]["blockNumber"]

                variables = {"first": self.interval, "skip": skip, "cutoff": cutoff_block}
                query = gql.gql(
                    """
                query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
                    delegateVotingPowerChanges(first: $first, skip: $skip, orderBy: blockNumber, orderDirection: asc, where: {blockNumber_gt: $cutoff}) {
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
                """
                )
                result = self.call_the_graph_api(graph_url, query, variables, "delegateVotingPowerChanges")
                if result["delegateVotingPowerChanges"] == []:
                    break
                entries.extend(result["delegateVotingPowerChanges"])
                skip += self.interval

            for entry in entries:
                entry["protocol"] = protocol
                self.data["delegateVotingPowerChanges"].append(entry)

            logging.info(f"Found {len(entries)} delegateVotingPowerChangeš for {protocol}")
            self.metadata[f"{protocol}_delegate_voting_changes_cutoff_block"] = cutoff_block

    def get_delegates(self):
        logging.info("Getting delegates")
        for entry in self.graph_urls:
            protocol = entry["protocol"]
            graph_url = entry["url"]
            skip = 0
            cutoff = 0
            entries = []
            while True:
                if skip > 5000:
                    skip = 0
                    cutoff = entries[-1]["delegatedVotesRaw"]

                variables = {"first": self.interval, "skip": skip, "cutoff": cutoff}
                query = gql.gql(
                    """
                query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
                    delegates(first: $first, skip: $skip, orderBy: delegatedVotesRaw, orderDirection: asc, where: {delegatedVotesRaw_gte: $cutoff}) {
                        id
                        delegatedVotesRaw
                        delegatedVotes
                        tokenHoldersRepresented {
                                id
                        }
                        numberVotes
                    }
                }      
                """
                )
                result = self.call_the_graph_api(graph_url, query, variables, "delegates")
                if result["delegates"] == []:
                    break
                entries.extend(result["delegates"])
                skip += self.interval

            for entry in entries:
                entry["protocol"] = protocol
                self.data["delegates"].append(entry)

            logging.info(f"Found {len(entries)} delegates for {protocol}")

    def get_token_holders(self):
        logging.info("Getting token holders")
        for entry in self.graph_urls:
            protocol = entry["protocol"]
            graph_url = entry["url"]
            skip = 0
            cutoff = 0
            entries = []
            while True:
                if skip > 5000:
                    skip = 0
                    cutoff = entries[-1]["tokenBalanceRaw"]

                variables = {"first": self.interval, "skip": skip, "cutoff": cutoff}
                query = gql.gql(
                    """
                    query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
                        tokenHolders(first: $first, skip: $skip, orderBy: tokenBalanceRaw, orderDirection: asc, where: {tokenBalanceRaw_gte: $cutoff}) {
                            id
                            tokenBalance
                            tokenBalanceRaw
                            totalTokensHeld
                            totalTokensHeldRaw
                            }
                        }
                """
                )
                result = self.call_the_graph_api(graph_url, query, variables, "tokenHolders")
                if result["tokenHolders"] == []:
                    break
                entries.extend(result["tokenHolders"])
                skip += self.interval

            for entry in entries:
                entry["protocol"] = protocol
                self.data["tokenHolders"].append(entry)

            logging.info(f"Found {len(entries)} tokenHolders for {protocol}")

    def run(self):
        self.get_delegation_events()
        self.get_delegation_voting_changes()
        self.get_delegates()
        self.get_token_holders()
        self.save_metadata()
        self.save_data()


if __name__ == "__main__":
    scraper = DelegationScraper()
    scraper.run()
    logging.info("Run complete. Good job dev.")
