from ..helpers import Scraper
import logging
import time
import os
import gql
import logging
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log
gql_log.setLevel(logging.WARNING)

DEBUG = os.environ.get("DEBUG", False)

class DAOHausScraper(Scraper):
    def __init__(self, bucket_name="daohaus", allow_override=False):
        super().__init__(bucket_name, allow_override=allow_override)
        self.cutoff_timestamp = self.metadata.get("cutoff_timestamp", 0)
        self.interval = 1000
        self.graph_urls = {
            "mainnet":"https://api.thegraph.com/subgraphs/name/odyssy-automaton/daohaus",
            "xdai":"https://api.thegraph.com/subgraphs/name/odyssy-automaton/daohaus-xdai",
            "optimism":"https://api.thegraph.com/subgraphs/name/odyssy-automaton/daohaus-optimism",
        }

    def call_the_graph_api(self, query, variables, expectations, chain, counter=0):
        time.sleep(counter)
        if counter > 20:
            return None

        transport = AIOHTTPTransport(url=self.graph_urls[chain])
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)
        try:
            result = client.execute(query, variable_values=variables)
            for key in expectations:
                if result.get(key, None) == None:
                    logging.error(f"theGraph API did not return {key}: {result} | counter: {counter}")
                    return self.call_the_graph_api(query, variables, expectations, chain, counter=counter+1)
        except Exception as e:
            logging.error(f"An exception occurred getting the graph API {e} counter: {counter} client: {client}")
            return self.call_the_graph_api(query, variables, expectations, chain, counter=counter+1)
        return result
        
    def fetch_daoHaus_data(self):
        for chain in self.graph_urls.keys():
            logging.info(f"Fetching information for chain: {chain}")
            self.data[chain] = {
                "daoMetas": [],
                "moloches": [],
                "tokenBalances": [],
                "votes": [],
                "members": [],
                "proposals": []
            }
            result = {key:["init"] for key in self.data[chain]}
            skip = 0
            retry = 0
            if DEBUG:
                req = 0
                max_req = 5
            query = gql.gql("""
                query($first: Int!, $skip: Int!, $cutoff: String!) {
                    daoMetas(first: $first, skip: $skip) 
                    {
                        id
                        title
                        version
                        newContract
                        http
                    }
                    moloches(first: $first, skip: $skip, orderBy: createdAt, orderDirection:desc, where:{createdAt_gt: $cutoff}) {
                        id
                        version
                        summoner
                        newContract
                        summoningTime
                        createdAt
                        periodDuration
                        votingPeriodLength
                        gracePeriodLength
                        proposalDeposit
                        approvedTokens {
                            id
                            tokenAddress
                            whitelisted
                            symbol
                            decimals
                        }
                        guildBankAddress
                        guildBankBalanceV1
                        tokens {
                            id
                            tokenAddress
                            whitelisted
                            symbol
                            decimals
                        }
                        totalLoot
                        totalShares
                    }
                    tokenBalances(first: $first, skip: $skip) 
                    {
                        id
                        moloch {
                            id
                        }
                        token {
                            tokenAddress
                        }
                        tokenBalance
                    }
                    votes(first: $first, skip: $skip, orderBy: createdAt, orderDirection:desc, where:{createdAt_gt: $cutoff}) 
                    {
                        id
                        createdAt
                        proposal 
                        {
                            proposalId
                        }
                        uintVote
                        molochAddress
                        memberAddress
                        proposalIndex
                        memberPower
                    }
                    members(first: $first, skip: $skip, orderBy: createdAt, orderDirection:desc, where:{createdAt_gt: $cutoff}) 
                    {
                        id
                        createdAt
                        memberAddress
                        tokenTribute
                        molochAddress
                        shares
                        loot
                        exists
                        tokenTribute
                        didRagequit
                        kicked
                        jailed
                    }
                    proposals(first: $first, skip: $skip, orderBy: createdAt, orderDirection:desc, where:{createdAt_gt: $cutoff}) {
                        createdAt
                        createdBy
                        proposalIndex
                        proposalId
                        molochAddress
                        memberAddress
                        applicant
                        proposer
                        sponsor
                        processor
                        sharesRequested
                        lootRequested
                        tributeOffered
                        tributeToken
                        tributeTokenSymbol
                        tributeTokenDecimals
                        paymentRequested
                        paymentToken
                        paymentTokenSymbol
                        paymentTokenDecimals
                        startingPeriod
                        yesVotes
                        noVotes
                        sponsored
                        sponsoredAt
                        processed
                        processedAt
                        didPass
                        cancelled
                        cancelledAt
                        aborted
                        whitelist
                        guildkick
                        newMember
                        trade
                        details
                        maxTotalSharesAndLootAtYesVote
                        yesShares
                        noShares
                        votingPeriodStarts
                        votingPeriodEnds
                        gracePeriodEnds
                        uberHausMinionExecuted
                        executed
                        minionAddress
                        isMinion
                        minionExecuteActionTx {
                            id
                            createdAt
                        }
                    }
                }""")
            while sum([len(result.get(key, [])) for key in result]) > 0:
                if DEBUG:
                    if req > max_req:
                        break
                    req += 1
                variables = {"first": self.interval, "skip": skip, "cutoff": str(self.cutoff_timestamp)}
                result = self.call_the_graph_api(query, variables, self.data[chain].keys(), chain)
                if result != None:
                    for key in self.data[chain]:
                        self.data[chain][key] += result.get(key, [])
                    skip += self.interval
                    retry = 0
                    logging.info(f"Query success, skip is at: {skip}")
                else:
                    retry += 1
                    if retry > 10:
                        skip += self.interval
                    logging.error(f"Query unsuccessful, skip is at: {skip}")

    def run(self):
        self.fetch_daoHaus_data()
        self.save_data()
        self.metadata["cutoff_timestamp"] = int(self.runtime.timestamp())
        self.save_metadata()

if __name__ == "__main__":
    scraper = DAOHausScraper()
    scraper.run()