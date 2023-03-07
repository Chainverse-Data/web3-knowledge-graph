from ..helpers import Scraper
import logging
import os
import logging

DEBUG = os.environ.get("DEBUG", False)

class DAOHausScraper(Scraper):
    def __init__(self, bucket_name="daohaus", allow_override=False):
        super().__init__(bucket_name, allow_override=allow_override)

        self.interval = 1000
        if DEBUG:
            self.interval = 10

        self.graph_urls = {
            "mainnet":"https://api.thegraph.com/subgraphs/name/odyssy-automaton/daohaus",
            "xdai":"https://api.thegraph.com/subgraphs/name/odyssy-automaton/daohaus-xdai",
            "optimism":"https://api.thegraph.com/subgraphs/name/odyssy-automaton/daohaus-optimism",
            "polygon": "https://api.thegraph.com/subgraphs/name/odyssy-automaton/daohaus-matic"
        }
        self.last_cutoffs = {}

        for chain in self.graph_urls.keys():
            self.data[chain] = {
                "daoMetas": [],
                "moloches": [],
                "tokenBalances": [],
                "votes": [],
                "members": [],
                "proposals": []
            }
            self.last_cutoffs[chain] = {}
            for key in self.data[chain]:
                if key in ["daoMetas", "tokenBalances"]:
                    self.last_cutoffs[chain][key] = self.metadata.get("last_cutoffs", {chain: {key: ""}})[chain][key]
                else:
                    self.last_cutoffs[chain][key] = self.metadata.get("last_cutoffs", {chain: {key: "0"}})[chain][key]
            logging.info(f"Last Cutoffs are: {self.last_cutoffs}")

    def fetch_dao_meta(self):
        logging.info(f"Fetching daoMetas information")
        query = """
                query($first: Int!, $cutoff: ID!) {
                    daoMetas(first: $first, orderBy: id, orderDirection:desc, where:{id_gt: $cutoff}) 
                    {
                        id
                        title
                        version
                        newContract
                        http
                    }
                }
                """
        self.fetch_data(query, "daoMetas", "id")

    def fetch_moloches(self):
        logging.info(f"Fetching moloches information")
        query = """
                    query($first: Int!, $cutoff: String!) {
                        moloches(first: $first, orderBy: createdAt, orderDirection:desc, where:{createdAt_gt: $cutoff}) {
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
                    }
                    """
        self.fetch_data(query, "moloches", "createdAt")

    def fetch_token_balances(self):
        logging.info(f"Fetching token balances information")
        query = """
                query($first: Int!, $cutoff: ID!) {
                    tokenBalances(first: $first, orderBy: id, orderDirection:desc, where:{id_gt: $cutoff}) 
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
                }
                """
        self.fetch_data(query, "tokenBalances", "id")

    def fetch_votes(self):
        logging.info(f"Fetching votes information")
        query = """
                query($first: Int!, $cutoff: String!) {
                    votes(first: $first, orderBy: createdAt, orderDirection:desc, where:{createdAt_gt: $cutoff}) 
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
                }
                """
        self.fetch_data(query, "votes", "createdAt")

    def fetch_members(self):
        logging.info(f"Fetching members information")
        query = """
                query($first: Int!, $cutoff: String!) {
                    members(first: $first, orderBy: createdAt, orderDirection:desc, where:{createdAt_gt: $cutoff}) 
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
                }
                """
        self.fetch_data(query, "members", "createdAt")

    def fetch_proposals(self):
        logging.info(f"Fetching proposals information")
        query = """
                query($first: Int!, $cutoff: String!) {
                    proposals(first: $first, orderBy: createdAt, orderDirection:desc, where:{createdAt_gt: $cutoff}) {
                        id
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
                }
                """
        self.fetch_data(query, "proposals", "createdAt")

    def fetch_data(self, query, key, cutoff_key):
        for chain in self.graph_urls.keys():
            logging.info(f"Fetching information for chain: {chain}. \nQuery: {query} \nParams: {key} | {self.last_cutoffs[chain][key]} | {cutoff_key}")
            result = {key:["init"]}
            retry = 0
            cutoff = self.last_cutoffs[chain][key]
            variables = {"first": self.interval, "cutoff": cutoff}
            if DEBUG:
                req = 0
                max_req = 6
            while sum([len(result.get(key, [])) for key in result]) > 0:
                if DEBUG:
                    if req > max_req:
                        break
                    req += 1
                result = self.call_the_graph_api(self.graph_urls[chain], query, variables, [key])
                if result != None:
                    data = result.get(key, [])
                    self.data[chain][key] += data
                    if len(data) > 0:
                        cutoff = data[-1][cutoff_key]
                        variables["cutoff"] = cutoff
                    retry = 0
                    logging.info(f"Query success, cutoff is at: {cutoff}")
                else:
                    retry += 1
                    if retry > 5:
                        result = {key:[]}
                        logging.error(f"Query unsuccessful. Giving up on it.")
                    logging.error(f"Query unsuccessful. Retrying! cutoff is at: {cutoff}")
            self.last_cutoffs[chain][key] = cutoff

    def run(self):
        self.fetch_dao_meta()
        self.fetch_moloches()
        self.fetch_token_balances()
        self.fetch_votes()
        self.fetch_members()
        self.fetch_proposals()
        self.save_data()
        self.metadata["last_cutoffs"] = self.last_cutoffs
        self.save_metadata()

if __name__ == "__main__":
    scraper = DAOHausScraper()
    scraper.run()