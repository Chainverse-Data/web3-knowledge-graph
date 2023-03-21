from .cypher import Cypher


class Indexes(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    def create_constraints(self):
        pass

    def create_indexes(self):
        pass

    def contracts(self):
        query = """CREATE INDEX UniqueAddress IF NOT EXISTS FOR (n:Contract) ON (n.address)"""
        self.query(query)

    def proposals(self):
        query = """CREATE INDEX UniquePropID IF NOT EXISTS FOR (n:Proposal) ON (n.snapshotId)"""
        self.query(query)

    def spaces(self):
        query = """CREATE INDEX UniqueSpaceID IF NOT EXISTS FOR (n:Space) ON (n.snapshotId)"""
        self.query(query)

    def wallets(self):
        query = """CREATE INDEX UniqueWalletAddress IF NOT EXISTS FOR (n:Wallet) ON (n.address)"""
        self.query(query)

    def accounts(self):
        query = "CREATE INDEX AccountHandles IF NOT EXISTS FOR (n:Account) ON (n.handle)"
        self.query(query)

    def tokens(self):
        query = """CREATE INDEX UniqueTokenAddress IF NOT EXISTS FOR (d:Token) ON (d.address)"""
        self.query(query)

    def ens(self):
        query = "CREATE INDEX ENSName IF NOT EXISTS FOR (n:Ens) ON (n.name)"
        self.query(query)

    def transactions(self):
        query = """CREATE INDEX UniqueTransaction IF NOT EXISTS FOR (n:Transaction) ON (n.txHash)"""
        self.query(query)

    def aliases(self):
        query = """CREATE INDEX UniqueAlias IF NOT EXISTS FOR (n:Alias) ON (n.name)"""
        self.query(query)

    def articles(self):
        query = """CREATE INDEX UniqueArticleID IF NOT EXISTS FOR (n:Mirror) ON (n.uri)"""
        self.query(query)

    def twitter(self):
        query = """CREATE INDEX UniqueTwitterID IF NOT EXISTS FOR (n:Twitter) ON (n.handle)"""
        self.query(query)

    def gitcoin_grants(self):
        query = """CREATE INDEX UniqueGrantID IF NOT EXISTS FOR (n:GitcoinGrant) ON (n.id)"""
        self.query(query)

    def gitcoin_users(self):
        query = """CREATE INDEX UniqueUserHandle IF NOT EXISTS FOR (n:GitcoinUser) ON (n.handle)"""
        self.query(query)

    def gitcoin_bounties(self):
        query = """CREATE INDEX UniqueBountyID IF NOT EXISTS FOR (n:GitcoinBounty) ON (n.id)"""
        self.query(query)

    def mirror_articles(self):
        query = """CREATE INDEX UniqueArticleID IF NOT EXISTS FOR (a:Mirror) ON a.originalContentDigest"""
        self.query(query)

    def daohaus_dao(self):
        query = """CREATE INDEX UniqueDaoID IF NOT EXISTS FOR (a:Dao) ON a.daohausId"""
        self.query(query)

    def daohaus_proposal(self):
        query = """CREATE INDEX UniqueProposalID IF NOT EXISTS FOR (a:Proposal) ON a.proposalId"""
        self.query(query)

    def website(self):
        query = """CREATE INDEX UniqueWebsiteID IF NOT EXISTS FOR (a:Website) ON a.url"""
        self.query(query)

    def email(self):
        query = "CREATE INDEX Emails IF NOT EXISTS FOR (e:Email) ON (e.email)"
        self.query(query)
        
    def wicIndexes(self):
        query = """CREATE FULLTEXT INDEX wicArticles IF NOT EXISTS FOR (a:Article) ON EACH [a.text, a.title]"""
        self.query(query)
        query = """CREATE FULLTEXT INDEX wicBios IF NOT EXISTS FOR (a:Twitter|Github|Dune) ON EACH [a.bio]"""
        self.query(query)
        query = """CREATE FULLTEXT INDEX wicGrants IF NOT EXISTS FOR (a:Grant) ON EACH [a.text, a.title]"""
        self.query(query)
        query = """CREATE FULLTEXT INDEX wicProposals IF NOT EXISTS FOR (a:Proposal) ON EACH [a.text, a.title]"""
        self.query(query)
        query = """CREATE FULLTEXT INDEX wicTwitter IF NOT EXISTS FOR (a:Twitter) ON EACH [a.bio]"""
        self.query(query)
