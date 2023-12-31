from ...helpers import count_query_logging
from ...helpers import Cypher


class AccountsCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    @count_query_logging
    def set_wallet_account_label(self):
        query = """
            CALL apoc.periodic.commit("
                MATCH (wallet:Wallet)
                WHERE NOT wallet:Account 
                WITH wallet LIMIT 10000
                SET wallet:Account 
                RETURN count(*)
            ")
        """
        count = self.query(query)[0].value()
        return count
    
    @count_query_logging
    def set_account_type(self, label):
        query = f"""
            CALL apoc.periodic.commit("
                MATCH (account:{label}) 
                WHERE account.accountType IS NULL 
                WITH account LIMIT 10000
                SET account.accountType = '{label}'
                RETURN count(*)
            ")
        """
        count = self.query(query)[0].value()
        return count
    
    @count_query_logging
    def link_wallet_twitter_accounts(self):
        query = f"""
            CALL apoc.periodic.commit("
                MATCH (wallet:Wallet)-[:HAS_ALIAS]-(alias:Alias:Ens)-[:HAS_ALIAS]-(twitter:Twitter:Account)
                WHERE NOT (wallet)-[:HAS_ACCOUNT]-(twitter)
                AND NOT (alias)-[:HAS_ALIAS]-(:Entity)
                WITH wallet, twitter LIMIT 10000 
                MERGE (twitter)-[r:HAS_ACCOUNT]->(wallet)
                SET r.citation = 'Twitter - self-attested in tweet or bio.'
                RETURN count(*)
            ")
        """
        count = self.query(query)[0].value()
        return count
    
    @count_query_logging
    def link_wallet_github_accounts(self):
        query = f"""
            CALL apoc.periodic.commit("
                MATCH (wallet:Wallet)<-[:HAS_WALLET]-(github:Github)
                WHERE NOT (wallet)-[:HAS_ACCOUNT]->(github)
                WITH wallet, github LIMIT 10000
                MERGE (wallet)-[r:HAS_ACCOUNT]->(github)
                SET r.citation = 'Github has wallet'
                RETURN count(distinct(r))
            ")
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def link_same_handles(self):
        query = f"""
            MATCH (account1:Account)
            MATCH (account2:Account)
            WHERE id(account1) <> id(account2) 
            AND account1.handle IS NOT NULL 
            AND account2.handle IS NOT NULL 
            AND account1.handle = account2.handle 
            AND NOT (account1)-[:HAS_ACCOUNT]-(account2)
            MERGE (account1)-[r:HAS_ACCOUNT]-(account2)
            SET r.citation = "Same handle"
            return count(r)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def link_mirror_authors_to_twitter(self, threshold=3, proportion=0.8):
        query = f"""
            MATCH (w:Wallet)-[a:AUTHOR]-(m:Mirror:Article)-[r:REFERENCES]-(t:Twitter)
            WITH w.address as address, t.handle as handle, COUNT(*) as refs
            WHERE refs > {threshold}
            MATCH (w:Wallet)-[a:AUTHOR]-(m:Mirror:Article)
            WHERE w.address in address
            WITH address, handle, refs, count(a) as authorship
            WITH address, handle, toFloat(refs)/toFloat(authorship) as proportion
            WHERE proportion > {proportion}
            MATCH (w:Wallet {{address: address}})
            MATCH (t:Twitter {{handle: handle}})
            WHERE NOT (w)-[:HAS_ACCOUNT]-(t)
            MERGE (w)-[r:HAS_ACCOUNT]-(t)
            SET r.likely = true
            SET r.citation = "High reference proportion in Mirror Articles"
            RETURN count(r)
        """
        count = self.query(query)[0].values()
        return count

    @count_query_logging
    def connect_wallet_dune_twitter(self):
        count = 0
        query = """
            MATCH (wallet:Wallet)-[:HAS_ACCOUNT]-(:Dune)-[:HAS_ACCOUNT]-(twitter:Twitter)
            WHERE NOT (wallet)-[:HAS_ACCOUNT]->(twitter)
            MERGE (wallet)-[r:HAS_ACCOUNT]->(twitter)
            SET r.citation = 'Wallet - Dune - Twitter'
            return count(r)
        """
        count += self.query(query)[0].value()
        return count

    @count_query_logging
    def connect_wallet_github_twitter(self):
        count = 0
        query = """
            match (wallet:Wallet)-[:HAS_ACCOUNT]-(:Github)-[:HAS_ACCOUNT]-(twitter:Twitter)
            WHERE NOT (wallet)-[:HAS_ACCOUNT]->(twitter)
            MERGE (wallet)-[r:HAS_ACCOUNT]->(twitter)
            SET r.citation = 'Wallet - Github - Twitter'
            return count(r)
        """
        count += self.query(query)[0].value()
        return count

    @count_query_logging
    def connect_wallet_alias_account(self):
        count = 0
        query = """
            match (wallet:Wallet)-[:HAS_ALIAS]-(:Alias:Ens)-[:HAS_ACCOUNT]-(account:Account)
            WHERE NOT (wallet)-[:HAS_ACCOUNT]->(account)
            MERGE (wallet)-[r:HAS_ACCOUNT]->(account)
            set r.citation = 'Wallet - ENS - Account'
            return count(r)
        """
        count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_mirror_accounts(self):
        query = f"""
        CALL apoc.periodic.commit('
            MATCH (m:Article)-[:AUTHOR]-(wallet:Wallet)
            MATCH (m:Mirror:Account)
            OPTIONAL MATCH (wallet)-[:HAS_ALIAS]-(alias:Alias {{primary:True}})
            WHERE NOT wallet.address = m.author
            WITH wallet.address AS address, coalesce(alias.name, wallet.address) AS handle, "https://mirror.xyz/" + wallet.address AS url LIMIT 10000
            MERGE (account:Mirror:Account {{author:address}})
            ON CREATE SET account.url = url, account.handle = handle, account.accountType = "Mirror", account.uuid = apoc.create.uuid()
            ON MATCH SET account.lastUpdateDt = timestamp()
            RETURN COUNT(*)
        ')
        """
        count = self.query(query)[0].value()
        return count 

    @count_query_logging
    def link_mirror_accounts(self):
        query = """
        CALL apoc.periodic.commit('
            MATCH (wallet:Wallet)
            MATCH (account:Account:Mirror)
            WHERE NOT (wallet)-[:HAS_ACCOUNT]-(:Mirror:Account)
            AND wallet.address = account.author 
            WITH wallet, account limit 5000
            MERGE (wallet)-[accn:HAS_ACCOUNT]->(account)
            SET accn.citation = "Has mirror account"
            RETURN count(*)
        ')
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def handle_token_accounts(self):
        twitter = """
        CALL apoc.periodic.commit('
            MATCH (token:Token) 
            WHERE ((token:ERC721) or (token:ERC1155)) 
            AND token.twitterUsername IS NOT NULL 
            AND NOT (token)-[:HAS_ACCOUNT]-(:Twitter)
            WITH token limit 1000
            MATCH (twitter:Twitter)
            WHERE tolower(token.twitterUsername) = twitter.handle 
            WITH token, twitter 
            MERGE (token)-[acc:HAS_ACCOUNT]->(twitter)
            SET acc.citation = "Is a token account"
            RETURN COUNT(*)
        ')
        """
        self.query(twitter)

        return None 
    
