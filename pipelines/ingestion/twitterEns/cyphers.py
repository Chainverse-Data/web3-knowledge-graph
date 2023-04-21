from ...helpers import Cypher
from ...helpers import Constraints
from ...helpers import Indexes
from ...helpers import Queries
from ...helpers import count_query_logging


class TwitterEnsCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    # def create_constraints(self):
    #     constraints = Constraints()
    #     constraints.wallets()
    #     constraints.aliases()
    #     constraints.ens()
    #     constraints.twitter()

    # def create_indexes(self):
    #     indexes = Indexes()
    #     indexes.wallets()
    #     indexes.aliases()
    #     indexes.ens()
    #     indexes.twitter()

    @count_query_logging
    def create_or_merge_twitter_accounts(self, urls):
        count = self.queries.create_or_merge_twitter(urls)
        return count

    @count_query_logging
    def create_or_merge_twitter_alias(self, urls):
        count = self.queries.create_or_merge_ens_alias(urls)
        return count

    @count_query_logging
    def create_or_merge_twitter_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def link_twitter_alias(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                    MATCH (a:Alias:Ens {{name: toLower(twitter.ens)}})
                    MATCH (t:Twitter:Account {{handle: toLower(twitter.handle)}})
                    MERGE (t)-[r:HAS_ALIAS]->(a)
                    r.citation = "Alias from ENS Twitter pipeline"
                    return count(r)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_wallet_alias(self, urls):
        count = self.queries.link_wallet_alias(urls)
        return count
