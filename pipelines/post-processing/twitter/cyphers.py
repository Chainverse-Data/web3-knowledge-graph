from ...helpers import Cypher
from ...helpers import count_query_logging

class TwitterCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    @count_query_logging
    def clean_twitter_nodes(self):
        query = """
        MATCH (n:Twitter)
        SET n:Account
        RETURN count(n)
        """
        count = self.query(query)[0].value()
        return count
