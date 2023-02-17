from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging
import os
DEBUG = os.environ.get("DEBUG", False)

class TokenMetadataCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)


    @get_query_logging
    def get_empty_tokens(self):
        token_node_query = f"""
            MATCH (t:Token:ERC20) 
            WHERE t.address IS NOT NULL AND (t.name IS NULL OR t.logo IS NULL OR t.symbol IS NULL) 
            RETURN t 
        """
        if DEBUG:
            token_node_query += " LIMIT 100"
        tokens = self.query(token_node_query)
        tokens = [dict(token.get('t')) for token in tokens]
        return tokens

    @count_query_logging
    def add_token_node_metadata(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                MATCH (t:Token:ERC20 {{address: tokens.address}})
                SET t.name = tokens.name,
                    t.symbol = tokens.symbol,
                    t.decimals = toInteger(tokens.decimals),
                    t.logo = tokens.logo,
                    t.lastUpdatedDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                return count(t)"""
            count += self.query(query)[0].value()
        return count
