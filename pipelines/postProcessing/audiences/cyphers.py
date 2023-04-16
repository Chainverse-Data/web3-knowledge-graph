from ...helpers import count_query_logging
from ...helpers import Cypher


class AccountsCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    @count_query_logging
    def create_audience_by_condition_or_context(self, condition_or_context, audienceId):
        query = f"""
            MATCH (wallet:Wallet)-[:_HAS_CONTEXT]-(wic:_Wic:_Context:_{condition_or_context})
            MATCH (audience:Audience {{audienceId: "{audienceId}"}})
            MERGE (wallet)-[:IS_PART_OF]-(audience)
            RETURN count(wallet)
        """
        print(query)
        count = self.query(query)[0].value()
        return count

    
    @count_query_logging
    def create_audience(self, audienceId, name, imageUrl, description):
        query = f"""
            MERGE (audience:Audience {{audienceId: $audienceId}})
            SET audience.name = $name
            SET audience.imageUrl = $imageUrl
            SET audience.description = $description
            RETURN count(audience)
        """
        params = {
            "audienceId": audienceId, 
            "name": name, 
            "imageUrl": imageUrl, 
            "description": description
        }
        count = self.query(query, parameters=params)[0].value()
        return count