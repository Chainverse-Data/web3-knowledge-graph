from pipelines.helpers.decorators import get_query_logging
from ...helpers import count_query_logging
from ...helpers import Cypher


class AccountsCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    @get_query_logging
    def get_wic_conditions(self):
        query = f"""
            MATCH (condition:_Wic:_Condition)
            RETURN condition
        """
        records = self.query(query)
        conditions = [record["condition"] for record in records]
        return conditions

    @get_query_logging
    def get_wic_contexts(self):
        query = f"""
            MATCH (context:_Wic:_Context)
            RETURN context
        """
        records = self.query(query)
        contexts = [record["context"] for record in records]
        return contexts
    
    @count_query_logging
    def create_audience(self, params):
        query = f"""
            MERGE (audience:Audience {{audienceId: $audienceId}})
            SET audience.name = $name
            SET audience.collectionName = $name
            SET audience.imageUrl = $imageUrl
            SET audience.description = $description
            RETURN count(audience)
        """
        count = self.query(query, parameters=params)[0].value()
        return count

    @count_query_logging
    def flag_existing_edges(self):
        query = """
            CALL apoc.periodic.commit("
                MATCH (audience:Audience)-[edge:IS_PART_OF]-(wallet:Wallet)
                WHERE edge.toRemove = false
                WITH edge LIMIT 10000
                SET edge.toRemove = true
                RETURN count(edge)            
            ")
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def create_audience_by_context(self, wic):
        query = f"""
            MATCH (wallet:Wallet)-[:_HAS_CONTEXT]-(wic:_Wic:_Context:_{wic})
            MATCH (audience:Audience {{audienceId: "{wic}"}})
            MERGE (wallet)-[edge:IS_PART_OF]-(audience)
            SET edge.toRemove = false
            RETURN count(wallet)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def create_audience_by_condition(self, wic):
        query = f"""
            MATCH (wallet:Wallet)-[:_HAS_CONTEXT]-(:_Wic:_Context)-[:_HAS_CONDITION]-(:_Wic:_Condition:_{wic})
            MATCH (audience:Audience {{audienceId: "{wic}"}})
            MERGE (wallet)-[edge:IS_PART_OF]-(audience)
            SET edge.toRemove = false
            RETURN count(wallet)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def clean_edges(self):
        query = """
            CALL apoc.periodic.commiut("
                MATCH (audience:Audience)-[edge:IS_PART_OF]-(wallet:Wallet)
                WHERE edge.toRemove = true
                WITH edge LIMIT 10000
                DELETE edge
                RETURN count(edge)            
            ")
        """
        count = self.query(query)[0].value()
        return count
