from ...helpers.decorators import count_query_logging
from ...helpers import Cypher, Indexes

class WICCypher(Cypher):
    def __init__(self, subgraph_name, conditions, database=None):
        super().__init__(database)
        self.conditions = conditions
        self.subgraph_name = subgraph_name
        self.clear_subgraph()
        self.create_main()
        self.create_conditions()
        self.create_contexts()

    def create_indexes(self):
        indexes = Indexes()
        indexes.wicIndexes()

    @count_query_logging
    def clear_subgraph(self):
        query = f"""
            MATCH (w:_Wic:_{self.subgraph_name})
            DETACH DELETE w
            RETURN count(w)
        """
        count = self.query(query)[0].value()
        return count
    
    @count_query_logging
    def create_main(self): 
        query = f"""
            MERGE (main:_Wic:_Main:_{self.subgraph_name})
            SET main._displayName = '{self.subgraph_name}'
            return count(main)
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def create_conditions(self):
        count = 0 
        for condition in self.conditions:
            create_condition = f"""
                MATCH (main:_Wic:_Main:_{self.subgraph_name})
                MERGE (condition:_Wic:_Condition:_{condition}:_{self.subgraph_name})
                SET condition._displayName = '{condition}'
                WITH main, condition 
                MERGE (main)-[r:_HAS_CONDITION]->(condition)
                RETURN count(condition)
            """
            count += self.query(create_condition)[0].value()
        return count
    
    @count_query_logging
    def create_contexts(self):
        count = 0
        for condition in self.conditions:
            for context in self.conditions[condition]:
                if type(self.conditions[condition][context]) == dict:
                    count += self.create_context_query(condition, context)
                    for subcontext in self.conditions[condition][context]["subcontexts"]:
                        count += self.create_context_query(condition, subcontext)
                else:
                    count += self.create_context_query(condition, context)
        return count
        
    def create_context_query(self, condition, context):
        create_context = f"""
            MERGE (context:_Wic:_Context:_{self.subgraph_name}:_{condition}:_{context})
            SET context._displayName = '{context}'
            SET context._main = '{self.subgraph_name}'
            WITH context
            MATCH (condition:_Wic:_Condition:_{self.subgraph_name}:_{condition})
            WITH context, condition
            MERGE (context)-[r:_HAS_CONDITION]->(condition)
            RETURN count(context)
        """
        count = self.query(create_context)[0].value()
        return count
