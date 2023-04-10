import logging
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
            CALL apoc.periodic.commit("
                MATCH (:_Wic:_{self.subgraph_name})-[edge:_HAS_CONTEXT]-()
                WITH edge LIMIT 10000
                DELETE edge
                RETURN count(edge)
            ")
        """
        self.query(query)[0].value()
        
        query = f"""
            MATCH (wic:_Wic:_{self.subgraph_name})
            DETACH DELETE wic
            RETURN count(wic)
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
                logging.info(f"Creating {context}")
                context_types = self.conditions[condition][context]["types"]
                definition = self.conditions[condition][context]["definition"]
                if "subcontexts" in self.conditions[condition][context]:
                    count += self.create_context_query(condition, context, context_types, definition)
                    for subcontext in self.conditions[condition][context]["subcontexts"]:
                        subcontext_types = self.conditions[condition][context]["subcontexts"][subcontext]["types"]
                        subdefinition = self.conditions[condition][context]["subcontexts"][subcontext]["definition"]
                        count += self.create_context_query(condition, subcontext, subcontext_types, subdefinition)
                else:
                    count += self.create_context_query(condition, context, context_types, definition)
        return count
        
    def create_context_query(self, condition, context, types, definition):
        create_context = f"""
            MERGE (context:_Wic:_Context:_{self.subgraph_name}:_{condition}:_{context}:{":".join(["_" + t for t in types])})
            SET context._condition = '{condition}'
            SET context._displayName = '{context}'
            SET context._main = '{self.subgraph_name}'
            SET context._types = apoc.convert.toList({types})
            SET context._definition = '{definition}'
            WITH context
            MATCH (condition:_Wic:_Condition:_{self.subgraph_name}:_{condition})
            WITH context, condition
            MERGE (context)-[r:_HAS_CONDITION]->(condition)
            RETURN count(context)
        """
        count: int = self.query(create_context)[0].value()
        return count
