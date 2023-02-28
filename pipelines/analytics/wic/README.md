# Wallet In Context

This submodule encompasses the Wallet In Context flows.

# Base ontology

Every WIC module follows the following ontology, there are three main nodes types _Main, _Condition and _Context. 

Nodes:
-  (main:_Wic:_*SUBGRAPH-NAME*:_Main)
-  (condtion:_Wic:_*SUBGRAPH-NAME*:_Condition:_*CONDITION_NAME*)
-  (context:_Wic:_*SUBGRAPH-NAME*:_Condition:_*CONDITION_NAME*:_Context:_*CONTEXT_NAME*)

Edges:
- (main)-[:HAS_CONDITION]->(condition)
- (context)-[:HAS_CONDITION]->(condition)

## Additional ontologies

In some cases, to mark an item as part of a group, the relation `_PARADIGM_CASE` is used:
- (node)-[:_PARADIGM_CASE]->(context)

# Base classes

This module provides two base classes: WICAnalysis and WICCypher. They are parent class that must be inherited by any of the children classes and will take care of deploying the subgraph as well as doing checks, cleanups and function calls.

## WICAnalysis
WICAnalysis is the main processing class, this class defines the configuration variables, as well as the subgraph name. It is in charge of defining the functions fo be called for each context. 

```python
class WICAnalysis(Analysis):
    def __init__(self, bucket_name) -> None:
        """Initialize with a bucket name"""

    def process_conditions(self):
        """Calling this function in the run will go through each condition, context and subcontexts and trigger their processing"""
```

## WICCypher

WICCypher is the main cypher class, it defines the Neo4J cypher queries that need to be triggered on the DB. It will read from the configuration and subgraph name to clear and created the subgraph. It is in charge of creating _Main, _Condition and _Context.

```python
from ...helpers.decorators import count_query_logging
from ...helpers.cypher import Cypher

class WICCypher(Cypher):
    def __init__(self, subgraph_name, conditions, database=None):
        """
            WICCyphert takes the subgraph_name and conditions dictionary as inputs.
            It first clears the subgraph, then iterate over all conditions and contexts to create the nodes and connect them.
        """

    @count_query_logging
    def clear_subgraph(self):
        """Clear the subgraph prior to running the anlaysis and recreating the relationships"""
    @count_query_logging
    def create_main(self): 
        """Creates the main node of the subgraph"""

    @count_query_logging
    def create_conditions(self):
        """Create the nodes for each condition"""
    
    @count_query_logging
    def create_contexts(self):
        """Create the context nodes for every condition"""
        
    def create_context_query(self, condition, context):
        """Creates a context node"""
```

# Configuration

Each WIC must define a configuration. This must be define in the children class inheriting from the WICAnalysis, in the `__init__` function, prior to intanciating the `WICCypher`. By default the configuration has two levels, Condition and Context, but a SubContext can be defined within a context. If a subcontext exists, then the root context will first be executed, followed by any number of subcontext that depends on it. Moreover, the root_context will be sent to all subcontext children to be optionally used in queries or processing. 



Functions for the context and subcontext must accept the following arguments:

```python
def context_function(self, context):
    "Takes the context name as an argument"
    # do something like call the cyphers

def subcontext_function(self, root_context, context):
    "Takes the root_context and the context name as arguments"
    # do something like call the cyphers
```

## Subgraph name

Each Wallet In Context must define its own subgraph. To this end a variable subgraph_name must be defined in the Analysis class in the init function and be passed as an argument to the WICCypher instance.

## Conditions and contexts

A conditions configuration is a nested dictionary with three levels: Condition, Context, (optional) Subcontext. Each Context or Subcontext must have a function associated with them that will be called at runtime.  

```python
conditions = {
    "Condition": {
        "Context": function,
        "Context": function
    },
    "Condition": {
        "Context": function,
        "Context": function,
        "Context": function
    },
    "Condition": {
        "Context": {
            "call": function,
            "subcontexts": {
                "Subcontext": function,
                "Subcontext": function
            }
        },
        "Context": function
    }
}
```

# Writing a WIC module

Each module must define two files: `cyphers.py` and `analyze.py`. 

## cypher.py: The WICCypher class

```python
from .. import WICCypher
from ....helpers import count_query_logging

class MyCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
        
    @count_query_logging
    def connect_some_nodes(self, context):
        cool_query = f"""
            SOME ingenuous query
            THAT will connect nodes
            TO the context node
        """
        count = self.query(cool_query)[0].value()
        return count 

    def get_benchmark(self):
        get_benchmark = """
            A query to get some number to be used in a different query
            It is easier to maintain small queries
        """
        benchmark = self.query(get_benchmark)[0].value()
        return benchmark

    @count_query_logging
    def connect_with_benchmark(self, context, benchmark):
        connect_benchmark = f"""
            A query that uses {benchmark} values in it
            Also can use the {context} in a (:_Context:{context}) like
            MATCH (wic:_Wic:{self.subgraph_name}:_Context:{context})
            
        """
        count = self.query(connect_benchmark)[0].value()
        return count 
```

## analyze.py: The WICAnalytic class

```python
from .. import WICAnalysis
from .cyphers import FarmerCyphers

class MyAnalysis(WICAnalysis):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""
    def __init__(self):
        self.subgraph_name = "MyCoolSubGraphName"
        self.conditions = {
            "FirstCondition": {
                "CoolContext": self.process_cool_context
            }, 
            "SecondCondition": {
                "NeatContext": self.process_neat_context
            },
            "ThirdCondition": {
                "RootContext": {
                    "call": self.process_root_context,
                    "subcontexts": {
                        "ChildContext": self.process_child_context,
                        "SiblingContext": self.process_sibling_context,
                    }
                }
            }
        }
        
        self.cyphers = FarmerCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-farming")

    def process_cool_context(self, context):
        # Here call the cyphers
        self.cypher.connect_cool_context(context)

    def process_neat_context(self, context):
        # Here call the cyphers
        self.cypher.connect_neat_context(context)

    def process_root_context(self, context):
        # Here call the cyphers
        self.cypher.connect_root_context(context)

    def process_child_context(self, root_context, context):
        # Here call the cyphers
        self.cypher.connect_child_context(root_context, context)

    def process_sibling_context(self, root_context, context):
        # Here call the cyphers
        self.cypher.connect_sibling_context(root_context, context)


    def run(self):
        self.process_conditions()

if __name__ == '__main__':
    analysis = MyAnalysis()
    analysis.run()
```

