# Gitcoin analytics

This module reads the Wallet - Grant donation biPartite from the Neo4J instance to extract the grants and wallet communities. 

# Analyses

First, the BiPartite graph is projected to the grant and wallet subtype. Each of the projection is then subjected to Louvain community detection. The communities are then written back to the Neo4J instance as follows:

(Object)-[HAS_PARTITION]-(Partition:Gitcoin {partitionTarget: Object})

Object can:
  - Wallet
  - Grant:Gitcoin

# Ontology

## Nodes
  - Partition:Gitcoin
    - method: The method used to compute the community (here "Louvain")
    - partition: The partition ID
    - partitionTarget: The partition Target Object 

## Edges
  - HAS_PARTITION