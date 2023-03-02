# Github Post Processor

This processor takes care of going through the Neo4J database, looking for github Github:Accounts, and then will go through each Github:Repository they own to collect information about who they work with, as well as their declared metadata, the readme and languages.

# Configuration

This uses the github API, delcared through the ENV var: `GITHUB_API_KEY`. This env var can accept multiple Github keys, that will be used in a rotating manner. To declare multiple keys, write each token comma seperated.

# Ontology

Nodes:
  - Github:Account
  - Github:Repository
  - Twitter:Account
  - Email:Account
Edges:
  - (Github:Account)-[IS_FOLLOWING]->(Github:Account)
  - (Github:Account)-[IS_OWNER]->(Github:Repository)
  - (Github:Account)-[IS_CONTRIBUTOR]->(Github:Repository)
  - (Github:Account)-[IS_SUBSCRIBER]->(Github:Repository)
  - (Github:Account)-[HAS_ACCOUNT]->(Email:Account)
  - (Github:Account)-[HAS_ACCOUNT]->(Twitter:Account)