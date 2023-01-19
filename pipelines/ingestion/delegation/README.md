# Delegation Ingestor

This ingestor will take care of data obtained by the Delegation scraper.

# Ontology

Nodes:

- Wallet:Delegation
- Delegation:Transaction
- Token:ERC20
- Entity

Edges:

- Entity -[HAS_STRATEGY]-> Token:ERC20
- Wallet -[DELEGATED]-> Delegation:Transaction
- Delegation:Transaction -[DELEGATED_TO]-> Wallet
- 

# Bucket

This ingestor reads and writes from the following bucket: `delegation`

# Metadata

No unique metadata created

# Flags

No unique flags created
