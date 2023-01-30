# Delegation Ingestor

This ingestor will take care of data obtained by the Delegation scraper.

# Ontology

Nodes:

- Wallet
- Delegation
- Token:ERC20
- Entity

Edges:

- Entity -[HAS_STRATEGY] {delegation:true} -> Token:ERC20
- Entity -[HAS_DELEGATION]-> Delegation
- Delegation -[USE_TOKEN]-> Token:ERC20
- Wallet -[IS_DELEGATE]-> Delegation
- Wallet -[IS_DELEGATING]-> Delegation
- Wallet -[DELEGATES_TO]-> Wallet 

# Bucket

This ingestor reads and writes from the following bucket: `delegation`

# Metadata

No unique metadata created

# Flags

No unique flags created
