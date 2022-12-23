# Token-Holders Ingestor

This ingestor will take care of data obtained by the Token Holders scraper.

# Ontology

Nodes:

- Token:`token_type`
  - symbol
  - decimal
- Wallet

`token_type` can be: ERC20 | ERC721 | ERC1155

Edges:
- (wallet)-[edge:HOLDS]->(token)
  - balance
  - numericBalance

# Bucket

This ingestor reads and writes from the following bucket: `token-holders`

# Metadata

No unique metadata created

# Flags

No unique flags created
