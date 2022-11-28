# Mirror Ingestor

This ingestor will take care of data obtained by the Mirror scraper.

# Ontology

Nodes:
- Mirror:Article
- Mirror:ERC721
- Twitter:Account
- Wallet

Edges:
- Wallet -[IS_AUTHOR]-> Mirror:Article
- Mirror:Article -[HAS_NFT]-> Mirror:ERC721
- Mirror:Article -[REFERENCES]-> Twitter
- Wallet -[IS_OWNER]-> Mirror:ERC721
- Wallet -[IS_RECEIPIENT]-> Mirror:ERC721

# Bucket

This ingestor reads and writes from the following bucket: `mirror`

# Metadata

No unique metadata created

# Flags

No unique flags created