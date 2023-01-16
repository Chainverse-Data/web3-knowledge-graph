# Mirror Ingestor

This ingestor will take care of data obtained by the Mirror scraper.

# Ontology

Nodes:
- Mirror:MirrorArticle:Article
- Mirror:Token:ERC721
- Twitter:Account
- Wallet

Edges:
- Wallet -[IS_AUTHOR]-> Mirror:Article
- Mirror:MirrorArticle:Article -[HAS_NFT]-> Mirror:Token:ERC721
- Mirror:MirrorArticle:Article -[REFERENCES]-> Twitter
- Wallet -[IS_OWNER]-> Mirror:Token:ERC721
- Wallet -[IS_RECEIPIENT]-> Mirror:Token:ERC721

# Bucket

This ingestor reads and writes from the following bucket: `mirror`

# Metadata

No unique metadata created

# Flags

No unique flags created