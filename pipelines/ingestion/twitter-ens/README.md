# Twitter-Ens Ingestor

This ingestor will take care of data obtained by the twitter-ens scraper.

# Ontology

Nodes:

- Alias
- Wallet
- Twitter:Account

Edges:

- (Twitter)-[HAS_ALIAS]->(Alias)
- (Wallet)-[HAS_ALIAS]->(Alias)

# Bucket

This ingestor reads and writes from the following bucket: `twitter-ens`

# Metadata

No unique metadata created

# Flags

No unique flags created
