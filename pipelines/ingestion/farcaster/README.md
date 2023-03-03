# Farcaster Ingestor

This ingestor will take care of data obtained by the Farcaster scraper.

# Ontology

Nodes:
- Farcaster:Account
  - id
  - name
  - address
  - url
  - fId
- Wallet

Edges:
- (Wallet)-[HAS_ACCOUNT]->(Farcaster:Account)

# Bucket

This ingestor reads and writes from the following bucket: `farcaster`

# Metadata

No unique metadata created

# Flags

No unique flags created