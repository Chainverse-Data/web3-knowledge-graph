# Lens Ingestor

This ingestor will take care of data obtained by the Lens scraper.

# Ontology

Nodes:
- Alias:Lens
  - name
  - creator
  - owner
  - profileId
  - createdOn
- Wallet

Edges:
- (Wallet)-[HAS_ALIAS]->(Alias:Lens)

# Bucket

This ingestor reads and writes from the following bucket: `lens`

# Metadata

No unique metadata created

# Flags

No unique flags created


