# Ens Ingestor

This ingestor will take care of data obtained by the Ens scraper.

# Ontology

Nodes:

- Alias
- Ens:Nft
- Wallet
- Transaction

Edges:

- (Wallet)-[HAS_ALIAS]->(Alias)
- (Wallet)-[RECEIVED]->(Transaction)
- (Ens)-[TRANSFERRED]->(Transaction)
- (Ens)-[HAS_NAME]->(Alias)

# Bucket

This ingestor reads and writes from the following bucket: `ens`

# Metadata

No unique metadata created

# Flags

No unique flags created
