# Twitter Website extraction Post-Processing

This post-processor finds the websites in the twitter nodes and the handles in the twitter bios.

It resolves the websites from minimfied twitter urls to the complete URL and the domain.

It extracts the mentionned handles in the bio and links the mentionned twitter accounts.

## Ontology

Creates new nodes:
`Website`:
  - url: The url of the website

`Domain`:
  - domain: The domain name

It creates the following relationships
- (Twitter)-[:BIO_MENTIONED]->(Twitter)
- (Twitter)-[:HAS_WEBSITE]->(Website)
- (Website)-[:HAS_DOMAIN]->(Domain)

## Bucket

This post-processor reads and writes from the following bucket: `twitter-relations`

## Flags

No unique flags created
