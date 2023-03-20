# Token Metadata Post-Processing

This post-processor finds ERC-20 tokens without name and logos and attempts to fill in the missing data.

## APIs
This uses the Alchemy ethereum endpoint: `alchemy_getTokenMetadata`

## Ontology

Adjusts `token` nodes
Properties:

- `name` (str)
- `logo` (str)
- `decimals` (int)

## Bucket

This scraper reads and writes from the following bucket: `token-metadata`

## Flags

No unique flags created
