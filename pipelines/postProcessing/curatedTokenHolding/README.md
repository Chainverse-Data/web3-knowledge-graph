# Curated token holders pipeline

This pipeline gets all the holders for a curated set of tokens. It processes the ERC20 and ERC721|ERC1155 tokens.

Any token with the `manualSelection` property set to `true` will be added to the pipeline.

# Ontology

Nodes:
- (Wallet)
- (Token:ERC20)
- (Token:ERC721)
- (Token:ERC1155)

Edges:
- (Wallet)-[HOLDS_TOKEN]->(Token:ERC721|ERC1155)
  - tokenID: The token ID
  - balance: The balance
- (Wallet)-[HOLDS]->(Token:ERC20)
  - tokenID: The token ID
  - balance: The balance


# ERC20 selectors

## citizen ERC20

Gets the token held by at least 25% of "good" Web3 citizen

```cypher
MATCH (w:Wallet)-[r:HOLDS]->(t:Token)
WHERE t:ERC20
WITH t, count(distinct(w)) AS count_holders
WHERE count_holders > 250 
MATCH (w)-[r:HOLDS]->(t:Token:ERC721)
MATCH (w)-[:_HAS_CONTEXT]->(context:_Wic)
WHERE not context:_IncentiveFarming
WITH t, count_holders, count(distinct(w)) AS count_wic
WITH t, tofloat(count_wic) / count_holders AS thres
WHERE thres > {propotion}
RETURN distinct(t.address) AS address
```

## over represented ERC20

Gets the token held by at least 5% of all wallets in the graph

```cypher
MATCH (wallet)-[r:HOLDS]->(token:Token)
WHERE token:ERC20 AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
WITH count(distinct(wallet)) as citizens
MATCH (wallet)-[r:HOLDS]->(token:Token)
WHERE token:ERC20 AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
WITH token, apoc.node.degree(token, "HOLDS") AS holders, citizens
WHERE toFloat(holders)/toFloat(citizens) > {propotion}
RETURN distinct(token.address) AS address
```

## manual selection

Gets the tokens that were manually added to the selection

```cypher
MATCH (t:Token)
WHERE t:ERC20 AND t.manualSelection
RETURN distinct(t.address) AS address
```

# ERC721|ERC1155 tokens

## Good web3 citizen

Token held by 25% of the good web3 citizens

```cypher
MATCH (w:Wallet)-[r:HOLDS]->(t:Token)
WHERE t:ERC721 OR t:ERC1155
WITH t, count(distinct(w)) AS count_holders
WHERE count_holders > 250 
MATCH (w)-[r:HOLDS]->(t:Token:ERC721)
MATCH (w)-[:_HAS_CONTEXT]->(context:_Wic)
WHERE not context:_IncentiveFarming
WITH t, count_holders, count(distinct(w)) AS count_wic
WITH t, tofloat(count_wic) / count_holders AS thres
WHERE thres > {propotion}
RETURN distinct(t.address) AS address
```

## Over represented tokens

Token held by at least 5% of the wallets in the graph

```cypher
MATCH (wallet)-[r:HOLDS]->(token:Token)
WHERE (token:ERC721 OR token:ERC1155) AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
WITH count(distinct(wallet)) as citizens
MATCH (wallet)-[r:HOLDS]->(token:Token)
WHERE (token:ERC721 OR token:ERC1155) AND NOT (wallet)-[:HAS_CONTEXT]->(:_Wic:_IncentiveFarming)
WITH token, apoc.node.degree(token, "HOLDS") AS holders, citizens
WHERE toFloat(holders)/toFloat(citizens) > {propotion}
RETURN distinct(token.address) AS address
```

## Blue chip tokens

Tokens with a floor price over 10ETH

```cypher
MATCH (t:Token)
WHERE (t:ERC721 OR t:ERC1155) AND t.floorPrice > {min_price}
RETURN distinct(t.address) AS address
```

## Manual selection

```cypher
MATCH (t:Token)
WHERE (t:ERC721 OR t:ERC1155) AND t.manualSelection
RETURN distinct(t.address) AS address
```