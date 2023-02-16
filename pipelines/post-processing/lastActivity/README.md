# Last Activity post-processing

This module gets the first and last transaction date for all wallets in the graph. It uses Alchemy to get those actions. The following chains are scrapped:
- ethereum
- optimism
- arbitrum
- solana
- polygon

## Input

This module will get all the Wallet nodes and all the Wallet nodes with a missing FirstTx.

## Process

For each node without a FirstTx, the module calls the Alchemy endpoint `alchemy_getAssetTransfers` to recover the first transaction for this wallet. It does this for all chains described above. 

The module does the same for all wallets but calling for the LastTx then, to recover the most recent activity of a wallet. 

Finally it writes back in the Neo4J database by updating the wallet properties.

## Ontology changes

The module adds the following properties to the `Wallet` nodes:

- Wallet:
  - ethereumFirstTxDate: datetime or null
  - polygonFirstTxDate: datetime or null
  - optimisimFirstTxDate: datetime or null
  - arbitrumFirstTxDate: datetime or null
  - solanaFirstTxDate: datetime or null
  - ethereumLastTxDate: datetime or null
  - polygonLastTxDate: datetime or null
  - optimisimLastTxDate: datetime or null
  - arbitrumLastTxDate: datetime or null
  - solanaLastTxDate: datetime or null