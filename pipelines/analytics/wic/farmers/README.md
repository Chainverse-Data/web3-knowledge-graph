# Farmers

This Wallet In context looks for wallet engaged in farming activities.

# GovernanceFarming

## SuspiciousSnapshot

Looks for wallets that have voted on suspicous snapshot DAOs. Suspicious is defined as DAOs using stable coins as their voting tokens.

# MarketplaceFarming

## Mirror

Looks for wallet with Mirror authorship and finds the 99.5% percentile ofthe number of mirror articles authored by a single wallet. It flags the wallet as suspiciously farming mirror.

# Extensions

## Cosigners

This extends the previous two by looking for wallets who co-signed on Multisigs, and flags them as suspicious co-signers.
