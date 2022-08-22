# shoot-your-snapshot

Pulls snapshot from snapshot so you can have a snapshot of your snapshot on your snapshot.

# Snapshot data pipeline

Objective: Once a week pull all available data for each on-chain entity that uses Snapshot from the Snapshot API (https://docs.snapshot.org/).

- Store raw data in S3

### s3 Data Storage

All data is stored in the **diamond-dao** AWS account in the **chainverse** bucket

**Spaces:** snapshot/spaces/`MM-DD-YYYY`/spaces.json\
**Proposals:** snapshot/proposals/`MM-DD-YYYY`/proposals.json\
**Votes:** snapshot/votes/`MM-DD-YYYY`/votes.json
