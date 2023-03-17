# NFTfi Scraper

This scraper reads the NFTfi contract to extract the loan started, repayed and liquidated.

# Data

```json
{
    "LoanStarted": [<LoanStarted Object>],
    "LoanRepaid": [<LoanRepaid Object>],
    "LoanLiquidated": [<LoanLiquidated Object>],
}
```

## LoanStarted Object example

```json
{
  "loanId": 6000,
  "borrower": "0x8264e9e0f4CbcBBbb3F8eCAec0A625B590Ae790e",
  "lender": "0x4DAF2C826A63d065A673bDd8B49d0F71656532a9",
  "loanPrincipalAmount": 1490000000000000000,
  "maximumRepaymentAmount": 1538986301369863000,
  "nftCollateralId": 5350,
  "loanStartTime": 1648318408,
  "loanDuration": 2592000,
  "loanInterestRateForDurationInBasisPoints": 4294967295,
  "nftCollateralContract": "0x892848074ddeA461A15f337250Da3ce55580CA85",
  "loanERC20Denomination": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
  "interestIsProRated": false,
  "event": "LoanStarted",
  "transactionHash": "b'\\x8c\\xdf\\xa9\\xcbW\\x91\\xd0\\x0e\\xdf{\\xda\\xa3o\\xc7\\xcb>\\xd0[\"\\xbbM<\\xebB\\x02\\xd2S\\xf4p\\x87\\xdf\\x86'",
  "contractAddress": "0x88341d1a8f672d2780c8dc725902aae72f143b0c",
  "blockNumber": 14463390
}
```

## LoanRepaid Object example
```json
{
  "loanId": 6621,
  "borrower": "0xD79b937791724e47F193f67162B92cDFbF7ABDFd",
  "lender": "0x23b4cD421B7cB7d274689B2A7c100BAF8546941B",
  "loanPrincipalAmount": 80000000000000000000000,
  "nftCollateralId": 7221,
  "amountPaidToLender": 81873972603000000000000,
  "adminFee": 98630137000000000000,
  "nftCollateralContract": "0xb7F7F6C52F2e2fdb1963Eab30438024864c313F6",
  "loanERC20Denomination": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
  "event": "LoanRepaid",
  "transactionHash": "b'\\xe2\\xa6\\xa4\\tA\\xc5\\xa2\\xd6\\xc5\\xbc\\x1f\\x0cp\\xefs5\\xc4\\x18\\xc1\\x0bF\\xadAap\\xbf\\xce\\xd5}?]?'",
  "contractAddress": "0x88341d1a8f672d2780c8dc725902aae72f143b0c",
  "blockNumber": 15069396
}
```

## LoanLiquidated Object example

```json
{
  "loanId": 10,
  "borrower": "0xe09b8a054DfCda9C6A5f90D85066D9b6D1BD8025",
  "lender": "0xc35A5FEc6BE6957899E15559Be252Db882220b37",
  "loanPrincipalAmount": 30000000000000000,
  "nftCollateralId": 2612,
  "loanMaturityDate": 1589646285,
  "loanLiquidationDate": 1590628011,
  "nftCollateralContract": "0xF3E778F839934fC819cFA1040AabaCeCBA01e049",
  "event": "LoanLiquidated",
  "transactionHash": "b'7\\xf6\\xca\\xb8\\xb2\\xaa9\\t9\\xbdf\\xd5\\x1d\\x9c\\xfc<W\\x1c\\x84\\xe9\\x19\\x0e\\x00\\x94\\x06\\xeb\\xaf7P\\x1a\\x95\\xbd'",
  "contractAddress": "0x88341d1a8f672d2780c8dc725902aae72f143b0c",
  "blockNumber": 10151216
}
```

# Bucket

This scraper reads and writes from the following bucket: `nftfi`

# Metadata

The scraper reads and writes the following metadata variables:

- start_block Sets the last execution block as cutoff

# Flags

No unique flags created
