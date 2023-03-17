from ...helpers import Cypher
from ...helpers import Queries
from ...helpers import count_query_logging


class NFTfiCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_indexes(self):
        query = """CREATE INDEX NFTfiLoans IF NOT EXISTS FOR (n:NFTfi) ON (n.loanId)"""
        self.query(query)

    @count_query_logging
    def create_or_merge_loans(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MERGE (loan:NFTfi:Loan {{loanId: toInteger(data.loanId)}})
                ON CREATE SET loan.uuid = apoc.create.uuid(),
                    loan.loanPrincipalAmount = data.loanPrincipalAmount, 
                    loan.maximumRepaymentAmount = data.maximumRepaymentAmount, 
                    loan.loanStartTime = datetime({{epochSeconds: toInteger(data.loanStartTime)}}), 
                    loan.loanDuration = toInteger(data.loanDuration), 
                    loan.loanInterestRateForDurationInBasisPoints = toInteger(data.loanInterestRateForDurationInBasisPoints), 
                    loan.interestIsProRated = toBoolean(data.interestIsProRated), 
                    loan.txHash = data.transactionHash, 
                    loan.contractAddress = data.contractAddress, 
                    loan.blockNumber = toInteger(data.blockNumber), 
                    loan.createdDt = datetime(apoc.date.toISO8601(toInteger(data.createdDate), 'ms')),
                    loan.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                ON MATCH SET loan.loanPrincipalAmount = data.loanPrincipalAmount, 
                    loan.maximumRepaymentAmount = data.maximumRepaymentAmount, 
                    loan.loanStartTime = datetime({{epochSeconds: toInteger(data.loanStartTime)}}), 
                    loan.loanDuration = toInteger(data.loanDuration), 
                    loan.loanInterestRateForDurationInBasisPoints = toInteger(data.loanInterestRateForDurationInBasisPoints), 
                    loan.interestIsProRated = toBoolean(data.interestIsProRated), 
                    loan.txHash = data.transactionHash, 
                    loan.contractAddress = data.contractAddress, 
                    loan.blockNumber = data.blockNumber, 
                    loan.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                return count(loan)
            """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def link_or_merge_loans_borrowers(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (borrower:Wallet {{address: toLower(data.borrower)}})
                MATCH (loan:NFTfi:Loan {{loanId: toInteger(data.loanId)}})
                MERGE (borrower)-[r:BORROWED]->(loan)
                RETURN count(r)
            """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def link_or_merge_loans_lenders(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (lender:Wallet {{address: toLower(data.lender)}})
                MATCH (loan:NFTfi:Loan {{loanId: toInteger(data.loanId)}})
                MERGE (lender)-[r:LENT]->(loan)
                RETURN count(r)
            """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def link_or_merge_loans_collateral(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (collateral:Token:ERC721 {{address: toLower(data.nftCollateralContract)}})
                MATCH (loan:NFTfi:Loan {{loanId: toInteger(data.loanId)}})
                MERGE (collateral)-[r:IS_COLLATERAL]->(loan)
                SET r.tokenId = data.nftCollateralId
                RETURN count(r)
            """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def link_or_merge_loans_demonination(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (denomination:Token:ERC20 {{address: toLower(data.loanERC20Denomination)}})
                MATCH (loan:NFTfi:Loan {{loanId: toInteger(data.loanId)}})
                MERGE (denomination)-[r:IS_DENOMINATION]->(loan)
                RETURN count(r)
            """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def set_loan_status(self, urls, status):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (loan:NFTfi:Loan {{loanId: toInteger(data.loanId)}})
                SET loan:{status}
                SET loan.{status.lower()}blockNumber = toInteger(data.blockNumber)
                SET loan.{status.lower()}txHash = toInteger(data.transactionHash)
                RETURN count(loan)
            """
            count += self.query(query)[0].value()
        return count