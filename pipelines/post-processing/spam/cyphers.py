from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging

class SpamCyphers(Cypher):
    ### class to identify spam contracts / spammers to leverage in WIC Incentive Farming
    def __init__(self, database=None):
        super().__init__(database)
    
    @count_query_logging
    def label_spam_contracts(self, contractAddresses):
        query = f"""
        match
            (token:Token)
        where
            token.address in {contractAddresses}
        and not
            token:SpamContract
        set
            token:SpamContract
        return 
            count(token)
        """
        count = self.query(query)[0].value()

        return count 

    ### add 


        
