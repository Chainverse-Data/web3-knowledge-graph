from datetime import datetime
import logging
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging


class FarmerCyphers(Cypher):

    def __init__(self, database=None):
        super().__init__(database)
        self.conditions = ["GovernanceFarming", "MarketplaceFarming"]


    @count_query_logging

    def clean_subgraph(self):
        query = """
        match 
            (w:_Wic:_IncentiveFarming)
        detach delete 
            w
        return 
            count(w) as count
        """
        count = self.query(query)[0].value()
        return count 
    
    @count_query_logging

    def create_subgraph(self):

        query_main_node = """
        with 
            apoc.create.uuid() as uuid,
            datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as datetime
        create 
            (wic:_Wic:_Main:_IncentiveFarming)
        set 
            wic._chainverseId = 'porWic' + substring(uuid, 1, 6)
        set 
            wic._createdDt = datetime
        set 
            wic._lastUpdateDt = datetime 
        set
            wic._displayName = "Incentive Farming"
            """
        main_response = self.query(query_main_node)

        for condition in self.conditions:

            condition_query = """
            with 
                apoc.create.uuid() as uuid,
                datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as datetime
            create
                (condition:_Wic:_Condition:_IncentiveFarming:_{condition})
            set 
                condition._chainverseId = 'porWic' + substring(uuid, 1, 6)
            set 
                condition._createdDt = datetime
            set 
                condition._lastUpdateDt = datetime 
            set 
                condition._displayName = '{condition}'
                """.format(condition=condition)
            condition_response = self.query(condition_query)

        
        connect_conditions = """
        with
            datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as datetime
        match 
            (main:_Wic:_Main:_IncentiveFarming)
        match
            (condition:_Wic:_Condition:_IncentiveFarming)
        with 
            main, condition, datetime
        merge 
            (main)-[r:_HAS_CONDITION]->(condition)
        set 
            r.createdDt = datetime
        return 
            count(distinct(condition))
        """

        self.query(connect_conditions)


    @count_query_logging

    def suspicious_daos_snapshot(self):

        snapshot_query = """
        with 
            apoc.create.uuid() as uuid,
            datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as datetime
        create
            (context:_Wic:_IncentiveFarming:_GovernanceFarming:_Context:_SuspiciousSnapshot)
        set 
            context._displayName = 'Suspicious Snapshot'
        set
            context.createdDt = datetime"""

        self.query(snapshot_query)

        snapshot_connect = """
        with 
            datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as datetime
        match
            (context:_Wic:_SuspiciousSnapshot:_Context)
        match
            (condition:_Wic:_Condition:_IncentiveFarming:_GovernanceFarming)
        with 
            context, condition, datetime
        merge 
            (context)-[r:HAS_CONDITION]->(condition)
        set 
            r.createdDt = datetime
        return
            count(distinct(context)) as count
        """

        self.query(snapshot_connect)

        connect_wallets = """
        with 
            datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as datetime
        match
            (context:_Wic:_SuspiciousSnapshot:_Context)
        match
            (wallet:Wallet)-[r:VOTED]->(p:Proposal)-[:HAS_PROPOSAL]-(entity:Entity)-[:HAS_STRATEGY]-(token:Token)
        where 
            token.symbol in ['DAI', 'USDC', 'ETH', 'WETH', 'USDT']
        with 
            wallet, context, datetime, entity.snapshotId as snapshotId
        merge 
            (wallet)-[con:HAS_CONTEXT]->(context)
        set 
            con.createdDt = datetime
        set 
            con._context = "This wallet participated in a DAO that does not use its native token to establish voting power: " +  snapshotId + " ."
        return
            count(distinct(wallet)) as count
        """
        
        count = self.query(connect_wallets)[0].value()

        return count 

    @count_query_logging
    def suspicious_mirror(self):

        subgraph_query = """
        with 
            datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as datetime
        create
            (context:_Wic:_IncentiveFarming:_MarketplaceFarming:_Context:_Mirror)
        set
            context.createdDt = datetime
        set
            context._displayName = "Mirror Farming"
        return 
            count(context) as count
        """

        self.query(subgraph_query)

        subgraph_connect = """
        with 
            datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as datetime
        match 
            (context:_Wic:_IncentiveFarming:_MarketplaceFarming:_Context:_Mirror)
        match
            (condition:_Wic:_Condition:_MarketplaceFarming)
        with 
            context, condition, datetime 
        merge
            (context)-[r:HAS_CONDITION]->(condition)
        set
            r.createdDt = datetime
        return 
            count(context) as context
        """

        self.query(subgraph_connect)


        get_extreme = """
        match 
            (w:Wallet)-[r:AUTHOR]->(a:Article)
        with 
            w, count(distinct(a)) as articles
        with 
            apoc.agg.percentiles(articles, [.995]) as arts
        return 
            arts[0] as cutoff"""

        cutoff = self.query(get_extreme)[0].value()

        connect_extreme = f"""
        match 
            (w:Wallet)-[r:AUTHOR]->(a:Article)
        with 
            w, count(distinct(a)) as articles
        where 
            articles > {cutoff}
        match 
            (w)
        match 
            (context:_Context:_Mirror:`_MarketplaceFarming`)
        with 
            datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as datetime, w, context
        merge
            (w)-[con:HAS_CONTEXT]->(context)
        set
            con.createdDt = datetime
        set
            con._context = "This wallet has published an more articles on mirror than 99.995% of Mirror authors: " + "https://mirror.xyz/" + w.address
        return 
            count(distinct(w)) as count
        """

        count = self.query(connect_extreme)[0].value()
        return count 

    @count_query_logging
    def cosigner_expansion(self):

        subgraph = """
        with 
            datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as datetime
        create
            (context:_Wic:_IncentiveFarming:_Context:_Extensions:_Cosigners)
        set 
            context._displayName = 'Incentive Farming - Co-signer'
        set
            context.createdDt = datetime
        with 
            context, datetime
        match
            (main:_Wic:_IncentiveFarming:_Main)
        with 
            context, main, datetime
        merge 
            (context)-[r:HAS_CONTEXT]->(main)
        set
            r.createdDt = datetime
        """
        self.query(subgraph)
        

        connect = """
        with 
            datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')) as datetime
        match
            (wallet:Wallet)-[r:HAS_CONTEXT]->(context:_IncentiveFarming:_Context)
        match
            (wallet:Wallet)-[:IS_SIGNER]-(:MultiSig)-[:IS_SIGNER]-(otherwallet)
        match
            (cosigners:_Wic:_IncentiveFarming:_Context:_Extensions:_Cosigners)
        where not
            (otherwallet)-[:HAS_CONTEXT]->(:_IncentiveFarming)
        with 
            otherwallet, cosigners, wallet, datetime
        match
            (otherwallet)
        match
            (cosigners)
        match
            (wallet)
        merge
            (otherwallet)-[con:HAS_CONTEXT]->(cosigners)
        merge
            (otherwallet)-[conbud:HAS_CONTEXT_BUDDY]->(wallet)
        set 
            conbud.`_context` = cosigners.`_displayName`
        set
            con.createdDt = datetime
        set 
            conbud.createdDt = datetime
        return count(otherwallet)        
        """
        count = self.query(connect)[0].value()
        return count


        
        
