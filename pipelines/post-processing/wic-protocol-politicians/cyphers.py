from datetime import datetime
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging


class ProtocolPoliticiansCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

        self.conditions = [
            {
                "condition": "Voting",
                "contexts": ["EngagedVoter"]
            },
            {
                "condition": "ProposalAuthor",
                "contexts": ["ProposalAuthor"] ## must pass
            },
            {
                "condition": "Delegation",
                "contexts": ["Delegate"]
            },
            {
                "condition": "Leadership",
                "contexts": ["DaoAdmin"]
            }
        ]
        self.subgraph_name = "ProtocolPoliticians"

    @count_query_logging
    def clear_subgraph(self):
        name = self.subgraph_name
        query = """
        match
            (w:_Wic:_{name})
        detach delete
            w
        return
            count(distinct(w))
        """.format(name=name)
        count = self.query(query)[0].value()

        return count 

    @count_query_logging
    def create_main(self):
        name = self.subgraph_name
        query = """
        create
            (m:_Wic:_Main:_{name})
        set
            m._displayName = "{name}"
        return
            count(m)
        """.format(name=name)
        count = self.query(query)[0].value()
        
        return count 

    @count_query_logging
    def create_condition(self):
        count = 0 
        name = self.subgraph_name
        conditions = self.conditions
        for con in conditions:
            condition = con['condition']
            query = """
            create
                (c:_Wic:_Condition:_{name}:_{condition})
            set
                c._displayName = '{condition}'
            return 
                count(distinct(c))
                """.format(name=name, condition=condition)
            count += self.query(query)[0].value()

        connect_conditions = """
        match
            (c:_Condition:_{name})
        match
            (m:_Main:_{name})
        with 
            c, m 
        merge
            (m)-[r:_HAS_CONDITION]->(c)
        return
            count(distinct(c))
        """.format(name=name)
        count += self.query(connect_conditions)[0].value()

        for cond in conditions:
            condition = cond['condition']
            for ctxt in cond['contexts']:
                query_context = """
                create
                    (c:_Wic:_Context:_{name}:_{condition}:_{ctxt})
                set
                    c._displayName = '{ctxt}'
                return
                    count(distinct(c))""".format(name=name, condition=condition, ctxt=ctxt)
                count += self.query(query_context)[0].value()

                connect_context = """
                match
                    (c:_Wic:_Context:_{name}:_{ctxt})
                match
                    (con:_Wic:_Condition:_{name}:_{condition})
                with
                    c, con
                merge
                    (c)-[r:_HAS_CONDITION]->(con)
                return 
                    count(distinct(c))
                """.format(name=name, ctxt=ctxt, condition=condition)
                count += self.query(connect_context)[0].value()

        return count 
    
    @count_query_logging
    def voters(self): ##FocusedVoter
        ### I would like to revisit this after we do network enrichment for ML. 
        ### I/e maybe we need to add VOTED edges between wallets and entities and put count on the edge
        query = """
         match
            (w:Wallet)-[r:VOTED]->(p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)
        match
            (wic:_Wic:_Context:_EngagedVoter)
        with
            w, count(distinct(p)) as votes, wic
        where
            votes > 10
        merge
            (w)-[r:_HAS_CONTEXT]->(wic)
        set
            r._count = votes
        return
            count(distinct(w))
        """
        count = self.query(query)[0].value()

        return count 


    @count_query_logging
    def proposal_author(self): ##FocusedProposalAuthor

        engaged_benchmark_query = """
        match
            (w:Wallet)-[r:AUTHOR]->(p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)
        with
            w, count(distinct(p)) as authored
        return 
            tofloat(apoc.agg.percentiles(authored, [.5])[0])
        """
        engaged_benchmark = self.query(engaged_benchmark_query)[0].value()

        engaged_query = """
        with 
            tofloat({engaged_benchmark}) as engaged_benchmark
        match
            (w:Wallet)-[r:AUTHOR]->(p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)
        match
            (wic:_Wic:_Context:_ProposalAuthor)
        with
            w, count(distinct(p)) as authored, engaged_benchmark, wic
        with
            w, wic, (tofloat(authored) / engaged_benchmark) as againstBenchmark
        merge
            (w)-[r:_HAS_CONTEXT]->(wic)
        set
            r._againstBenchmark = againstBenchmark
        return
            count(distinct(w))
        """.format(engaged_benchmark=engaged_benchmark)
        count = self.query(engaged_query)[0].value()

        return count 


    @count_query_logging
    def delegates(self): 
        delegates = """
        match
            (delegate:Wallet)<-[r:DELEGATED_TO]-(:Delegation)-[:DELEGATED]-(delegator)
        match
            (wic:_Wic:_Delegate:_Context)
        where 
            id(delegate) <> id(delegator)
        with
            delegate, wic, count(distinct(delegator)) as delegators
        merge
            (delegate)-[r:_HAS_CONTEXT]->(wic)
        set
            r._count = delegators
        return
            count(distinct(delegate))
        """
        count = self.query(delegates)[0].value()
        
        return count 


    @count_query_logging
    def dao_admins(self):
        query = f"""
        match
            (w:Wallet)-[r:CONTRIBUTOR]->(i:Entity)
        match
            (wic:_DaoAdmin:_ProtocolPoliticians)
        with 
            w,wic, count(distinct(i)) as contributing
        merge
            (w)-[r:_HAS_CONTEXT]->(wic)
        set
            r._count = contributing
        return
            count(distinct(w))
        """
        count = self.query(query)[0].value()

        return count



