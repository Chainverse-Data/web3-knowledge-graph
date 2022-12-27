from datetime import datetime
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging


class ProtocolPoliticiansCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

        self.conditions = [
            {
                "condition": "Voting",
                "contexts": ["Voter", "EngagedVoter"]
            },
            {
                "condition": "ProposalAuthor",
                "contexts": ["ProposalAuthor", "EngagedProposalAuthor"] ## must pass
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

        connect_conditions  = """
        match
            (c:_Condition:_{name})
        match
            (m:_Main:_{name})
        with 
            c, m 
        merge
            (m)-[r:_HAS_CONDITION]->(c)
        return
            count(distinct(mc))
        """.format(name=name)
        count + self.query(connect_conditions)
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
                count += self.query(query_context)

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
                count += self.query(connect_context)

        return count 
    
    @count_query_logging
    def voters(self): ##FocusedVoter
        ### I would like to revisit this after we do network enrichment for ML. 
        ### I/e maybe we need to add VOTED edges between wallets and entities and put count on the edge
        benchmark_query = """
        match
            (w:Wallet)-[r:VOTED]->(p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)
        with
            w, count(distinct(p)) as votes
        return 
            tofloat(apoc.agg.percentiles(votes, [.95])[0])
        """
        benchmark = self.query(benchmark_query)[0].value()

        connect_query = """
        with 
            {benchmark} as benchmark
        call
            apoc.periodic.commit('
            match (w:Wallet)-[r:VOTED]->(p:Proposal)
            where not (w)-[:_HAS_CONTEXT]->(:_Context:_Voter)
            with w, benchmark
            limit 10000
            match (w:Wallet)-[r:VOTED]->(p:Proposal)
            match (wic:_Wic:_Context:_Voter)
            with w, count(distinct(p)) as votes, benchmark, wic
            with w, (tofloat(votes) / benchmark) as againstBenchmark, wic
            merge
                (w)-[r:_HAS_CONTEXT]->(wic)
            set
                r._againstBenchmark = againstBenchmark
            return
                count(w)')        
        """.format(benchmark=benchmark)
        self.query(connect_query)

        engaged_query = """
        with 
            {benchmark} as benchmark
        match 
            (w:Wallet)-[r:VOTED]->(p:Proposal)
        match
            (wic:_Wic:_Context:_EngagedVoter)
        with 
            w, count(distinct(p)) as votes, benchmark, wic
        where
            votes > benchmark
        merge
            (w)-[r:_HAS_CONTEXT]->(wic)
        return 
            count(distinct(w))
        """
        count = self.query(engaged_query)



    def proposal_author(self): ##FocusedProposalAuthor

        normie_benchmark_query = """
        match
            (w:Wallet)-[r:AUTHOR]->(p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)
        with
            w, count(distinct(p)) as authored
        return 
            tofloat(apoc.agg.percentiles(authored, [.5])[0])
        """

        normie_benchmark = self.query(normie_benchmark_query)[0].value()

        engaged_benchmark_query = """
        match
            (w:Wallet)-[r:AUTHOR]->(p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)
        with
            w, count(distinct(p)) as authored
        return 
            tofloat(apoc.agg.percentiles(authored, [.95])[0])
        """
        engaged_benchmark = self.query(engaged_benchmark_query)[0].value()

        connect_query = """
        with 
            {normie_benchmark} as benchmark
        call
            apoc.periodic.commit('
            match (w:Wallet)-[r:AUTHOR]->(p:Proposal)
            where not (w)-[:_HAS_CONTEXT]->(:_Context:_ProposalAuthor)
            with w, benchmark
            limit 10000
            match (w:Wallet)-[r:AUTHOR]->(p:Proposal)
            match (wic:_Wic:_Context:_ProposalAuthor)
            with w, count(distinct(p)) as authored, benchmark, wic
            with w, (tofloat(authored) / benchmark) as againstBenchmark, wic
            merge
                (w)-[r:_HAS_CONTEXT]->(wic)
            set
                r._againstBenchmark = againstBenchmark
            return
                count(w)')        
        """.format(normie_benchmark=normie_benchmark)
        self.query(connect_query)

        engaged_query = """
        with 
            {engaged_benchmark} as engaged_benchmark
        match
            (w:Wallet)-[r:AUTHOR]->(p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)
        match
            (wic:_Wic:_Context:_EngagedVoter)
        with
            w, count(distinct(p)) as authored, engaged_benchmark, wic
        where
            authored > engaged_benchmark
        merge
            (w)-[r:_HAS_CONTEXT]->(wic)
        return
            count(distinct(w))
        """.format(engaged_benchmark=engaged_benchmark)
        count = self.query(engaged_query)[0].value()

        return count 
    
    
    def delegates(self): 
        
        normie_benchmark_query = """
        match
            (delegate:Wallet)<-[r:DELAGATED_TO]-(:Delegation)-[:DELEGATED]-(delegator)
        where not
            id(delegate) = id(delegator)
        with
            delegate, count(distinct(delegator)) as delegators
        return tofloat(apoc.agg.percentiles(delegators, [.5])) as benchmark
        """

        normie_benchmark = self.query(normie_benchmark_query)[0].value()

        connect = """
        with
            {normie_benchmark} as benchmark
        match
            (delegate:Wallet)<-[r:DELAGATED_TO]-(:Delegation)-[:DELEGATED]-(delegator)
        match
            (wic:_Wic:_Delegate)
        where not
            id(delegate) = id(delegator)
        with
            delegate, wic, count(distinct(delegator)) as count_represented, benchmark 
        with
            delegate, wic, (tofloat(count_represented) / benchmark) as againstBenchmark
        merge
            (delegate)-[r:_HAS_CONTEXT]->(wic)
        set
            r._againstBenchmark = againstBenchmark
        return
            count(distinct(delegate))
        """.format(normie_benchmark=normie_benchmark)

        based_benchmark_query = """
        match
            (delegate:Wallet)<-[r:DELAGATED_TO]-(:Delegation)-[:DELEGATED]-(delegator)
        where not
            id(delegate) = id(delegator)
        with
            delegate, count(distinct(delegator)) as delegators
        return tofloat(apoc.agg.percentiles(delegators, [.95])) as benchmark
        """

        based_benchmark = self.query(based_benchmark_query)[0].value()



