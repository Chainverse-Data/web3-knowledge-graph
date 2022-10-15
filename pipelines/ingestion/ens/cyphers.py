from ...helpers import Cypher
from ...helpers import Constraints
from ...helpers import Indexes
from ...helpers import Queries
from ...helpers import count_query_logging
import logging
import sys


class EnsCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_constraints(self):
        constraints = Constraints()
        constraints.wallets()
        constraints.aliases()
        constraints.ens()
        constraints.transactions()

    def create_indexes(self):
        indexes = Indexes()
        indexes.wallets()
        indexes.aliases()
        indexes.ens()
        indexes.transactions()

    @count_query_logging
    def create_or_merge_ens_items(self, urls):
        count = self.queries.create_or_merge_ens(urls)
        return count

    @count_query_logging
    def link_ens(self, urls):
        count = self.queries.link_ens(urls)
        return count
