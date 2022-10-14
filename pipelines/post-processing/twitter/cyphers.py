from ...helpers import Cypher

class TwitterCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    def create_constraints(self):
        pass

    def create_indexes(self):
        pass

    # Do some stuff