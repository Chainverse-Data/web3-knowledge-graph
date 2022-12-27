import logging
from ..helpers import Processor
from .cyphers import ProtocolPoliticiansCyphers
from datetime import datetime, timedelta

class ProtocolPoliticiansProcessor(Processor):

    def __init__(self):
        self.cyphers = ProtocolPoliticiansCyphers()
        super().__init__("protocol-politicians")

    def run(self):
        self.cyphers.clear_subgraph()
        self.cyphers.create_main()
        self.cyphers.create_condition()
        self.cyphers.proposal_author()
        self.cyphers.delegates()
        self.cyphers.dao_admins()
        self.cyphers.voters()

if __name__=="__main__":
    processor = ProtocolPoliticiansProcessor()
    processor.run()

