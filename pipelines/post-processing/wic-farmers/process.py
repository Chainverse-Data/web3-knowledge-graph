import logging
from ..helpers import Processor
from .cyphers import FarmerCyphers
from datetime import datetime, timedelta
import os
import re
import json
import time

class IncentiveFarmerProcessor(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""
    def __init__(self):
        self.cyphers = FarmerCyphers()
        super().__init__("wic-farming")

    def clean_subgraph(self):
        logging.info("Cleaning subgraph")
        self.cyphers.clean_subgraph()
        logging.info("Subgraph cleaned")

    def build_subgraph(self):
        logging.info("Building Incentive Farmer Subgraph...")
        self.cyphers.create_subgraph()
        logging.info("Subgraph created")

    def connect_suspicious_snapshot(self):
        logging.info("Connecting suspicious snapshot wallets")
        self.cyphers.suspicious_daos_snapshot()
        logging.info("Wallets connected")
    
    def connect_suspicious_mirror(self):
        logging.info("Connecting suspicious mirror authors...")
        self.cyphers.suspicious_mirror()
        logging.info("Suspicious authors connected")
    
    def connect_multisig_cosigners(self):
        logging.info("Connecting suspicious multisig cosigners...")
        self.cyphers.cosigner_expansion()
        logging.info("Cosigners connected")



    def run(self):
        self.clean_subgraph()
        self.build_subgraph()
        self.connect_suspicious_snapshot()
        self.connect_suspicious_mirror()
        self.connect_multisig_cosigners()




if __name__ == '__main__':
    processor = IncentiveFarmerProcessor()
    processor.run()