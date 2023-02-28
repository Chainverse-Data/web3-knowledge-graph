import logging
from ...helpers import Processor
from .cyphers import LaborCyphers
from datetime import datetime, timedelta
import os
import re
import json
import time

class LaborRels(Processor):
    """Labels wallets that have sent funds into DAOs or received funds from DAOs"""
    def __init__(self):
        self.cyphers = LaborCyphers()
        super().__init__("web3-labor")
