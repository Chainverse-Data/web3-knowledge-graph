

import logging
from .cyphers import AccountsCyphers
from ..helpers import Processor

class AccountsProcessor(Processor):
    def __init__(self):
        self.cyphers = AccountsCyphers()
        self.audiences = {
            "Dune wizards": {
                "audienceId": "duneWizards",
                "wic_target": "DuneWizard",
                "name": "Dune wizards",
                "imageUrl": "",
                "description": ""
            },
            "Blur Power Traders": {
                "audienceId": "blurTraders",
                "wic_target": "BlurPowerUser",
                "name": "Blur Power Traders",
                "imageUrl": "",
                "description": ""
            },
            "Web3 Music Collectors": {
                "audienceId": "web3MusicCollectors",
                "wic_target": "Web3MusicCollector",
                "name": "Web3 Music Collectors",
                "imageUrl": "",
                "description": ""
            },
            "Web3 Founders": {
                "audienceId": "web3Founders",
                "wic_target": "Founder",
                "name": "Web3 Music Collectors",
                "imageUrl": "",
                "description": ""
            }
        }
        super().__init__(bucket_name="audiences-processing")

    def process_audiences(self):
        for audience in self.audiences:
            self.cyphers.create_audience(self.audiences[audience]["audienceId"], self.audiences[audience]["name"], self.audiences[audience]["imageUrl"], self.audiences[audience]["description"])
            self.cyphers.create_audience_by_condition_or_context(self.audiences[audience]["wic_target"], self.audiences[audience]["audienceId"])

    def run(self):
        self.process_audiences()

if __name__ == "__main__":
    P = AccountsProcessor()
    P.run()