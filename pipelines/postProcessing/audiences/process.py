

import logging
import re
from .cyphers import AccountsCyphers
from ..helpers import Processor

class AccountsProcessor(Processor):
    def __init__(self):
        self.cyphers = AccountsCyphers()
        self.split_name = re.compile("(?<=[a-z])(?=[A-Z])")
        super().__init__(bucket_name="audiences-processing")

    def get_current_wics(self):
        conditions = self.cyphers.get_wic_conditions()
        contexts = self.cyphers.get_wic_contexts()
        wics = conditions + contexts
        return wics

    def process_wic(self, wic):
        wic_name = wic.get("_displayName", "")
        params = {
            "audienceId": wic_name,
            "name": " ".join(self.split_name.split(wic_name)),
            "imageUrl": wic.get("_imageUrl", ""),
            "description": wic.get("_definition", "")
        }
        self.cyphers.create_audience(params)
        if "_Context" in wic.labels:
            self.cyphers.create_audience_by_context(wic_name)
        else:
            self.cyphers.create_audience_by_condition(wic_name)
            
    def process_audiences(self):
        wics = self.get_current_wics()
        self.cyphers.flag_existing_edges()
        for wic in wics:
            logging.info(f"Creating audience for WIC: {wic}")
            self.process_wic(wic)
        self.cyphers.clean_edges()

    def run(self):
        self.process_audiences()

if __name__ == "__main__":
    P = AccountsProcessor()
    P.run()