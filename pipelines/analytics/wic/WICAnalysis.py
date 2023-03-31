import logging

from tqdm import tqdm
from ..helpers import Analysis

TYPES = {
    "experience": "Experience",
    "interests": "Interest",
    "influences": "Influence",
}

class WICAnalysis(Analysis):
    def __init__(self, bucket_name) -> None:
        try:
            assert len(self.conditions) > 0, "No conditions found!"
        except:
            raise ValueError("Conditions must be declared before instancing with super().init")

        Analysis.__init__(self, bucket_name)

    def process_conditions(self):
        for condition in tqdm(self.conditions, position=0):
            logging.info(f"Processing Condition: {condition}")
            for context in tqdm(self.conditions[condition], position=1):
                logging.info(f"Processing Context: {context}")
                if "subcontexts" in self.conditions[condition][context]:
                    self.conditions[condition][context]["call"](context)
                    for subcontext in tqdm(self.conditions[condition][context]["subcontexts"], position=2):
                        logging.info(f"Processing Subcontext: {subcontext}")
                        self.conditions[condition][context]["subcontexts"][subcontext]["call"](context, subcontext)
                else:
                    self.conditions[condition][context]["call"](context)