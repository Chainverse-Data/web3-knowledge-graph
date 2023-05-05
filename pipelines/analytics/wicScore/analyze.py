import networkx as nx
import logging
import numpy as np
from scipy import stats
from tqdm import tqdm
from ..helpers import Analysis, Networks
from .cyphers import WICScoreAnalyticsCyphers



class WICScoreAnalysis(Analysis):
    def __init__(self):
        self.cyphers = WICScoreAnalyticsCyphers()
        super().__init__()

    def compute_score(self, data):
        weighted_degrees = [r["deg"] for r in data]
        scores = []
        for deg in weighted_degrees:
            if deg < 0:
                scores.append(deg/np.min(weighted_degrees))
            elif deg > 0:
                scores.append(deg/np.max(weighted_degrees))
            else:
                scores.append(deg)
        scores = [score * 100 for score in scores] 
        return scores

    def calculate_score(self, data):
        logging.info(f"Calculating score for {len(data)} wallets")
        scores = self.compute_score(data)
        results = {}
        for i in range(len(data)):
            results[data[i]["address"]] = scores[i]
        return results

    def run(self):
        logging.info(f"Calculating positive scores")
        data = self.cyphers.get_WIC_scores()
        scores = self.calculate_score(data)
        logging.info(f"Saving the results ...")
        results = []
        for wallet in scores.keys():
            results.append({
                "address": wallet,
                "reputationScore": scores.get(wallet, None),
            })
        self.cyphers.save_reputation_score(results)
   
if __name__ == '__main__':
    analytics = WICScoreAnalysis()
    analytics.run()