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
        self.negative_labels = ["_SuspiciousSnapshot", "_Mirror", "_NftWashTrading", "_SpamTokenDeployer"]

    def compute_score(self, data):
        degrees = [r["deg"] for r in data]
        logdegs = np.log(degrees)
        z = stats.zscore(logdegs)
        score = z - np.min(z)
        score = score / np.max(score)
        score *= 100
        return score

    def calculate_score(self, data):
        logging.info(f"Calculating score for {len(data)} wallets")
        scores = self.compute_score(data)
        results = {}
        for i in range(len(data)):
            results[data[i]["address"]] = scores[i]
        return results

    def run(self):
        logging.info(f"Calculating positive scores")
        positiveData = self.cyphers.get_positive_WIC_degrees(self.negative_labels)
        positiveResults = self.calculate_score(positiveData)
        logging.info(f"Calculating negative scores")
        negativeData = self.cyphers.get_negative_WIC_degrees(self.negative_labels)
        negativeResults = self.calculate_score(negativeData)
        logging.info(f"Saving the results ...")
        wallets = list(positiveResults.keys()) + list(negativeResults.keys())
        wallets = list(set(wallets))
        results = []
        for wallet in wallets:
            results.append({
                "address": wallet,
                "positiveReputationScore": positiveResults.get(wallet, None),
                "negativeReputationScore": negativeResults.get(wallet, None),
            })
        self.cyphers.save_reputation_score(results)
   
if __name__ == '__main__':
    analytics = WICScoreAnalysis()
    analytics.run()