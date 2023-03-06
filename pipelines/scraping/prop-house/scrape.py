from ..helpers import Scraper
import logging
import numpy as np
import tqdm

class PropHouseScraper(Scraper):
    def __init__(self, allow_override=True):
        self.bucket_name="prop-house"

        super().__init__(self.bucket_name, allow_override=allow_override)

        self.graph_url = "https://prod.backend.prop.house/graphql"
        self.headers = {"accept": "application/json", "content-type": "application/json"}

        self.interval = 100
        self.data["communities"] = []
        self.data["auctions"] = []
        self.data["proposals"] = []
        self.data["votes"] = []

    def get_communities(self):
        logging.info(f"Getting communities from {self.graph_url}...")

        comm_query = """
                query allCommunities {
                    communities {
                        id
                        contractAddress
                        name
                        profileImageUrl
                        description
                        auctions {
                            title
                            id
                        }
                        createdDate
                    }
                }
            """
        result = self.call_the_graph_api(self.graph_url, comm_query, None, [counter=])
        self.data["communities"] = result["communities"]
        logging.info(f"Found {len(self.data['communities'])} communities")

    def get_auctions_and_proposals(self):
        logging.info(f"Getting auctions and proposals from {self.graph_url}...")

        for community in tqdm.tqdm(self.data["communities"], desc="Communities"):
            for auction in tqdm.tqdm(community["auctions"], desc="Auctions", leave=False):
                auction_id = auction["id"]
                auction_query = """
                        query getAuction ($id: Int!) {
                            auction (id: $id) {
                            id
                            title
                            startTime
                            proposalEndTime
                            fundingAmount
                            currencyType
                            description
                            numWinners
                            community {
                                id
                            }
                            proposals {
                                address
                                id
                                title
                                tldr
                                createdDate
                                votes {
                                    address
                                    id
                                    direction
                                    weight
                                    createdDate
                                }
                            }
                            createdDate
                            }
                        }
                    """
                auction_variables = {"id": auction_id}
                result = self.call_the_graph_api(self.graph_url, auction_query, auction_variables, ["auction"])
                community_id = result["auction"]["community"]["id"]
                proposals = result["auction"].pop("proposals")
                num_winners = result["auction"]["numWinners"]
                self.data["auctions"].append(result["auction"])
                vote_counts = []
                for i in range(len(proposals)):
                    votes = proposals[i].pop("votes")
                    vote_count = 0
                    for j in range(len(votes)):
                        vote_count += votes[j]["weight"]
                        votes[j]["proposalId"] = proposals[i]["id"]
                        votes[j]["communityId"] = community_id
                        votes[j]["auctionId"] = auction_id
                        self.data["votes"].append(votes[j])
                    vote_counts.append(vote_count)

                indices = np.argsort(vote_counts)[::-1][:num_winners]
                for i in range(len(proposals)):
                    if i in indices:
                        proposals[i]["winner"] = True
                    else:
                        proposals[i]["winner"] = False
                    proposals[i]["communityId"] = community_id
                    proposals[i]["auctionId"] = auction_id
                    self.data["proposals"].append(proposals[i])

    def run(self):
        self.get_communities()
        self.get_auctions_and_proposals()
        self.save_metadata()
        self.save_data()


if __name__ == "__main__":
    scraper = PropHouseScraper()
    scraper.run()
    logging.info("Run complete!")
