from .. import WICAnalysis
from .cyphers import DiversityCyphers

class DiversityAnalysis(WICAnalysis):
    def __init__(self):
        self.subgraph_name = 'Dei'
        self.conditions = {
            "Voting": {
                "GenderFocusedProposal": self.process_gendered_focused_proposals, 
                "UrmFocusedProposal": self.process_urm_focuses_proposals
            },
            "Writing": {
                "GenderFocusedArticle": self.process_gendered_focused_articles,
                "UrmFocusedArticle": self.process_urm_focused_articles, 
                "UrmFocusedBio": self.process_urm_focused_bio, 
                "GenderFocusedBio":self.process_gendered_focused_bio
            },
            "FinancialSupporter": {
                "GenderFocusedGrant": self.process_gendered_focused_grant, 
                "UrmFocusedGrant": self.process_urm_focused_grant
            }
        }
        self.cyphers = DiversityCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-diversity")

        self.gender_keywords = ["gender equality", "sexism", "lgbtqia", "women in web3"]
        self.urm_keywords = ["under represented minorities", "racial justice", "DEI and black OR latino OR asian", "diversity equity and inclusion", "african", "minorities", "empowering black", "empowering latino", "spanish speaking"]

    def process_gendered_focused_proposals(self, context):
        self.cyphers.connect_proposals(context, self.gender_keywords)
        self.cyphers.connect_authors_to_proposals(context)
        self.cyphers.connect_voters_to_proposals(context)
        self.cyphers.connect_proposals_to_entities(context)

    def process_urm_focuses_proposals(self, context):
        self.cyphers.connect_proposals(context, self.urm_keywords)
        self.cyphers.connect_authors_to_proposals(context)
        self.cyphers.connect_voters_to_proposals(context)
        self.cyphers.connect_proposals_to_entities(context)

    def process_gendered_focused_articles(self, context):
        self.cyphers.connect_articles(context, self.gender_keywords)
        self.cyphers.connect_authors_to_articles(context)
        self.cyphers.connect_collectors_to_articles(context)

    def process_urm_focused_articles(self, context):
        self.cyphers.connect_articles(context, self.urm_keywords)
        self.cyphers.connect_authors_to_articles(context)
        self.cyphers.connect_collectors_to_articles(context)

    def process_urm_focused_bio(self, context):
        self.cyphers.connect_bio(context, self.urm_keywords)

    def process_gendered_focused_bio(self, context):
        self.cyphers.connect_bio(context, self.gender_keywords)

    def process_gendered_focused_grant(self, context):
        self.cyphers.connect_grants(context, self.gender_keywords)
        self.cyphers.connect_donors_to_grants(context)
        self.cyphers.connect_admins_to_grants(context)

    def process_urm_focused_grant(self, context):
        self.cyphers.connect_grants(context, self.urm_keywords)
        self.cyphers.connect_donors_to_grants(context)
        self.cyphers.connect_admins_to_grants(context)

    def run(self):
        self.process_conditions()

if __name__=="__main__":
    analysis = DiversityAnalysis()
    analysis.run()
