import scraping.helpers.Scraper

class GitCoinScraper(Scraper):
    def __init__(self):
        super.__init__(self, "gitcoin_bucket")
    
    def run(self):
        print(self.bucket)
        print("Well that seems to be working!")

if __name__ == "__main__":
    scraper = GitCoinScraper()
    scraper.run()