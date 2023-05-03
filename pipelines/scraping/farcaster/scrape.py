from ..helpers import Scraper
from ...helpers import multiprocessing
import logging
from farcaster import Warpcast
import os


class FarcasterScraper(Scraper):
    def __init__(self, bucket_name="farcaster"):
        super().__init__(bucket_name)

        self.client = Warpcast(os.environ["WARPCAST_API_KEY"])
        self.multi = multiprocessing.Multiprocessing()

    def get_users(self):
        logging.info("Getting the users")
        users = []
        response = self.client.get_recent_users(limit=100)
        while True:
            for user in response.users:
                tmp = {
                    "fid": user.fid,
                    "fname": user.username,
                    "name": user.display_name,
                    "profileUrl": user.pfp.url if user.pfp else "",
                    "bio": user.profile.bio.text if user.profile.bio else "",
                    "url": f"https://warpcast.com/{user.username}",
                }
                try:
                    tmp["address"] = self.client.get_custody_address(fid=user.fid).custody_address
                except:
                    continue
                users.append(tmp)
            if not response.cursor:
                break
            if len(users) % 1000 == 0:
                logging.info(f"Current: {len(users)} users")
            response = self.client.get_recent_users(cursor=response.cursor, limit=100)

        self.data["users"] = users
        logging.info(f"Found {len(users)} users")

    def get_followers(self):
        logging.info("Getting the followers")

        def get_individual(entry):
            client = Warpcast(os.environ["WARPCAST_API_KEY"])
            followers = []
            response = client.get_followers(fid=entry["fid"], limit=100)
            while True:
                for follower in response.users:
                    tmp = {
                        "fid": entry["fid"],
                        "follower": follower.fid,
                    }
                    followers.append(tmp)
                if not response.cursor:
                    break
                response = client.get_followers(fid=entry["fid"], cursor=response.cursor, limit=100)
            return followers

        followers = self.multi.parallel_process(get_individual, self.data["users"], "Multiprocessing followers")
        followers = [follower for sublist in followers for follower in sublist]

        self.data["followers"] = followers
        logging.info(f"Found {len(followers)} followers")

    def run(self):
        self.get_users()
        self.get_followers()
        self.save_data()
        self.save_metadata()


if __name__ == "__main__":
    scraper = FarcasterScraper()
    scraper.run()
    logging.info("Run complete!")
