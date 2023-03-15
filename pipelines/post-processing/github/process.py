import os
from tqdm import tqdm
from .cyphers import GithubCypher
from ..helpers import Processor
import numpy as np

DEBUG = os.environ.get("DEBUG", False)

class GithubProcessor(Processor):
    def __init__(self):
        self.github_tokens = [key.strip() for key in os.environ.get("GITHUB_API_KEY", "").split(",")]
        self.cyphers = GithubCypher()
        
        self.user_keys = ["id", "login", "avatar_url", "html_url", "name", "company", "blog", "location", "email", "hireable", "bio", "twitter_username", "public_repos", "public_gists", "followers", "following", "created_at", "updated_at"]
        self.contributor_keys = ["login", "contributions"]
        self.repository_keys = ["id","name","full_name","private","owner","html_url","description","fork","created_at","updated_at","pushed_at","homepage","size","stargazers_count","watchers_count","language","has_issues","has_projects","has_downloads","has_wiki","has_pages","has_discussions","forks_count","mirror_url","archived","disabled","open_issues_count","license","allow_forking","is_template","web_commit_signoff_required","topics","visibility","forks","open_issues","watchers"]
        
        self.chunk_size = 10
        super().__init__("github-processing")

    def init_data(self):
        self.data = {}
        self.data["users"] = {}
        self.data["repositories"] = {}

    def get_headers(self):
        token = np.random.choice(self.github_tokens)
        github_headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        return github_headers

    def get_all_handles(self):
        handles = self.cyphers.get_all_github_accounts()
        handles = [el["handle"] for el in handles]
        if DEBUG:
            handles = handles[:10]
        return handles
    
    ##########################################
    #                  USERS                 #
    ##########################################

    def get_followers(self, handle):
        url = f"https://api.github.com/users/{handle}/followers"
        followers_raw_data = self.get_request(url, headers=self.get_headers(), decode=False, json=True, retry_on_404=False) 
        followers_data = []
        if followers_raw_data:
            follower_pbar = tqdm(followers_raw_data, desc="Getting followers information", position=2, leave=False)
            for follower in follower_pbar:
                if "login" in follower:
                    follower_pbar.set_description("Getting followers information: " +  follower["login"])
                    self.get_user_data(follower["login"], follow_through=False)
                    followers_data.append(follower["login"])
        return followers_data

    def get_user_data(self, handle, follow_through=True):
        if handle in self.data["users"]:
            return self.data["users"][handle]
        url = f"https://api.github.com/users/{handle}"
        user_raw_data = self.get_request(url, headers=self.get_headers(), decode=False, json=True, retry_on_404=False)
        if user_raw_data:
            user_data = {key: value for (key, value) in user_raw_data.items() if key in self.user_keys}
            if follow_through:
                followers = self.get_followers(handle)
                user_data["followers_handles"] = followers
                repositories = self.get_user_repos(handle)
                user_data["repositories"] = repositories
            self.data["users"][handle] = user_data

    def get_user_repos(self, handle):
        url = f"https://api.github.com/users/{handle}/repos"
        repos_raw_data = self.get_request(url, headers=self.get_headers(), decode=False, json=True, retry_on_404=False)
        repositories = []
        if repos_raw_data:
            repo_pbar = tqdm(repos_raw_data, position=1, desc="Getting repository information", leave=False)
            for repo in repo_pbar:
                if "full_name" in repo:
                    repo_pbar.set_description(desc="Getting repository information: " + repo["full_name"])
                    self.get_repository_data(repo["full_name"])
            repositories = [repo["full_name"] for repo in repos_raw_data]
        return repositories

    ##########################################
    #                  REPOS                 #
    ##########################################

    def get_contributors(self, repository):
        url = f"https://api.github.com/repos/{repository}/contributors"
        contributors_raw_data = self.get_request(url, headers=self.get_headers(), decode=False, json=True, retry_on_404=False)
        contributors_data = []
        if contributors_raw_data:
            cont_pbar = tqdm(contributors_raw_data, desc="Getting contributors information", position=2, leave=False)
            for contributor in cont_pbar:
                if "login" in contributor:
                    cont_pbar.set_description(desc="Getting contributors information: " + contributor["login"])
                    self.get_user_data(contributor["login"], follow_through=False)
                    contributor = {key: value for (key, value) in contributor.items() if key in self.contributor_keys}
                    contributors_data.append(contributor["login"])
        return contributors_data

    def get_subscribers(self, repository):
        url = f"https://api.github.com/repos/{repository}/subscribers"
        subscribers_raw_data = self.get_request(url, headers=self.get_headers(), decode=False, json=True, retry_on_404=False)
        subscribers_data = []
        if subscribers_raw_data:
            sub_pbar = tqdm(subscribers_raw_data, desc="Getting subscribers information", position=2, leave=False)
            for subscriber in sub_pbar:
                if "login" in subscriber:
                    sub_pbar.set_description(desc="Getting subscribers information: " + subscriber["login"])
                    self.get_user_data(subscriber["login"], follow_through=False)
                    subscribers_data.append(subscriber["login"])
        return subscribers_data

    def get_repo_readme(self, repository):
        url = f"https://api.github.com/repos/{repository}/readme"
        readme_data = self.get_request(url, headers=self.get_headers(), decode=False, json=True, retry_on_404=False)
        if readme_data:
            readme_file = self.get_request(readme_data["download_url"], decode=True, retry_on_404=False)
            if readme_file:
                return readme_file
            else:
                return ""
        else:
            return ""

    def get_repository_data(self, repository):
        if repository in self.data["repositories"]:
            return self.data["repositories"][repository]
        url = f"https://api.github.com/repos/{repository}"
        repos_raw_data = self.get_request(url, headers=self.get_headers(), decode=False, json=True, retry_on_404=False)
        if repos_raw_data and type(repos_raw_data) == dict:
            repos_raw_data["languages"] = self.get_repository_languages(repository)
            if "owner" in repos_raw_data:
                if repos_raw_data["owner"]:
                    repos_raw_data["owner"] = repos_raw_data["owner"].get("login", None)
                else:
                    repos_raw_data["owner"] = None
            else:
                repos_raw_data["owner"] = None
            if "license" in repos_raw_data:
                if repos_raw_data["license"]:
                    repos_raw_data["license"] = repos_raw_data["license"].get("name", None)
                else:
                    repos_raw_data["license"] = None
            else:
                repos_raw_data["license"] = None
            if "parent" in repos_raw_data:
                self.get_repository_data(repos_raw_data["parent"]["full_name"])
                repos_raw_data["parent"] = repos_raw_data["parent"]["full_name"]
            repos_data = {key: value for (key, value) in repos_raw_data.items() if key in self.repository_keys}
            contributors = self.get_contributors(repository)
            subscribers = self.get_subscribers(repository)
            readme = self.get_repo_readme(repository)
            repos_data["contributors_handles"] = contributors
            repos_data["subscribers_handles"] = subscribers 
            repos_data["readme"] = readme
            self.data["repositories"][repository] = repos_data
    
    def get_repository_languages(self, repository):
        url = f"https://api.github.com/repos/{repository}/languages"
        languages = self.get_request(url, headers=self.get_headers(), decode=False, json=True, retry_on_404=False)
        if languages:
            languages = [key for key in languages]
        else:
            languages = []
        return languages

    def process_github_accounts(self, handles):
        self.parallel_process(self.get_user_data, handles, "Getting users accounts information")

    def ingest_github_data(self):

        users = [self.data["users"][handle] for handle in self.data["users"]]
        users_urls = self.save_json_as_csv(users, self.bucket_name, f"processor_users_{self.asOf}")

        emails = [{"handle": user["login"], "email":user["email"]} for user in users if user["email"]]
        emails_urls = self.save_json_as_csv(emails, self.bucket_name, f"processor_emails_{self.asOf}")

        twitters = [{"handle": user["login"], "twitter":user["twitter_username"]} for user in users if user["twitter_username"]]
        twitters_urls = self.save_json_as_csv(twitters, self.bucket_name, f"processor_twitters_{self.asOf}")


        repositories = [self.data["repositories"][handle] for handle in self.data["repositories"]]
        repositories_urls = self.save_json_as_csv(repositories, self.bucket_name, f"processor_repositories_{self.asOf}")

        followers = []
        for user in self.data["users"]:
            if "followers_handles" in self.data["users"][user]:
                for follower in self.data["users"][user]["followers_handles"]:
                    followers.append({
                        "handle": user,
                        "follower": follower
                    })
        followers_urls = self.save_json_as_csv(followers, self.bucket_name, f"processor_followers_{self.asOf}")
        
        contributors = []
        for repo in self.data["repositories"]:
            if "contributors_handles" in self.data["repositories"][repo]:
                for contributor in self.data["repositories"][repo]["contributors_handles"]:
                    contributors.append({
                        "full_name": repo,
                        "contributor": contributor
                    })
        contributors_urls = self.save_json_as_csv(contributors, self.bucket_name, f"processor_contributors_{self.asOf}")

        subscribers = []
        for repo in self.data["repositories"]:
            if "subscribers_handles" in self.data["repositories"][repo]:
                for subscriber in self.data["repositories"][repo]["subscribers_handles"]:
                    subscribers.append({
                        "full_name": repo,
                        "subscriber": subscriber
                    })
        subscribers_urls = self.save_json_as_csv(subscribers, self.bucket_name, f"processor_subscribers_{self.asOf}")
        
        self.cyphers.create_or_merge_users(users_urls)
        self.cyphers.create_or_merge_repositories(repositories_urls)
        self.cyphers.link_followers(followers_urls)
        self.cyphers.link_owners(repositories_urls)
        self.cyphers.link_contributors(contributors_urls)
        self.cyphers.link_subscribers(subscribers_urls)
        self.cyphers.create_or_merge_email(emails_urls)
        self.cyphers.link_email(emails_urls)
        self.cyphers.create_or_merge_twitter(twitters_urls)
        self.cyphers.link_twitter(twitters_urls)

    def run(self):
        handles = self.get_all_handles()
        for i in range(0, len(handles), self.chunk_size):
            self.init_data()
            self.process_github_accounts(handles[i: i+self.chunk_size])
            self.ingest_github_data()

if __name__ == '__main__':
    P = GithubProcessor()
    P.run()
