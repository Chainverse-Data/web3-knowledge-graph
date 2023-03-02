
from ...helpers import Cypher
from ...helpers import get_query_logging, count_query_logging

class GithubCypher(Cypher):
    def __init__(self) -> None:
        super().__init__()
    
    @get_query_logging
    def get_all_github_accounts(self):
        query = f"""
            MATCH (github:Github:Account)
            RETURN github.handle as handle
        """
        handles = self.query(query)
        return handles

    @count_query_logging
    def create_or_merge_users(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MERGE (user:Github:Account {{handle: toLower(data.login)}})
                ON CREATE SET   user.uuid = apoc.create.uuid(),
                                user.id = data.id,
                                user.avatar_url = data.avatar_url,
                                user.html_url = data.html_url,
                                user.name = data.name,
                                user.company = data.company,
                                user.blog = data.blog,
                                user.location = data.location,
                                user.email = data.email,
                                user.hireable = data.hireable,
                                user.bio = data.bio,
                                user.twitter_username = data.twitter_username,
                                user.public_repos = toInteger(data.public_repos),
                                user.public_gists = toInteger(data.public_gists),
                                user.followers = toInteger(data.followers),
                                user.following = toInteger(data.following),
                                user.created_at = datetime(data.created_at),
                                user.updated_at = datetime(data.updated_at),
                                user.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                user.ingestedBy = "{self.CREATED_ID}"
                ON MATCH SET    user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                user.avatar_url = data.avatar_url,
                                user.html_url = data.html_url,
                                user.name = data.name,
                                user.company = data.company,
                                user.blog = data.blog,
                                user.location = data.location,
                                user.email = data.email,
                                user.hireable = data.hireable,
                                user.bio = data.bio,
                                user.twitter_username = data.twitter_username,
                                user.public_repos = toInteger(data.public_repos),
                                user.public_gists = toInteger(data.public_gists),
                                user.followers = toInteger(data.followers),
                                user.following = toInteger(data.following),
                                user.updated_at = datetime(data.updated_at),
                                user.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(user)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_repositories(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MERGE (repo:Github:Repository {{full_name: data.full_name}})
                ON CREATE SET   repo.uuid = apoc.create.uuid(),
                                repo.id = data.id,
                                repo.name = data.name,
                                repo.full_name = data.full_name,
                                repo.private = data.private,
                                repo.owner = data.owner,
                                repo.html_url = data.html_url,
                                repo.description = data.description,
                                repo.fork = data.fork,
                                repo.created_at = datetime(data.created_at),
                                repo.updated_at = datetime(data.updated_at),
                                repo.pushed_at = datetime(data.pushed_at),
                                repo.homepage = data.homepage,
                                repo.size = data.size,
                                repo.stargazers_count = toInteger(data.stargazers_count),
                                repo.watchers_count = toInteger(data.watchers_count),
                                repo.language = data.language,
                                repo.has_issues = data.has_issues,
                                repo.has_projects = data.has_projects,
                                repo.has_downloads = data.has_downloads,
                                repo.has_wiki = data.has_wiki,
                                repo.has_pages = data.has_pages,
                                repo.has_discussions = data.has_discussions,
                                repo.forks_count = toInteger(data.forks_count),
                                repo.mirror_url = data.mirror_url,
                                repo.archived = data.archived,
                                repo.disabled = data.disabled,
                                repo.open_issues_count = toInteger(data.open_issues_count),
                                repo.license = data.license,
                                repo.allow_forking = data.allow_forking,
                                repo.is_template = data.is_template,
                                repo.web_commit_signoff_required = data.web_commit_signoff_required,
                                repo.topics = data.topics,
                                repo.visibility = data.visibility,
                                repo.forks = toInteger(data.forks),
                                repo.open_issues = toInteger(data.open_issues),
                                repo.watchers = toInteger(data.watchers),
                                repo.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                repo.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                repo.ingestedBy = "{self.CREATED_ID}"
                ON MATCH SET    repo.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                repo.name = data.name,
                                repo.full_name = data.full_name,
                                repo.private = data.private,
                                repo.owner = data.owner,
                                repo.html_url = data.html_url,
                                repo.description = data.description,
                                repo.fork = data.fork,
                                repo.created_at = datetime(data.created_at),
                                repo.updated_at = datetime(data.updated_at),
                                repo.pushed_at = datetime(data.pushed_at),
                                repo.homepage = data.homepage,
                                repo.size = data.size,
                                repo.stargazers_count = toInteger(data.stargazers_count),
                                repo.watchers_count = toInteger(data.watchers_count),
                                repo.language = data.language,
                                repo.has_issues = data.has_issues,
                                repo.has_projects = data.has_projects,
                                repo.has_downloads = data.has_downloads,
                                repo.has_wiki = data.has_wiki,
                                repo.has_pages = data.has_pages,
                                repo.has_discussions = data.has_discussions,
                                repo.forks_count = toInteger(data.forks_count),
                                repo.mirror_url = data.mirror_url,
                                repo.archived = data.archived,
                                repo.disabled = data.disabled,
                                repo.open_issues_count = toInteger(data.open_issues_count),
                                repo.license = data.license,
                                repo.allow_forking = data.allow_forking,
                                repo.is_template = data.is_template,
                                repo.web_commit_signoff_required = data.web_commit_signoff_required,
                                repo.topics = data.topics,
                                repo.visibility = data.visibility,
                                repo.forks = toInteger(data.forks),
                                repo.open_issues = toInteger(data.open_issues),
                                repo.watchers = toInteger(data.watchers),
                                repo.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(repo)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_followers(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (handle:Github:Account {{handle: toLower(data.handle)}})
                MATCH (follower:Github:Account {{handle: toLower(data.follower)}})
                MERGE (follower)-[edge:IS_FOLLOWING]->(handle)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_owners(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (handle:Github:Account {{handle: toLower(data.owner)}})
                MATCH (repo:Github:Repository {{full_name: data.full_name}})
                MERGE (handle)-[edge:IS_OWNER]->(repo)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_contributors(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (handle:Github:Account {{handle: toLower(data.contributor)}})
                MATCH (repo:Github:Repository {{full_name: data.full_name}})
                MERGE (handle)-[edge:IS_CONTRIBUTOR]->(repo)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_subscribers(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (handle:Github:Account {{handle: toLower(data.subscriber)}})
                MATCH (repo:Github:Repository {{full_name: data.full_name}})
                MERGE (handle)-[edge:IS_SUBSCRIBER]->(repo)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_email(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MERGE (email:Email:Account {{email: toLower(data.email)}})    
                ON CREATE SET   email.uuid = apoc.create.uuid(),
                                email.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                email.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                email.ingestedBy = "{self.CREATED_ID}"
                ON MATCH SET    email.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                email.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(email)
            """
            count += self.query(query)[0].value()
        return count


    @count_query_logging
    def link_email(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (user:Github:Account {{handle: toLower(data.handle)}})    
                MATCH (email:Email:Account {{email: toLower(data.email)}})
                MERGE (user)-[edge:HAS_ACCOUNT]->(email)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_twitter(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MERGE (twitter:Twitter:Account {{handle: toLower(data.twitter)}})
                ON CREATE SET twitter.uuid = apoc.create.uuid(),
                        twitter.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        twitter.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        twitter.ingestedBy = "{self.CREATED_ID}"
                ON MATCH SET twitter.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        twitter.ingestedBy = "{self.UPDATED_ID}"
                RETURN count(twitter)
            """
        count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_twitter(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS data
                MATCH (user:Github:Account {{handle: toLower(data.handle)}})    
                MATCH (twitter:Twitter:Account {{handle: toLower(data.twitter)}})
                MERGE (user)-[edge:HAS_ACCOUNT]->(twitter)
                RETURN count(edge)
            """
            count += self.query(query)[0].value()
        return count
