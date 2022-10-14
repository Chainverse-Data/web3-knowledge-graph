from datetime import datetime


def get_recent_twitter(cutoff: datetime, conn):

    month = cutoff.month
    day = cutoff.day
    year = cutoff.year

    twitter_node_query = f"""
                            MATCH (t:Twitter) WHERE t.createdDt >= datetime({{year: {year}, month: {month}, day: {day}}}) AND NOT t:Trash
                            return t.handle
                        """

    x = conn.query(twitter_node_query)
    x = [y.get("t.handle") for y in x]
    return x


def get_recent_empty_twitter(cutoff: datetime, conn):

    month = cutoff.month
    day = cutoff.day
    year = cutoff.year

    twitter_node_query = f"""
                            MATCH (t:Twitter) WHERE t.createdDt >= datetime({{year: {year}, month: {month}, day: {day}}}) AND NOT EXISTS(t.name) AND NOT t:Trash
                            return t.handle
                        """

    x = conn.query(twitter_node_query)
    x = [y.get("t.handle") for y in x]
    return x


def add_twitter_node_info(url, conn):

    twitter_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                            MATCH (t:Twitter {{handle: twitter.handle}})
                            SET t.name = twitter.name,
                                t.bio = twitter.bio,
                                t.followerCount = toInteger(twitter.followerCount),
                                t.verified = toBoolean(twitter.verified),
                                t.userId = twitter.userId,
                                t.website = twitter.website,
                                t.profileImageUrl = twitter.profileImageUrl,
                                t.lastUpdatedDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(t)"""

    x = conn.query(twitter_node_query)
    print("twitter nodes info added", x)


def add_trash_labels(url, conn):

    twitter_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                            MATCH (t:Twitter {{handle: twitter.handle}})
                            SET t:Trash
                            return count(t)"""

    x = conn.query(twitter_node_query)
    print("trash labels added", x)


def merge_twitter_ens_relationships(url, conn):

    twitter_ens_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS twitter
                            MATCH (t:Twitter {{handle: twitter.handle}})
                            MATCH (a:Alias {{name: toLower(twitter.ens)}})
                            MERGE (t)-[r:HAS_ALIAS]->(a)
                            return count(r)"""

    x = conn.query(twitter_ens_query)
    print("twitter ens relationships merged", x)
