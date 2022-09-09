from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import re
from newspaper import Article
import json


def requestData(startBlock, step):
    stopBlock = startBlock + step
    all_results = []
    transport = AIOHTTPTransport(url="https://arweave.net/graphql")
    client = Client(transport=transport, fetch_schema_from_transport=True)

    query_string = f"""
            {{
                transactions(first:100, block: {{min:{startBlock}, max:{stopBlock}}}, tags: {{name: "App-Name", values: "MirrorXYZ"}}) {{
                    edges {{
                    cursor
                        node {{
                            id
                            tags{{
                                name
                                value
                            }}
                        }}
                    }}
                }}
            }}
            """
    query = gql(query_string)
    try:
        results = client.execute(query)
        results = results["transactions"]["edges"]
        while len(results) == 100:
            all_results.extend(results)
            cursor = results[-1]["cursor"]
            new_query = query_string.replace("first:100", f'first:100, after:"{cursor}"')
            query = gql(new_query)
            results = client.execute(query)
            results = results["transactions"]["edges"]

        all_results.extend(results)
        print(f"{len(all_results)} transactions found")
    except:
        print("No transactions found")
        return []

    return all_results


def getArweaveTxs(startBlock, endBlock, initialStep):
    step = initialStep
    results = []

    currentBlock = startBlock
    while currentBlock < endBlock:
        print("Getting block {}".format(currentBlock))
        returned = requestData(currentBlock, step)
        if len(returned) == 100 and step != 1:
            step = step // 10
            currentBlock -= step
            print("Reducing step to {}".format(step))
        elif len(returned) < 10 and step < initialStep:
            step = step * 10
            currentBlock -= step
            print("Increasing step to {}".format(step))
        else:
            results += returned
        currentBlock += step

    return results


def get_urls(text):
    URL_REGEX = r"""((?:(?:https|ftp|http)?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|org|uk)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|uk|ac)\b/?(?!@)))"""
    urls = re.findall(URL_REGEX, text)
    urls = [url for url in urls if ".gif" not in url]
    urls = [url for url in urls if ".jpg" not in url]
    urls = [url for url in urls if ".png" not in url]
    urls = [url for url in urls if ".jpeg" not in url]
    urls = [url for url in urls if ".mp4" not in url]
    urls = [url for url in urls if ".mp3" not in url]
    return urls


def scrape_urls(urls):
    output_list = []
    for url in urls:
        try:
            article = Article(url)
            article.download()
            article.parse()
            if article.text != "" and article.text is not None:
                if article.publish_date is None:
                    publish_date = 0
                else:
                    publish_date = article.publish_date.timestamp()
                output_list.append(
                    {"text": article.text, "title": article.title, "publish_date": publish_date, "url": url}
                )
        except:
            pass
            # print("Error scraping url")

    # print(f"URLs: {len(urls)}, Scraped: {len(output_list)}")
    return output_list


def parse_items(entry):
    txHash = entry[1]
    body = json.loads(entry[0])["content"].get("body", "")
    urls = get_urls(body)
    scraped_urls = scrape_urls(urls)
    for idx in range(len(scraped_urls)):
        scraped_urls[idx]["transaction"] = txHash
    return scraped_urls
