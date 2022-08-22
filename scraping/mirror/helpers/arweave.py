from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

def requestData(startBlock, step):
    stopBlock = startBlock + step
    results = []
    transport = AIOHTTPTransport(url="https://arweave.net/graphql")
    client = Client(transport=transport, fetch_schema_from_transport=True)

    query = gql(
        f"""
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
    )

    try:
        results = client.execute(query)
        results = results["transactions"]["edges"]
        print(f"{len(results)} transactions found")
    except:
        print("No transactions found")
        return []

    return results


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
            print("Increasing step to {}".format(step))
        else:
            results += returned
        currentBlock += step

    return results