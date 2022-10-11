spaces_query = """
        {
            spaces(
                first: 100,
                skip: 0,
                orderBy: "created",
                orderDirection: asc
            ) {
            id
                name
                about
                avatar
                terms
                location
                website
                twitter
                github
                network
                symbol
                strategies {
                name
                params
                }
                admins
                members
                filters {
                minScore
                onlyMembers
                }
                plugins
            }
        }
        """

proposals_query = """
        {
            proposals(
                first: 500,
                skip: 0,
                orderBy: "created",
                orderDirection: desc
            ) {
                id
                ipfs
                author
                created
                space {
                id
                name
                }
                network
                type
                strategies {
                name
                params
                }      
                plugins
                title
                body
                choices
                start
                end
                snapshot
                state
                link
            }
        }
        """

votes_query = """
        {
            votes (
                first: 1000
                skip: 0
                orderBy: "created",
                orderDirection: desc,
                where: {
                proposal_in: $proposalIDs
                }
            ) {
                id
                ipfs
                voter
                created
                choice
                proposal {
                id
                choices
                }
                space {
                id
                name
                }
            }
        }
        """
