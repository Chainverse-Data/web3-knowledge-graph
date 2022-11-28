# GitCoin Scraper

This scraper reads from the GitCoin API to retrieve the grants and bounties. It also reads onChain events for the BulkCheckout contract of GitCoin to recoved the donations event.

# Bucket

This scraper reads and writes from the following bucket: `gitcoin`

# Metadata

The scraper reads and writes the following metadata variables:
- last_grant_offset: Sets the offset for reading only new grants
- last_bounty_offset: Sets the offset for reading only new bounties
- last_block_number: : Sets the offset for reading only new blocks

# Flags

No unique flags created

# Data

```json
{
    "grants": [<grant Object>],
    "bounties": [<bounty Object>],
    "donations": [<donation Object>],
}
```

## grant Object example
```json
{
    "id": 7214,
    "active": true,
    "logo_url": "https://c.gitcoin.co/grants/9305dbac14d826e7ed327104e6005cb5/signal-2022-09-01-204201_002.png",
    "details_url": "/grants/7214/project-lion",
    "title": "Project LION",
    "description": "Project LION is a joint DAO effort from talentDAO and LabDAO to build natural language processing systems for analyzing and maintaining community health.\r\n\u00a0\r\nBy leveraging transformers \u2013 the same neural network architecture used to train OpenAI\u2019s DALL-E 2 & GPT-3 \u2013 LION will build on classical approaches to language and communication analysis through the lens of a single, open source system.\r\n\r\n\u00a0\r\nWhat is LION?\r\nL.I.O.N is an acronym for linguistic informatics for organizational networks.\r\n\u00a0\r\nThe LION project intends to develop an open source bridge for communication data processing [starting with Discord] and a pipeline for model training and deployment.\r\n\u00a0\r\nContributor activity will be analyzed ethically and privacy-preserving. Aggregate analytics will be measured against established metrics of well-being, such as likert-scale questionnaires. The core objective of LION is the hosting of a production-ready service developed with privacy-preserving best practices and ultimately, the publishing of a research article about findings.\r\n\u00a0\r\nIt is built on the concepts of Organizational Network Analysis [ONA] and Natural Language Processing [NLP] as approaches to modeling organizational communication networks. Like social network analysis, ONA produces a graph that visualizes and measures the strength of relationships between nodes. It has been used to model the interactions between an organization\u2019s members and predict burnout from measures like collaborative overload. Together with NLP tools like spaCy and empath, they have uncovered psychometrics like engagement, turnover intent, and cultural fit.\r\n\u00a0\r\nWe expect LION will improve our understanding of communication and coordination within digitally native organizations.\r\n\u00a0\r\nGrant funds will be used to cover stipends for open-source contributors to the project.\r\n\u00a0\r\n\u00a0\r\nProject team\r\nStanley Bishop - AI architect, LabDAO\r\nKenneth Francis - Psychometrics & Data Science Lead, talentDAO\r\n+ support from the LabDAO Data Science Lab\r\n",
    "description_rich": "{\"ops\":[{\"insert\":\"Project LION is a joint DAO effort from\"},{\"attributes\":{\"color\":\"#000000\",\"link\":\"https://twitter.com/talentDAO_\"},\"insert\":\" \"},{\"attributes\":{\"color\":\"#1155cc\",\"link\":\"https://twitter.com/talentDAO_\"},\"insert\":\"talentDAO\"},{\"insert\":\" and\"},{\"attributes\":{\"color\":\"#000000\",\"link\":\"https://twitter.com/lab_dao\"},\"insert\":\" \"},{\"attributes\":{\"color\":\"#1155cc\",\"link\":\"https://twitter.com/lab_dao\"},\"insert\":\"LabDAO\"},{\"insert\":\" to build natural language processing systems for analyzing and maintaining community health.\\n\"},{\"attributes\":{\"color\":\"#757087\"},\"insert\":\"\u00a0\"},{\"insert\":\"\\nBy leveraging transformers \u2013 the same neural network architecture used to train OpenAI\u2019s DALL-E 2 & GPT-3 \u2013 LION will build on classical approaches to language and communication analysis through the lens of a single, open source system.\\n\\n\"},{\"attributes\":{\"color\":\"#757087\"},\"insert\":\"\u00a0\"},{\"insert\":\"\\n\"},{\"attributes\":{\"color\":\"#434343\"},\"insert\":\"What is LION?\"},{\"attributes\":{\"header\":3},\"insert\":\"\\n\"},{\"insert\":\"L.I.O.N is an acronym for linguistic informatics for organizational networks.\\n\"},{\"attributes\":{\"color\":\"#757087\"},\"insert\":\"\u00a0\"},{\"insert\":\"\\nThe LION project intends to develop an open source bridge for communication data processing [starting with Discord] and a pipeline for model training and deployment.\\n\u00a0\\nContributor activity will be analyzed ethically and privacy-preserving. Aggregate analytics will be measured against established metrics of well-being, such as likert-scale questionnaires. The core objective of LION is the hosting of a production-ready service developed with privacy-preserving best practices and ultimately, the publishing of a research article about findings.\\n\"},{\"attributes\":{\"color\":\"#757087\"},\"insert\":\"\u00a0\"},{\"insert\":\"\\nIt is built on the concepts of Organizational Network Analysis [ONA] and Natural Language Processing [NLP] as approaches to modeling organizational communication networks. Like social network analysis, ONA produces a graph that visualizes and measures the strength of relationships between nodes. It has been used to\"},{\"attributes\":{\"color\":\"#000000\",\"link\":\"https://journals.sagepub.com/doi/10.1177/0003122416671873\"},\"insert\":\" model the interactions between an organization\u2019s members\"},{\"insert\":\" and predict burnout from measures like\"},{\"attributes\":{\"color\":\"#000000\",\"link\":\"https://www.robcross.org/wp-content/uploads/2021/04/HBR-Collaborative-Overload.pdf\"},\"insert\":\" collaborative overload\"},{\"insert\":\". Together with NLP tools like spaCy and\"},{\"attributes\":{\"color\":\"#000000\",\"link\":\"https://arxiv.org/abs/1602.06979\"},\"insert\":\" empath\"},{\"insert\":\", they have uncovered psychometrics like engagement, turnover intent, and cultural fit.\\n\"},{\"attributes\":{\"color\":\"#757087\"},\"insert\":\"\u00a0\"},{\"insert\":\"\\nWe expect LION will improve our understanding of communication and coordination within digitally native organizations.\\n\"},{\"attributes\":{\"color\":\"#757087\"},\"insert\":\"\u00a0\"},{\"insert\":\"\\nGrant funds will be used to cover stipends for open-source contributors to the project.\\n\"},{\"attributes\":{\"color\":\"#757087\",\"italic\":true},\"insert\":\"\u00a0\"},{\"insert\":\"\\n\"},{\"attributes\":{\"color\":\"#757087\",\"italic\":true},\"insert\":\"\u00a0\"},{\"insert\":\"\\n\"},{\"attributes\":{\"color\":\"#666666\"},\"insert\":\"Project team\"},{\"attributes\":{\"header\":4},\"insert\":\"\\n\"},{\"attributes\":{\"italic\":true,\"color\":\"#000000\",\"link\":\"https://twitter.com/ScienceStanley\"},\"insert\":\"Stanley Bishop\"},{\"attributes\":{\"italic\":true},\"insert\":\" - AI architect, LabDAO\"},{\"insert\":\"\\n\"},{\"attributes\":{\"italic\":true,\"color\":\"#000000\",\"link\":\"https://twitter.com/k3nnethfrancis\"},\"insert\":\"Kenneth Francis\"},{\"attributes\":{\"italic\":true},\"insert\":\" - Psychometrics & Data Science Lead, talentDAO\"},{\"insert\":\"\\n\"},{\"attributes\":{\"italic\":true},\"insert\":\"+ support from the LabDAO Data Science Lab\"},{\"insert\":\"\\n\"}]}",
    "last_update": "2022-09-07T12:00:49.481Z",
    "last_update_natural": "4\u00a0weeks ago",
    "sybil_score": "-2.5385",
    "weighted_risk_score": "0.0000",
    "is_clr_active": false,
    "clr_round_num": "",
    "is_contract_address": false,
    "admin_profile": {
        "url": "https://gitcoin.co/k3nnethfrancis",
        "handle": "k3nnethfrancis",
        "avatar_url": "https://gitcoin.co/dynamic/avatar/k3nnethfrancis"
    },
    "favorite": false,
    "team_members": [
        {
            "model": "dashboard.profile",
            "pk": 114989,
            "fields": {
                "handle": "niklastr"
            }
        },
        ...
        {
            "model": "dashboard.profile",
            "pk": 436494,
            "fields": {
                "handle": "sciencestanley"
            }
        }
    ],
    "is_on_team": false,
    "clr_prediction_curve": [
        [
            0.0,
            0.0,
            0.0
        ],
        ...
        [
            0.0,
            0.0,
            0.0
        ]
    ],
    "last_clr_calc_date": "5\u00a0days, 19\u00a0hours ago",
    "safe_next_clr_calc_date": "4\u00a0minutes from now",
    "amount_received_in_round": "662.7553",
    "amount_received": "662.7553",
    "positive_round_contributor_count": 52,
    "monthly_amount_subscribed": "0.0000",
    "is_clr_eligible": true,
    "slug": "project-lion",
    "url": "/grants/7214/project-lion",
    "contract_version": "2",
    "contract_address": "0x0",
    "token_symbol": "Any Token",
    "admin_address": "0x9F4BC8b266ec6832D1339B62a6f04F35fCdDd846",
    "zcash_payout_address": "0x0",
    "celo_payout_address": "0x0",
    "zil_payout_address": "0x0",
    "polkadot_payout_address": "0x0",
    "kusama_payout_address": "0x0",
    "harmony_payout_address": "0x0",
    "binance_payout_address": "0x0",
    "rsk_payout_address": "0x0",
    "algorand_payout_address": "0x0",
    "cosmos_payout_address": "0x0",
    "token_address": "0x0",
    "image_css": "",
    "verified": true,
    "tenants": [
        "ETH"
    ],
    "metadata": {
        "cv": 0,
        "gem": -112298948,
        "related": [
            [
                12,
                1187
            ],
            ...
            [
                7452,
                401
            ]
        ],
        "upcoming": -100014000,
        "wall_of_love": [],
        "last_calc_time_related": 1664830830.3899302,
        "last_calc_time_contributor_counts": 1664977306.6242256,
        "last_calc_time_sybil_and_contrib_amounts": 1664977307.7005854
    },
    "grant_type": [
        {
            "model": "grants.granttype",
            "pk": 17,
            "fields": {
                "name": "gr12",
                "label": "Grants Round 12"
            }
        }
    ],
    "twitter_handle_1": "projectlion_ai",
    "twitter_handle_2": "k3nnethfrancis",
    "reference_url": "https://project-lion-ai.github.io/hello-world/",
    "github_project_url": "https://github.com/Project-LION-AI",
    "funding_info": null,
    "admin_message": "",
    "link_to_new_grant": null,
    "region": {
        "name": "north_america",
        "label": "North America"
    },
    "has_external_funding": "no",
    "active_round_names": [],
    "is_idle": false,
    "is_hidden": false,
    "has_pending_claim": false,
    "has_claims_in_review": false,
    "has_claim_history": false,
    "grant_tags": [
        {
            "model": "grants.granttag",
            "pk": 61,
            "fields": {
                "name": "*DeSci",
                "is_eligibility_tag": true
            }
        }
    ]
}


```

## bounty Object example
```json
{
    "url": "https://gitcoin.co/issue/2918",
    "created_on": "2018-04-30T15:04:55Z",
    "modified_on": "2022-10-05T02:18:18.541386Z",
    "title": "Notify Gitcoin/Funder that Developer is Starting Work",
    "web3_created": "2018-03-22T03:10:08Z",
    "value_in_token": "170000000000000000.00",
    "token_name": "ETH",
    "token_address": "0x0000000000000000000000000000000000000000",
    "bounty_type": "Feature",
    "bounty_categories": [],
    "project_length": "Hours",
    "experience_level": "Intermediate",
    "github_url": "https://github.com/gitcoinco/web/issues/683",
    "github_comments": 29,
    "bounty_owner_address": "0x4331b095bc38dc3bce0a269682b5ebaefa252929",
    "bounty_owner_email": "Anonymous",
    "bounty_owner_github_username": "",
    "bounty_owner_name": "Anonymous",
    "fulfillments": [],
    "interested": [
        {
            "pk": 619,
            "profile": {
                "id": 3205,
                "url": "/darkdarkdragon",
                "name": "Ivan Tivonenko",
                "handle": "darkdarkdragon",
                "keywords": [
                    "Java",
                    "Haxe",
                    "Elixir",
                    "JavaScript",
                    "Go",
                    "C++",
                    "Nemerle",
                    "Objective-C",
                    "HTML",
                    "CSS",
                    "Lua",
                    "Shell"
                ],
                "position": 0,
                "avatar_url": "https://c.gitcoin.co/avatars/f775a3147221545afd6bc41536cfd3b2/darkdarkdragon.png",
                "github_url": "https://github.com/darkdarkdragon",
                "total_earned": 1.2546094569297013,
                "organizations": {
                    "livepeer": 2,
                    "gitcoinco": 6
                }
            },
            "created": "2018-04-21T16:19:21.429259Z",
            "pending": false,
            "issue_message": ""
        }
    ],
    "is_open": false,
    "expires_date": "2018-04-21T03:10:08Z",
    "activities": [
        {
            "activity_type": "new_tip",
            "pk": 54575,
            "created": "2019-09-23T19:02:37.640389Z",
            "profile": {
                "id": 3205,
                "url": "/darkdarkdragon",
                "name": "Ivan Tivonenko",
                "handle": "darkdarkdragon",
                "keywords": [
                    "Java",
                    "Haxe",
                    "Elixir",
                    "JavaScript",
                    "Go",
                    "C++",
                    "Nemerle",
                    "Objective-C",
                    "HTML",
                    "CSS",
                    "Lua",
                    "Shell"
                ],
                "position": 0,
                "avatar_url": "https://c.gitcoin.co/avatars/f775a3147221545afd6bc41536cfd3b2/darkdarkdragon.png",
                "github_url": "https://github.com/darkdarkdragon",
                "total_earned": 1.2546094569297013,
                "organizations": {
                    "livepeer": 2,
                    "gitcoinco": 6
                }
            },
            "metadata": {
                "amount": "0.1700",
                "from_name": "Kevin",
                "github_url": "https://github.com/gitcoinco/web/issues/683",
                "token_name": "ETH",
                "received_on": "2018-04-30 18:09:11.078130+00:00",
                "to_username": "darkdarkdragon",
                "value_in_eth": "0.1700",
                "value_in_usdt_now": "35.25"
            },
            "bounty": 2918,
            "tip": 174,
            "kudos": null
        },
        {
            "activity_type": "start_work",
            "pk": 6081,
            "created": "2018-04-21T16:19:21.429259Z",
            "profile": {
                "id": 3205,
                "url": "/darkdarkdragon",
                "name": "Ivan Tivonenko",
                "handle": "darkdarkdragon",
                "keywords": [
                    "Java",
                    "Haxe",
                    "Elixir",
                    "JavaScript",
                    "Go",
                    "C++",
                    "Nemerle",
                    "Objective-C",
                    "HTML",
                    "CSS",
                    "Lua",
                    "Shell"
                ],
                "position": 0,
                "avatar_url": "https://c.gitcoin.co/avatars/f775a3147221545afd6bc41536cfd3b2/darkdarkdragon.png",
                "github_url": "https://github.com/darkdarkdragon",
                "total_earned": 1.2546094569297013,
                "organizations": {
                    "livepeer": 2,
                    "gitcoinco": 6
                }
            },
            "metadata": {
                "id": 2918,
                "title": "Notify Gitcoin/Funder that Developer is Starting Work",
                "token_name": "ETH",
                "value_in_eth": "170000000000000000.00",
                "value_in_token": "170000000000000000.00",
                "value_in_usdt_now": "81.96",
                "token_value_in_usdt": "540.05",
                "token_value_time_peg": "2018-03-22 03:10:08+00:00"
            },
            "bounty": 2918,
            "tip": null,
            "kudos": null
        }
    ],
    "keywords": "web, gitcoinco, JavaScript, Python, HTML, CSS, Shell",
    "current_bounty": true,
    "value_in_eth": "0.17",
    "token_value_in_usdt": "540.05",
    "value_in_usdt_now": "231.30",
    "value_in_usdt": "91.81",
    "status": "done",
    "now": "2022-10-05T15:12:44.417159Z",
    "avatar_url": "https://gitcoin.co/dynamic/avatar/gitcoinco",
    "value_true": "0.17",
    "issue_description": "### User Story\n\nAs a developer/contributor, I want to notifiy Gitcoin that I'm starting work in one place.\n\n### Description\n\n[comment]: # (Feature or Bug? i.e Type: Bug)\n*Type*: Feature\n\n[comment]: # (Describe the problem and why this task is needed. Provide description of the current state, what you would like to happen, and what actually happen)\n*Summary*: Currently users make a comment on the Github issue that they'd like to start work, but sometimes forget to go to Gitcoin to click the start work button. Sometimes users also are not aware that they should ask questions about the ticket or provide an approach to the funder.\n\n### Expected Behavior\nUser should notify Gitcoin in just one place by clicking start work button. A short form opens asking the user a few questions and when user clicks submit, the notes automatically get posted to Github and we accept this as the user has started work.\n\n### Solution\n[comment]: # (Provide a summary of the solution and a task list on what needs to be fixed.)\n*Summary*: Please see attached wireframes and designs.\n\n### Definition of Done\n- All button states are implemented.\n- Form and confirmation is implemented.\n- GitcoinBot comment is updated to include the commentary that the developer provided directly in the \n   Gitcoin form, should appear on the Github thread (see wireframe).\n\n### Additional Information\nPlease work with @thelostone-mc who is building #419 (Issue details page)\nPlease review with @PixelantDesign\n\n### Materials\n![startwork_1](https://user-images.githubusercontent.com/238235/37747971-268ac088-2d58-11e8-97cd-66789e5ce89b.png)\n\n![startwork_spec](https://user-images.githubusercontent.com/238235/37747984-313ae062-2d58-11e8-9ed7-1c30c91de65c.png)\n\n[Gitcoin - Start Issue.pdf](https://github.com/gitcoinco/web/files/1835873/Gitcoin.-.Start.Issue.pdf)",
    "network": "mainnet",
    "org_name": "gitcoinco",
    "pk": 2918,
    "issue_description_text": "### User Story\n\nAs a developer/contributor, I want to notifiy Gitcoin that I'm starting work in one place.\n\n### Description\n\n[comment]: # (Feature or Bug? i.e Type: Bug)\n*Type*: Feature\n\n[comment]: # (Describe the problem and why this task is needed. Provide description of the current state, what you would like to happen, and what actually happen)\n*Summary*: Currently users make a comment on the Github issue that they'd like to start work, but sometimes forget to go to Gitcoin to click the start work button. Sometimes users also are not aware that they should ask questions about the ticket or provide an approach to the funder.\n\n### Expected Behavior\nUser should notify Gitcoin in just one place by clicking start work button. A short form opens asking the user a few questions and when user clicks submit, the notes automatically get posted to Github and we accept this as the user has started work.\n\n### Solution\n[comment]: # (Provide a summary of the solution and a task list on what needs to be fixed.)\n*Summary*: Please see attached wireframes and designs.\n\n### Definition of Done\n- All button states are implemented.\n- Form and confirmation is implemented.\n- GitcoinBot comment is updated to include the commentary that the developer provided directly in the \n   Gitcoin form, should appear on the Github thread (see wireframe).\n\n### Additional Information\nPlease work with @thelostone-mc who is building #419 (Issue details page)\nPlease review with @PixelantDesign\n\n### Materials\n![startwork_1](https://user-images.githubusercontent.com/238235/37747971-268ac088-2d58-11e8-97cd-66789e5ce89b.png)\n\n![startwork_spec](https://user-images.githubusercontent.com/238235/37747984-313ae062-2d58-11e8-9ed7-1c30c91de65c.png)\n\n[Gitcoin - Start Issue.pdf](https://github.com/gitcoinco/web/files/1835873/Gitcoin.-.Start.Issue.pdf)",
    "standard_bounties_id": 176,
    "web3_type": "bounties_network",
    "can_submit_after_expiration_date": false,
    "github_issue_number": 683,
    "github_org_name": "gitcoinco",
    "github_repo_name": "web",
    "idx_status": "done",
    "token_value_time_peg": "2018-03-22T03:10:08Z",
    "fulfillment_accepted_on": null,
    "fulfillment_submitted_on": null,
    "fulfillment_started_on": "2018-04-21T16:19:21.429259Z",
    "canceled_on": null,
    "canceled_bounty_reason": "",
    "action_urls": {
        "fulfill": "/issue/fulfill?pk=2918&network=mainnet",
        "increase": "/issue/increase?pk=2918&network=mainnet",
        "accept": "/issue/accept?pk=2918&network=mainnet",
        "cancel": "/issue/cancel?pk=2918&network=mainnet",
        "payout": "/issue/payout?pk=2918&network=mainnet",
        "advanced_payout": "/issue/advanced_payout?pk=2918&network=mainnet",
        "invoice": "/issue/invoice?pk=2918&network=mainnet"
    },
    "project_type": "traditional",
    "permission_type": "permissionless",
    "attached_job_description": null,
    "needs_review": false,
    "github_issue_state": "closed",
    "is_issue_closed": true,
    "additional_funding_summary": {},
    "funding_organisation": "",
    "paid": [
        "darkdarkdragon"
    ],
    "event": null,
    "admin_override_suspend_auto_approval": false,
    "reserved_for_user_handle": "",
    "is_featured": false,
    "featuring_date": null,
    "repo_type": "public",
    "funder_last_messaged_on": null,
    "can_remarket": false,
    "is_reserved": null,
    "contact_details": {},
    "usd_pegged_value_in_token_now": "0.17",
    "usd_pegged_value_in_token": "0.17",
    "value_true_usd": "0.00",
    "peg_to_usd": false,
    "owners": [],
    "payout_date": null,
    "acceptance_criteria": "",
    "resources": "",
    "bounty_source": "github",
    "bounty_owner_profile": null,
    "never_expires": false,
    "custom_issue_description": ""
}

```

## donation Object example
```json
{
    "token": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE", 
    "amount": 100000000000000000, 
    "donor": "0x9a568bFeB8CB19e4bAfcB57ee69498D57D9591cA", 
    "dest": "0x6e1A86826E3bFec941B947a7Dc05C4c718425a61", 
    "txHash": "0xa63595cce2bbcbfdecb596622b24aa68aa55a41c70e9a09e6e2d947fc7cb399b", "chain": "Ethereum", 
    "chainId": "1"
}

```

