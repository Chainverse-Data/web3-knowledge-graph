# ENS record lookup Post Processor

This processor gets all ENS names in the database, and looks up the text records for all of them. After it has done so, it updates the Alias:Ens node, and then creates the Type:Account and links them to the ENS node through a HAS_ACCOUNT edge 

# Ontology

Nodes:
  - Email:Account
  - Twitter:Account
  - Github:Account
  - Peepeth:Account
  - Linkedin:Account
  - Keybase:Account
  - Telegram:Account
  - Alias:Ens
    - description
    - display
    - email
    - keywords
    - mail
    - notice
    - location
    - phone
    - url
    - github
    - peepeth
    - linkedin
    - twitter
    - keybase
    - telegram
Edges:
  - (Alias:Ens)-[HAS_ACCOUNT]->(Email:Account)
  - (Alias:Ens)-[HAS_ACCOUNT]->(Twitter:Account)
  - (Alias:Ens)-[HAS_ACCOUNT]->(Github:Account)
  - (Alias:Ens)-[HAS_ACCOUNT]->(Peepeth:Account)
  - (Alias:Ens)-[HAS_ACCOUNT]->(Linkedin:Account)
  - (Alias:Ens)-[HAS_ACCOUNT]->(Keybase:Account)
  - (Alias:Ens)-[HAS_ACCOUNT]->(Telegram:Account)