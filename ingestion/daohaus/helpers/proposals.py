def label_proposal_type(row):
    if row['whitelist']:
        return 'whitelist'
    if row['trade'] :
        return 'trade'
    if row['newMember']:
        return 'newMember'
    if row['guildkick']:
        return 'guildKick'
    return 'other'

def label_status(row):
    if row['cancelled']:
        return 'cancelled'
    if row['didPass']:
        return 'passed'
    if row['aborted']:
        return 'aborted'
    return 'other'

def label_title(row):
    if (type(row['details']) is not dict):
        return ''
    
    x = row['details'].get('title', '')
    x = x.replace("'", '')
    x = x.replace('"', '')
    return x

def label_desc(row):
    if (type(row['details']) is not dict):
        return ''
    
    x = row['details'].get('description', '')
    x = x.replace("'", '')
    x = x.replace('"', '')
    return x