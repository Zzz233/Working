import requests

with requests.session() as s:
    resp = s.get('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=PMC7376887')
    print(resp.text)
