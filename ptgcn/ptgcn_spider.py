import requests
from lxml import etree
import json

url = 'http://www.ptgcn.com/products/AARSD1-Antibody-14900-1-AP.htm'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36',
}
with requests.Session() as s:
    resp = s.get(url=url, headers=headers)
    html = etree.HTML(resp.text)
    status = 0
# print(resp.text)

# product_name = html.xpath(
#     '//strong[@style="text-transform: capitalize;"]/text()')[0]
# print(product_name)
# catno = html.xpath('//span[@id="spnCatNo"]/text()')[0]
# print(catno)
# synonyms = html.xpath(
#     '//strong[@class="blue"][contains(text(),"Synonyms")]/../following-sibling::div[1]/text()')[
#     0].replace('\r\n', '').replace(' ', '')
# print(synonyms)
# application = html.xpath(
#     '//strong[@class="blue"][contains(text(), "Applications:")]/../following-sibling::div[1]/text()')[
#     0]
# print(application)
# clone_number = html.xpath(
#     '//span[@id="spnCatNo"]/following-sibling::span[2]/text()')[0]
# print(clone_number)
# # host_hpecies = html.xpath('//')
# reactivity_species = html.xpath(
#     '//strong[@class="blue"][contains(text(), "Source:")]/../following-sibling::div[1]/text()')
# print(reactivity_species)
# gene_id = html.xpath(
#     '//strong[@class="blue"][contains(text(), "Gene ID (NCBI):")]/../following-sibling::div[1]/a/text()')
# print(gene_id)
# species_reactivity = html.xpath(
#     '//strong[@class="blue"][contains(text(),"Species specificity:")]/../following-sibling::div[1]/text()')
# print(species_reactivity)
# immunogen = html.xpath(
#     '//strong[@class="blue"][contains(text(),"Immunogen:")]/../following-sibling::div[1]/a/text()')
# print(immunogen)
# predicted_MW = html.xpath(
#     '//strong[@class="blue"][contains(text(),"Calculated molecular weight:")]/../following-sibling::div[1]/text()')
# print(predicted_MW)
# observed_MW = html.xpath(
#     '//strong[@class="blue"][contains(text(),"Observed molecular weight:")]/../following-sibling::div[1]/text()')
# print(observed_MW)
# isotype = html.xpath(
#     '//strong[@class="blue"][contains(text(),"Isotype:")]/../following-sibling::div[1]/text()')
# print(isotype)

json_str = str(html.xpath(
    '//script[contains(text(),"#Country")]/text()')[0]).split(
    'result:')[1].replace(' });', '')
j = json.loads(json_str)

print(j)
print(j['table'][0]['rows'][0])
