from lxml import etree

file_path = 'D:/Dev/bio_work/1_pubmed/pubmed21n0567.xml'
xml = etree.parse(file_path)
items = xml.xpath("//PubmedArticle")

for item in items:
    reference_list = item.xpath(".//ReferenceList/Reference")
    print(reference_list)
    reference_results_list = []
    for reference in reference_list:
        try:
            citation_pmid = reference.xpath(
                './ArticleIdList/ArticleId[@IdType="pubmed"]/text()'
            )[0].strip()
            print(citation_pmid)
            # reference_results_list.append(new_reference)  # todo 结果
        except Exception:
            pass