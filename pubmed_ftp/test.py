results = []
for line in open("D:/Dev/bio_work/pubmed_ftp/ftp.txt", encoding="utf-8"):
    if ".xml.gz" in line and ".md5" not in line:
        results.append(
            "ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/" + line.split('"')[1]
        )
results.reverse()
for item in results:
    print(item)
