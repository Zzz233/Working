for line in open(r"D:\Dev\bio_work\pubmed_ftp\ftp_update.txt", encoding="utf-8"):
    if '.xml.gz"' in line:
        print("ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/" + line.split('"')[1])
