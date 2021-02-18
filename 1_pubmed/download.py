for line in open(r'D:\Dev\bio_work\1_pubmed\html.txt', encoding='utf-8'):
    if '.gz</a>' in line:
        print('https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/' + line.split('.gz">')[-1].split('</a>')[0])
