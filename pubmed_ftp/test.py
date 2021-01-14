for line in open("D:\\new4.txt", encoding="utf-8"):
    if "refseq_protein" in line and "gz.md5" not in line:
        print(
            "https://ftp.ncbi.nlm.nih.gov/blast/db/"
            + line.replace("\n", "").split("           ")[0]
        )
