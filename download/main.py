import requests
from lxml import etree


def get_list(link):
    headers = {'Host': 'www.wandoujia.com',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
               'Accept-Encoding': 'gzip, deflate, br',
               'Referer': 'https://www.baidu.com/link?url=Z1uHSR7dgBAx7YXd7TnkwvO702AhdoKT6RtOWLVh5CBAPlLpmFkdNXMBLvHvLR0_pErcxCkPCkp8f8Uzgapsma&wd=&eqid=e74ce72c000987fc000000066010c0e5',
               'Connection': 'keep-alive',
               'Cookie': '_uab_collina=161171118273404247361328; ctoken=sFgcl1PRaklDucjMKpq5yWYU; sid=21677690161171069894809221173623; sid.sig=tJwLwazdD2vKU_VPutOW7Nl509UkysnStw5a6rQVPgM; _pwid=41077600161171069802420412378053; wdj_source=seo_baidu; _ga=GA1.2.562588970.1611710698; _gid=GA1.2.1014307424.1611710698; Hm_lvt_c680f6745efe87a8fabe78e376c4b5f9=1611710698,1611711189,1611712295,1611713921; Hm_lpvt_c680f6745efe87a8fabe78e376c4b5f9=1611713921; UM_distinctid=1774171941d2d3-0b42672ad5ab2-4c3f207e-1fa400-1774171941e9c8; CNZZDATA1272849134=975533653-1611705651-https%253A%252F%252Fwww.baidu.com%252F%7C1611711054; cna=S66XGKQKtWECAasrvVN7zY3C; isg=BHR0ogS4qLdH4Tz7fAr0VrwbRjLmTZg3yxZD2A7Vnf-DeRXDNl8cxkp4-TGhmtCP; xlly_s=1; _uToken=T2gAdySAv6KEW8ogX5IZqvCtVSJIV6juvlFn8vxtouHCMv-_RsEIfei4rviJDO4ngiU%3D; x5sec=7b2277616762726964676561643b32223a2237613533353265393236643031623964613932653239323634313266356631324350756177344147454e584c70744c746d5a4b5450673d3d227d; _gat=1',
               'Upgrade-Insecure-Requests': '1',
               'Pragma': 'no-cache',
               'Cache-Control': 'no-cache',
               'TE': 'Trailers'}
    r = requests.get(url=link, headers=headers)
    xml = etree.HTML(r.text)
    link_list = xml.xpath('.//ul[@class="old-version-list"]/li[@class]/a[@data-app-id]/@href')
    # print(lis)
    return link_list


def get_download_list(link_list):
    results = {}
    for item in link_list:
        headers = {
            'Host': 'www.wandoujia.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': item,
            'Connection': 'keep-alive',
            'Cookie': '_uab_collina=161171118273404247361328; ctoken=sFgcl1PRaklDucjMKpq5yWYU; sid=21677690161171069894809221173623; sid.sig=tJwLwazdD2vKU_VPutOW7Nl509UkysnStw5a6rQVPgM; _pwid=41077600161171069802420412378053; wdj_source=seo_baidu; _ga=GA1.2.562588970.1611710698; _gid=GA1.2.1014307424.1611710698; Hm_lvt_c680f6745efe87a8fabe78e376c4b5f9=1611711189,1611712295,1611713921,1611713933; Hm_lpvt_c680f6745efe87a8fabe78e376c4b5f9=1611714700; UM_distinctid=1774171941d2d3-0b42672ad5ab2-4c3f207e-1fa400-1774171941e9c8; CNZZDATA1272849134=975533653-1611705651-https%253A%252F%252Fwww.baidu.com%252F%7C1611711054; cna=S66XGKQKtWECAasrvVN7zY3C; isg=BHR0ogS4qLdH4Tz7fAr0VrwbRjLmTZg3yxZD2A7Vnf-DeRXDNl8cxkp4-TGhmtCP; xlly_s=1; _uToken=T2gAdySAv6KEW8ogX5IZqvCtVSJIV6juvlFn8vxtouHCMv-_RsEIfei4rviJDO4ngiU%3D; x5sec=7b2277616762726964676561643b32223a2237613533353265393236643031623964613932653239323634313266356631324350756177344147454e584c70744c746d5a4b5450673d3d227d',
            'Upgrade-Insecure-Requests': '1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
        }
        r = requests.get(item, headers=headers)
        xml = etree.HTML(r.text)
        version = xml.xpath('.//p[@class="version-name"]/span/text()')[0].strip()
        download_link = xml.xpath('.//div[@class="qr-info"]/a/@href')
        results[version] = download_link
    return results


def download_from_url(link, save_file_path, chunk_size=128):
    r = requests.get(link, stream=True)
    with open(save_file_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            print(chunk)
            fd.write(chunk)
        print('done')


if __name__ == '__main__':
    page_list = get_list('https://www.wandoujia.com/apps/39743/history')
    print(page_list)
    version_url = get_download_list(page_list)
    for key, value in version_url:
        download_from_url(value, 'D:/Dev/bio_work/download/'+key)
