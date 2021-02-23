import requests

data = {'from': 'en',
        'to': 'zh',
        'query': 'Synthetic lethality, an interaction whereby the co-occurrence of two genetic events leads to cell death but one event alone does not, can be exploited for cancer therapeutics1. DNA repair processes represent attractive synthetic lethal targets since many cancers exhibit an impaired DNA repair pathway, which can lead to dependence on specific repair proteins2. The success of poly (ADP-ribose) polymerase 1 (PARP-1) inhibitors in homologous recombination-deficient cancers highlights the potential of this approach3. Hypothesizing that other DNA repair defects would give rise to synthetic lethal relationships, we queried dependencies in cancers with microsatellite instability (MSI), which results from deficient DNA mismatch repair (dMMR). Here we analyzed data from large-scale CRISPR/Cas9 knockout and RNA interference (RNAi) silencing screens and found that the RecQ DNA helicase WRN was selectively essential in MSI models in vitro and in vivo, yet dispensable in microsatellite stable (MSS) models. WRN depletion induced double-strand DNA breaks (DSB) and promoted apoptosis and cell cycle arrest selectively in MSI models. MSI cancer models required the helicase activity, but not the exonuclease activity of WRN. These findings expose WRN as a synthetic lethal vulnerability and promising drug target for MSI cancers.',
        'transtype': 'translang',
        'simple_means_flag': '3',
        'sign': '978366.659087',
        'token': 'fdcd1d56a8eacc0c4f5ee7e2c486bcba',
        'domain': 'common',
        }
headers = {
        'Host': 'fanyi.baidu.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Length': '1476',
        'Origin': 'https://fanyi.baidu.com',
        'Connection': 'keep-alive',
        'Referer': 'https://fanyi.baidu.com/translate',
        'Cookie': 'BAIDUID=7500A7B882ED9F98D2FA275FC9564F44:FG=1; BIDUPSID=7500A7B882ED9F98D2FA275FC9564F44; BDUSS=9CcGRaMW9KbGpmdmRvLXgwb3Z5Z3k1YnhtNkFCeE9zY3VyamhLbm85MVNQMVZnRVFBQUFBJCQAAAAAAAAAAAEAAABNBEiV1-a0q8qusMu0-sW3u8oAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFKyLWBSsi1geW; PSTM=1613607922; H_PS_PSSID=33425_33515_33272_33602_33584_22160; BDORZ=FFFB88E999055A3F8A630C64834BD6D0; BDSFRCVID=_8kOJexroG3VdZ5ea_oBDCggZ2KK0gOTDYLtOwXPsp3LGJLVN4vPEG0Ptf8gjM--J8jwogKK0gOTH6KF_2uxOjjg8UtVJeC6EG0Ptf8g0M5; H_BDCLCKID_SF=tbkD_C-MfIvhDRTvhCcjh-FSMgTBKI62aKDsQCO2BhcqJ-ovQTb4bhTybfQd2MjR0KvMhDocWKJJ8UbeWfvp3t_D-tuH3lLHQJnp2DbKLp5nhMJmb67JMxrDqtCOaJby523ion3vQpP-OpQ3DRoWXPIqbN7P-p5Z5mAqKl0MLPbtbb0xXj_0DTbLjH8jqTna--oa3RTeb6rjDnCrBT7OXUI82h5y05Jp5NLt0Rj7ahoUEn3TW65vyT8sXnORXx7JB5vvbPOMthRnOlRKX6O-0ML1Db3JKjvM2eDLslFy2t3oepvoD-Jc3MvByPjdJJQOBKQB0KnGbUQkeq8CQft20b0EeMtjW6LEK5r2SC8XtIQP; __yjs_duid=1_0cdec12ae1ea53534cc93a7202c1faee1613625336875; ab_sr=1.0.0_YTRkODQxOGY0MTQyMTgwMDcwNmRhMzYyNTRhYTg1ZGRjMDQ5N2ExYThjYWI1ZGQwOTk1YTBmNThmNzBlNzkzYTc5MmI2MjlhNTAzZTg3YmM0MzZmMWVlY2JkZDY5MWUx; BA_HECTOR=24250gak25a00l2g801g2sbni0r; Hm_lvt_64ecd82404c51e03dc91cb9e8c025574=1613639417; Hm_lpvt_64ecd82404c51e03dc91cb9e8c025574=1613639417; REALTIME_TRANS_SWITCH=1; FANYI_WORD_SWITCH=1; HISTORY_SWITCH=1; SOUND_SPD_SWITCH=1; SOUND_PREFER_SWITCH=1; __yjsv5_shitong=1.0_7_1d8d32fb7e746d6b024d6f79fd793411756a_300_1613639417757_27.18.51.45_2d816428',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
}
r = requests.post(url='https://fanyi.baidu.com/v2transapi?from=en&to=zh', json=data, headers=headers)
print(r.text)
