import pandas as pd
import numpy as np
import pymysql
import os
import re

conn = pymysql.connect(host='192.168.124.10', port=3306, user='root', passwd='app1234', db='new_antibodies_info',
                       use_unicode=True, charset="utf8")
query_sql = "SELECT * FROM bdbiosciences_antibody_images"
df = pd.read_sql(query_sql, con=conn)

df.sample()

desc_dict = {
    'Neutralization': ['Neutralization', 'Neutralising'],
    'Immunoelectrophoresis': ['Immunoelectrophoresis', 'Counter Current Immunoelectrophoresis'],
    'Nucleotide Array': ['Nucleotide Array'],
    'In situ hybridization': ['In situ hybridization'],
    'Radioimmunoprecipitation': ['Radioimmunoprecipitation'],
    'Mass Cytometry': ['Mass Cytometry', 'IMC™ '],
    'Northern Blot': ['Northern Blot'],
    'IRMA': ['immunoradiometric assay'],
    'Inhibition Assay': ['Inhibition Assay'],
    'Protein Array': ['Protein Array'],
    'Southern Blot': ['Southern Blot'],
    'Agglutination': ['Agglutination'],
    'ELISpot': ['ELISpot'],
    'Dot blot': ['Dot blot'],
    'CHIPseq': ['CHIPseq'],
    'EMSA': ['EMSA '],
    'MeDIP': ['MeDIP'],
    'Electron Microscopy': ['Electron Microscopy'],
    'Immunomicroscopy': ['Immunomicroscopy'],
    'GSA': ['GSA '],
    'FRET': ['FRET '],
    'FPIA ': ['FPIA '],
    'PepArr': ['PepArr'],
    'Immunodiffusion': ['Immunodiffusion', 'RID ', 'Double Immunodiffusion'],
    'ChIP': ['ChIP/Chip', 'CHIP '],
    'IHC': ['IHC-M', 'mIHC ', 'IHC-FrFl', 'IHC (Methanol fixed)', 'immunohistochemistry', 'IHC-P', 'IHC - Wholemount', 'Immunohistochemistry (Floating vibratome sections)', 'IHC-Glut', 'IHC-FoFr', 'IHC-Fr', 'IHC-R', 'IHC-G', 'IHC (PFA fixed)', 'ihc ', 'IHC '],
    'ELISA': ['Competitive ELISA', 'In-Cell ELISA', 'Indirect ELISA', 'Sandwich ELISA', 'elisa', 'EIA '],
    'RIA (Radioimmunoassay)': ['RIA (Radioimmunoassay)', 'Radioimmunoassay', 'RIA '],
    'IF': ['ICC/IF', 'Immunocytochemistry', 'Immunofluorescence ', 'ICC ', 'if ', 'FM '],
    'FC': ['Flow Cyt', 'FC '],
    'WB': ['Western Blot', 'WB '],
    'RIP': ['RNA Binding Protein Immunoprecipitation', 'RIP '],
    'IP': ['Immunoprecipitation', 'IP '],
    '11111': ['Purification', 'Depletion', 'Microcytotoxicity testing', 'Northwestern', 'Thin Layer Chromatography', 'Cellular Activation', 'Functional Studies', 'Multiplex Protein Detection', 'SDS-PAGE', 'Coagulation', 'Other'],
}


samples = df['Image_description']


def get_desc(item):
    key_result_list = []
    value_result_list = []
    if not item:
        return ['miaoshuweikong', None]
    for desc_key, desc_value in desc_dict.items():
        desc_value_lower = [i.lower() for i in desc_value if isinstance(i, str) is True]
        for single_word in desc_value_lower:
            if single_word in item.replace('ChIP Grade', '').\
                    replace('/CHIP', '').\
                    replace('-OIF', '').\
                    replace('KIP', '').\
                    replace('NAIP', '').\
                    replace('TFIIF', '').\
                    replace('IgG Fc', '').\
                    replace('Anti-LIF', '').\
                    replace('VEGFC', '').\
                    replace('/PFC', '').\
                    replace('Anti-MIF', '').\
                    replace('Anti-Fc', '').\
                    replace('Anti-PDGFC', '').\
                    replace('Anti-RFC', '').\
                    replace('Anti-AIF', '').\
                    replace('Anti-HIF', '').\
                    replace('RI/FCER1A', '').\
                    replace('RI/FCER1A', '').\
                    replace('Anti-ZIP', '').\
                    replace('Anti-TXNIP', '').\
                    replace('Anti-MBIP', '').\
                    replace('/MIP', '').\
                    replace('Anti-TGIF', '').\
                    replace('Anti-FLIP', '').\
                    replace('Anti-Tollip', '').\
                    replace('Anti-SSX2IP', '').\
                    replace('Anti-AIP', '').\
                    replace('/PDIP', '').\
                    replace(' HIP ', '').\
                    replace('Anti-AIBZIP', '').\
                    replace('Anti-UCHL5IP', '').\
                    replace('Anti-HBXIP', '').\
                    replace('Anti-GIP', '').\
                    replace('Anti-ZIP', '').\
                    replace('Anti-Diphtheria', '').\
                    replace('Anti-FILIP1/FILIP', '').\
                    replace('Bacteria ', '').\
                    replace('/HOIP', '').\
                    replace('Anti-CtIP', '').\
                    replace('/GNIP', '').\
                    replace('Anti-BCCIP', '').\
                    replace('Anti-GMIP', '').\
                    replace('Anti-SVIP', '').\
                    replace('Anti-MLIP', '').\
                    replace('Anti-PTIP', '').\
                    replace('Anti-NFATC2IP', '').\
                    replace('Anti-FIP', '').\
                    replace('Anti-PBXIP1', '').\
                    replace('/HPIP', '').\
                    replace('Anti-MPRIP', '').\
                    replace('Anti-ATRIP', '').\
                    replace('Anti-IRIP', '').\
                    replace('Anti-NRIP', '').\
                    replace('Anti-RIP', '').\
                    replace('Anti-AFM', '').\
                    replace('Anti-TRIF', '').\
                    replace('Anti-RABIF', '').\
                    replace(' motif ', '').\
                    replace('Anti-SPIF', '').\
                    replace('Anti-Mitochondria', '').\
                    replace(' hybrid ', '').\
                    replace('Anti-Listeria', '').\
                    replace('Anti-TUFM', '').\
                    replace('Anti-FIF', '').\
                    replace(' BiP ', '').lower():
                new_desc = desc_key
                if desc_key not in key_result_list:
                    key_result_list.append(new_desc)
                    value_result_list.extend(desc_dict[new_desc])

    if len(key_result_list) > 0:
        tag = ';'.join(m for m in key_result_list)
        tag_detail = ';'.join(n.strip() for n in value_result_list)
        return [tag, tag_detail]
    else:
        return ['wupipei', None]


if __name__ == '__main__':
    df[['tag', 'tag_detail']] = df['Image_description'].apply(get_desc)[:].tolist()
    # print(df.sample())
    # col_name = df.columns.tolist() # 将数据框的列名全部提取出来存放在列表里
    # print(col_name)
    os.getcwd()
    df.to_csv('Result.csv', index=False, columns=['Image_url', 'Image_description', 'tag', 'tag_detail'])  # , columns=['Image_description', 'tag', 'tag_detail']
    print('done')

