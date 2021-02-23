import pandas as pd
import numpy as np
import pymysql
import os
import re

conn = pymysql.connect(host='192.168.124.10', port=3306, user='root', passwd='app1234', db='new_antibodies_info',
                       use_unicode=True, charset="utf8")
query_sql = "SELECT * FROM abcam_antibody_images"
df = pd.read_sql(query_sql, con=conn)

df.sample()

desc_dict = {
    'Neutralization': ['Neutralization', 'Neutralising'], 'IHC': ['IHC-M', 'mIHC ', 'IHC-FrFl', 'IHC (Methanol fixed)', 'immunohistochemistry', 'IHC-P', 'ihc ', 'IHC - Wholemount', 'Immunohistochemistry (Floating vibratome sections)', 'IHC-Glut', 'IHC-FoFr', 'IHC-Fr', 'IHC-R', 'IHC-G', 'IHC ', 'IHC (PFA fixed)'], 'ChIP': ['CHIP ', 'ChIP/Chip'], 'IRMA': ['immunoradiometric assay'], '11111': ['Purification', 'Depletion', 'Microcytotoxicity testing', 'Northwestern', 'Thin Layer Chromatography', 'Cellular Activation', 'Functional Studies', 'Multiplex Protein Detection', 'Other', 'SDS-PAGE', 'Coagulation'], 'Immunoelectrophoresis': ['Immunoelectrophoresis', 'Counter Current Immunoelectrophoresis'], 'ELISA': ['Competitive ELISA', 'In-Cell ELISA', 'EIA ', 'Indirect ELISA', 'Sandwich ELISA', 'elisa'], 'RIA (Radioimmunoassay)': ['RIA (Radioimmunoassay)', 'RIA ', 'Radioimmunoassay'], 'PepArr': ['PepArr'], 'Nucleotide Array': ['Nucleotide Array'], 'IF': ['ICC/IF', 'ICC ', 'if ', 'Immunocytochemistry', 'Immunofluorescence ', 'FM '], 'Immunodiffusion': ['Immunodiffusion', 'RID ', 'Double Immunodiffusion'], 'In situ hybridization': ['In situ hybridization'], 'RIP': ['RNA Binding Protein Immunoprecipitation', 'RIP '], 'Radioimmunoprecipitation': ['Radioimmunoprecipitation'], 'Mass Cytometry': ['Mass Cytometry', 'IMC™ '], 'Northern Blot': ['Northern Blot'], 'Inhibition Assay': ['Inhibition Assay'], 'Protein Array': ['Protein Array'], 'GSA': ['GSA '], 'IP': ['Immunoprecipitation', 'IP '], 'FC': ['FC ', 'Flow Cyt'], 'Southern Blot': ['Southern Blot'], 'FPIA ': ['FPIA '], 'Agglutination': ['Agglutination'], 'FRET': ['FRET '], 'WB': ['WB ', 'Western Blot'], 'MeDIP': ['MeDIP'], 'Electron Microscopy': ['Electron Microscopy'], 'Immunomicroscopy': ['Immunomicroscopy'], 'EMSA': ['EMSA '], 'ELISpot': ['ELISpot'], 'Dot blot': ['Dot blot'], 'CHIPseq': ['CHIPseq'], 'Blocking': ['Blocking']
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
            if single_word in item.replace('ChIP Grade', '').lower():
                new_desc = desc_key
                if desc_key not in key_result_list:
                    key_result_list.append(new_desc)
                    value_result_list.extend(desc_dict[new_desc])
    if len(key_result_list) > 0:
        tag = ';'.join(m for m in key_result_list)
        tag_detail = ';'.join(n for n in value_result_list)
        return [tag, tag_detail]
    else:
        return ['wupipei', None]


if __name__ == '__main__':
    df[['tags', 'tags_detail']] = df['Image_description'].apply(get_desc)[:].tolist()
    print(df.sample())
    col_name = df.columns.tolist()                   # 将数据框的列名全部提取出来存放在列表里
    print(col_name)
    os.getcwd()
    df.to_csv('Result.csv', index=False, columns=['Image_description', 'tags', 'tags_detail'])
    print('done')

