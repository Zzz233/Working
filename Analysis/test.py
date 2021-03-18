import requests
from lxml import etree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import time
import random
import redis
import json

Base = declarative_base()


class Data(Base):
    __tablename__ = "standard_tag"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    tag = Column(String(100), nullable=True, comment="")
    tag_detail = Column(String(1000), nullable=True, comment="")

engine = create_engine(
    "mysql+pymysql://root:app1234@192.168.124.10:3306/new_antibodies_info?charset=utf8"
)
DBSession = sessionmaker(bind=engine)
session = DBSession()


pre_dict = {
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
    'Dot blot': ['Dot blot', 'Dot-blot'],
    'CHIPseq': ['CHIPseq'],
    'EMSA': ['EMSA '],
    'MeDIP': ['MeDIP'],
    'Electron Microscopy': ['Electron Microscopy'],
    'Immunomicroscopy': ['Immunomicroscopy'],
    'GSA': ['GSA '],
    'PepArr': ['PepArr'],
    'Immunodiffusion': ['Immunodiffusion', 'Double Immunodiffusion'],
    'ChIP': ['ChIP/Chip', 'CHIP '],
    'IHC': ['IHC-M', 'mIHC ', 'IHC-FrFl', 'Immunohistochemisty', 'Immunhohistochemical', 'Imunohistochemical', 'Immunocyochemistry', 'Immunohistochemiacl', 'Immunohisrochemical', 'IHC (Methanol fixed)', 'Immunocytochemical', 'immunohistochemistry', 'Immunohistrochemical', 'Immunohistroxchemical', 'Immunoistochemical', 'Immunohistofluorescent', 'Immunohistochemiical', 'Immunohistochemical', 'IHC-P', 'IHC - Wholemount', 'Immunohistochemistry (Floating vibratome sections)', 'IHC-Glut', 'IHC-FoFr', 'IHC-Fr', 'IHC-R', 'IHC-G', 'IHC (PFA fixed)', 'ihc ', 'IHC '],
    'ELISA': ['Competitive ELISA', 'In-Cell ELISA', 'Indirect ELISA', 'Sandwich ELISA', 'elisa', 'EIA '],
    'RIA (Radioimmunoassay)': ['RIA (Radioimmunoassay)', 'Radioimmunoassay'],
    'IF': ['ICC/IF', 'IF analysis', 'IF image', 'Immunofluroscence', 'Immunocytochemistry', 'immunofluoroscent', 'Immunoflurocent', 'Immunofluorescence', 'Immunfluorescent', 'Immunofluorescent', 'Immunoflurorescent', 'Immunoflourescence', 'Immunoflourescent', 'Immunofluroescence', 'Immunofluroescent', 'Immunofluoresence', 'Immunofluoresent', 'Immunoflurorescence', 'Immunoflouresence', 'Immunofluorescense', 'immunofluoroscence'],
    'FC': ['Flow Cyt', 'Flow Cyotometry'],
    'WB': ['Western Blot', 'Wesstern blot', 'Western', 'Immunoblot', 'Western bltot', 'Westerm blot', 'Westernblot', 'Western bllot', 'by chemiluminescence', 'WB '],
    'RIP': ['RNA Binding Protein Immunoprecipitation'],
    'IP': ['Immunoprecipitation', 'MeRIP', 'for IP'],
    '11111': ['Purification', 'Depletion', 'Microcytotoxicity testing', 'Northwestern', 'Thin Layer Chromatography', 'Cellular Activation', 'Functional Studies', 'Multiplex Protein Detection', 'SDS-PAGE', 'Coagulation'],
}

last_dict = {
    'Neutralization': ['Neutralization', 'Neutralising'],
    'Immunoelectrophoresis': ['Immunoelectrophoresis', 'Counter Current Immunoelectrophoresis'],
    'Nucleotide Array': ['Nucleotide Array'],
    'In situ hybridization': ['In situ hybridization'],
    'Radioimmunoprecipitation': ['Radioimmunoprecipitation'],
    'Mass Cytometry': ['Mass Cytometry', 'IMC™'],
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
    'PepArr': ['PepArr '],
    'Immunodiffusion': ['Immunodiffusion', 'RID ', 'Double Immunodiffusion'],
    'ChIP': ['ChIP/Chip', 'CHIP'],
    'IHC': ['IHC-M', 'mIHC ', 'Immunhistochemical', 'IHC-FrFl', 'IHC (Methanol fixed)', 'immunohistochemistry', 'Immunohistochemical', 'mmunohistochemical', 'IHC-P', 'IHC - Wholemount', 'Immunohistochemistry (Floating vibratome sections)', 'IHC-Glut', 'IHC-FoFr', 'IHC-Fr', 'IHC-R', 'IHC-G', 'IHC (PFA fixed)', 'ihc ', 'IHC '],
    'ELISA': ['Competitive ELISA', 'In-Cell ELISA', 'Indirect ELISA', 'Sandwich ELISA', 'elisa', 'EIA '],
    'RIA (Radioimmunoassay)': ['RIA (Radioimmunoassay)', 'Radioimmunoassay', 'RIA '],
    'IF': ['ICC/IF', 'Immunocytochemistry', 'Immunofluorescence', 'Immunfluorescent', 'Immunofluorescent', 'ICC ', 'if ', 'FM '],
    'FC': ['Flow Cyt', 'FC', 'Fluorescence', 'Fluorescent'],
    'WB': ['Western Blot', 'WB '],
    'RIP': ['RNA Binding Protein Immunoprecipitation', 'RIP '],
    'IP': ['Immunoprecipitation', 'IP ', 'MeRIP'],
    '11111': ['Purification', 'Depletion', 'Microcytotoxicity testing', 'Northwestern', 'Thin Layer Chromatography', 'Cellular Activation', 'Functional Studies', 'Multiplex Protein Detection', 'SDS-PAGE', 'Coagulation', 'Other'],
}

# 并集
# retD = list(set(listB).difference(set(listA)))


for pre_key, pre_value in pre_dict.items():
    last_value = last_dict[pre_key]
    pre_dict[pre_key] = list(set(pre_value).union(set(last_value)))

print(pre_dict)
objs = []
for pre_key, pre_value in pre_dict.items():
    tag_text = ';'.join(i for i in pre_value)
    new_data = Data(tag=pre_key, tag_detail=tag_text)
    objs.append(new_data)

session.bulk_save_objects(objs)
session.commit()
session.close()
print(11)