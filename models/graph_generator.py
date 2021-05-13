# -*- coding: utf-8 -*-
from neo4j import GraphDatabase
import pandas as pd
import os 
import subprocess
import requests
import unidecode
import re
import csv
import sys
import re, numpy as np, pandas as pd
from pprint import pprint
from nltk.corpus import stopwords
from connections.neo4j_connector import Neo4j_Connector
import spacy
from spacy.tokens import Doc
import numpy
from spacy.attrs import LOWER, POS, ENT_TYPE, IS_ALPHA
from models.tuple_extractor import Tuple_Extractor


class Graph_Generator:

    def __init__(self, neo4j_client, language="pt-br"):
        self.neo4j_client = neo4j_client
        
        self.language = language
        
        self.nlp = self._load_spacy()


    def _load_spacy(self):
        '''metodo privado que retorna o modelo do spacy baseado no idioma'''
        print("  --> Carregando modelo Spacy")
        
        if self.language == "pt-br":
           nlp = spacy.load('pt_core_news_lg')
           
           
        elif self.language == "us-en":
            nlp = spacy.load("en_core_web_sm")
        
        print("Pronto.")
        return nlp

    def extrair_ner(self, texto):
        doc = self.nlp(texto)

        list_of_dict = []

        for ent in doc.ents:
            #print(ent.text, ent.start_char, ent.end_char, ent.label_)
            dict_ner = {
                "nome": ent.text,
                "label": ent.label_
            }
            list_of_dict.append(dict_ner)

        df_ners = pd.DataFrame(list_of_dict)
        
        return df_ners
    
    def persistir_ners(self, df_texto):

        print("   --> Pre Processando NERs")
        df_texto['texto'] = df_texto.apply(lambda x: self.pre_process_text(x['texto']), axis=1) 
        print("Pronto!")

        for row in df_texto.iterrows():
            print("   --> Extraindo NER do video: {}".format(row[1]["video_id"]))
            texto = row[1]["texto"]
            video_id = row[1]["video_id"]
            df_ners = self.extrair_ner(texto)
            df_ners['count'] = ""
            

            print(df_ners)
            if len(df_ners) > 0: 
                df_agg = df_ners.groupby(['label', 'nome']).agg('count')
                df_agg.reset_index(inplace=True)
                df_agg['type'] = df_agg['label']

                # print(df_agg)
                
                # print("**")
                # print(type(df_agg))

                self.neo4j_client.add_nodes(df_agg)

                for r in df_agg.iterrows():
                    nome_ner = r[1]['nome']
                    label = r[1]['label']
                    contagem = r[1]['count']
                    relation = "EXTRAIU {count:" + str(contagem) + "}"
                    self.neo4j_client.criar_relacao_de_para(video_id, nome_ner, "video", label, relation)

    def pre_process_text(self, texto, rel=False, lower_case=False):
        texto = texto.replace('"', "")
        texto = texto.replace("'", "")

        if lower_case == True:
            texto.lower()

        if rel == True:
            texto = texto.replace(" ", "_")
            texto = texto.replace("-", "")
            texto = texto.replace(",", "_")
            texto = texto.replace("#", "")
            if texto[0] == "_":
                texto = texto[1:]
            if texto[-1] == "_":
                texto = texto[:-1]

        return texto


    def persistir_topicos_latentes(self, df_key_words, df_video_classifications):
        tipo_no = "TM"

        df_key_words['type'] = tipo_no
        print(" --> Persistindo Tópicos LDA no Neo4j")
        self.neo4j_client.add_nodes(df_key_words)
        print("Pronto!")

        print(" --> Criando Relações")
        
        for row in df_video_classifications.iterrows():
            nome_2 = "{}-topico_{}".format(row[1]['model'], row[1]['topico_1'])
            relation = "ABORDOU {prob:" + str(row[1]['prob_1']) + "}"
            self.neo4j_client.criar_relacao_de_para(row[1]['video_id'], nome_2, "video", tipo_no, relation)
        print("Pronto!")

    def persistir_tuplas_conhecimento(self, df):
        tuple_extractor = Tuple_Extractor()

        for row in df.iterrows():
            print("   --> Extraindo Tuplas do video: {}".format(row[1]["video_id"]))
            texto = row[1]["texto"]
            video_id = row[1]["video_id"]
            
            status = tuple_extractor.extrair_tupla(texto)
            if status == 1: #sucesso na extração das tuplas
                df_tuplas = tuple_extractor.get_ultimas_tuplas_geradas()
                
                df_tuplas['arg1'] = df_tuplas.apply(lambda x: self.pre_process_text(x['arg1']), axis=1) 
                df_tuplas['arg2'] = df_tuplas.apply(lambda x: self.pre_process_text(x['arg2']), axis=1) 
                df_tuplas['rel'] = df_tuplas.apply(lambda x: self.pre_process_text(x['rel'], rel=True), axis=1) 
                
                df_sentencas = pd.DataFrame()
                df_sentencas['nome'] = df_tuplas['sentenca']#.to_frame()
                df_sentencas['type'] = "SENTENÇA"

                df_arg1, df_arg2 = self.processar_df_tuplas(df_tuplas)
                
                self.neo4j_client.add_nodes(df_arg1)
                self.neo4j_client.add_nodes(df_arg2)



                self.neo4j_client.add_nodes(df_sentencas)

                print(" -> Criando Relações:")
                for row_tupla in df_tuplas.iterrows():
                    nome_1 = row_tupla[1]['arg1']
                    nome_2 = row_tupla[1]['arg2']
                    sentenca = row_tupla[1]['sentenca']
                    rel = row_tupla[1]['rel']

                    relacao = "RELAÇÃO_TUPLA {rel:'" + rel + "'}"
                   

                    self.neo4j_client.criar_relacao_de_para(nome_1, nome_2, "TUPLA", "TUPLA", relacao)
                    self.neo4j_client.criar_relacao_de_para(video_id, nome_1, "video", "TUPLA", "EXTRAIU_TUPLA")
                    self.neo4j_client.criar_relacao_de_para(video_id, nome_2, "video", "TUPLA", "EXTRAIU_TUPLA")

                    self.neo4j_client.criar_relacao_de_para(video_id, sentenca, "video", "SENTENÇA", "EXTRAIU_SENTENÇA")

            else:
                print("Erro na extração do video: {}".format(video_id))

    def processar_df_tuplas(self, df_tuplas):
        df_tuplas['type'] = "TUPLA"
        df_arg1 = df_tuplas[['arg1', 'type']]
        df_arg1 = df_arg1.rename(columns={"arg1": 'nome'})

        df_arg2 = df_tuplas[['arg2', 'type']]
        df_arg2 = df_arg1.rename(columns={"arg2": 'nome'})

        return df_arg1, df_arg2