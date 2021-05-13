# -*- coding: utf-8 -*-
"""
Created on Wed Mar 25 11:24:51 2020

@author: João Pedro Santos Rodrigues
@project: Classe que trata o topic modeling
"""
from spacy.tokens import Doc
import numpy
from spacy.attrs import LOWER, POS, ENT_TYPE, IS_ALPHA
from neo4j import GraphDatabase
import pandas as pd
import os 
#import subprocess
#import requests
#import unidecode
import re
import csv
from acessos import get_conn, read, persistir_banco, persistir_multiplas_linhas

import sys
import re, numpy as np, pandas as pd
from pprint import pprint
import spacy
# Gensim
import gensim
from gensim import corpora, models, similarities
import gensim, spacy, logging, warnings
import gensim.corpora as corpora
from gensim.utils import lemmatize, simple_preprocess
from gensim.models import CoherenceModel
import matplotlib.pyplot as plt
from gensim import corpora, models, similarities
# NLTK Stop words
from nltk.corpus import stopwords
from acessos import read, get_conn, persistir_uma_linha, persistir_multiplas_linhas, replace_df
from gensim.models.ldamulticore import LdaMulticore
import seaborn as sns
import matplotlib.colors as mcolors


#%matplotlib inline
warnings.filterwarnings("ignore",category=DeprecationWarning)
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)

import os

import warnings
warnings.filterwarnings("ignore",category=DeprecationWarning)
from gensim.test.utils import datapath


class Topic_Modeling:

    def __init__(self,  language="pt-br", stop_words_list=[]):
        self.language = language
        self.stop_words = self._load_stop_words(stop_words_list)
        self.nlp = self._load_spacy()
        self.model_list =[] 
        self.coherence_values = []
        self.lista_num_topics = []
        self.melhor_modelo = None

    def _load_spacy(self):
        '''metodo privado que retorna o modelo do spacy baseado no idioma'''
        #disable_list = ['parser', 'ner']
        disable_list = []
        
        if self.language == "pt-br":
           nlp = spacy.load('pt_core_news_lg', disable=disable_list)
           
           
        elif self.language == "us-en":
            nlp = spacy.load("en_core_web_sm", disable=disable_list)
            
        return nlp
    
    def _load_stop_words(self, stop_words_list=[]):
        '''metodo privado que retorna as stop words baseado no idioma'''
        if self.language == "pt-br":
           stop_words = stopwords.words('portuguese')
           stop_words.extend(stop_words_list)
           
        elif self.language == "us-en":
            stop_words = stopwords.words('english') #Testar
            stop_words.extend(['from', 'subject', 're', 'edu', 'use', 'not', 'would', 'say', 'could', '_', 'be', 'know', 'good', 'go', 'get', 'do', 'done', 'try', 'many', 'some', 'nice', 'thank', 'think', 'see', 'rather', 'easy', 'easily', 'lot', 'lack', 'make', 'want', 'seem', 'run', 'need', 'even', 'right', 'line', 'even', 'also', 'may', 'take', 'come'])
            stop_words.extend(stop_words_list)

        return stop_words


    def filtrar_pos_tag(self, texto, allowed_postags=["NOUN", "PROPN", "VERB", "ADJ"]):
        texto_saida = ""
        doc = self.nlp(texto)
        for token in doc:
            if token.pos_ in allowed_postags:
                texto_saida += " {}".format(token)
        return texto_saida
        
        
    def replace_ner_por_label(self, texto):
            texto_out = texto
            doc = self.nlp(texto)
            for ent in reversed(doc.ents):
                #label = " _" + ent.label_ + "_ "
                label = ent.label_
                comeco = ent.start_char
                fim = comeco + len(ent.text)
                
                texto_out = texto_out [:comeco] + label + texto_out[fim:]
            return texto_out

    def processamento_inicial(self, lista_documentos):
        '''remove emails, quebra de linhas e single quotes'''
        
        #Tratando abreviações
        lista_documentos = [re.sub('neh', 'né', sent) for sent in lista_documentos]
        lista_documentos = [re.sub('td', 'tudo', sent) for sent in lista_documentos]
        lista_documentos = [re.sub('tds', 'todos', sent) for sent in lista_documentos]
        lista_documentos = [re.sub('vc', 'você', sent) for sent in lista_documentos]
        lista_documentos = [re.sub('vcs', 'vocês', sent) for sent in lista_documentos]
        lista_documentos = [re.sub('voce', 'você', sent) for sent in lista_documentos]
        lista_documentos = [re.sub('tbm', 'também', sent) for sent in lista_documentos]
        
        # Remove Emails
        lista_documentos = [re.sub('\S*@\S*\s?', '', sent) for sent in lista_documentos]
        
        # Remove new line characters
        lista_documentos = [re.sub('\s+', ' ', sent) for sent in lista_documentos]
        
        # Remove distracting single quotes
        lista_documentos = [re.sub("\'", "", sent) for sent in lista_documentos]
        
        return lista_documentos

    def sent_to_words(self, sentences):
        '''tokeniza um unico documento'''
        for sentence in sentences:
            yield(gensim.utils.simple_preprocess(str(sentence), deacc=False))  # deacc=True removes punctuations
    
    def tokenizar(self, lista_documentos):        
        '''tokeniza uma lista de documentos'''
        lista_documentos_tokenizado =  list(self.sent_to_words(lista_documentos))
        return lista_documentos_tokenizado
    
    def montar_n_grams(self, lista_documentos_tokenizado):
        '''monta bi_grams e tri_grams de uma lista de documentos tokenizado
           utilizar este metodo depois de remover stop words'''
           
        bigram = gensim.models.Phrases(lista_documentos_tokenizado, min_count=5, threshold=100) # higher threshold fewer phrases.
        trigram = gensim.models.Phrases(bigram[lista_documentos_tokenizado], threshold=100)  
        
        # Faster way to get a sentence clubbed as a trigram/bigram
        bigram_mod = gensim.models.phrases.Phraser(bigram)
        trigram_mod = gensim.models.phrases.Phraser(trigram)
        
        #retorna lista bigram e trigram

        self.bigram = [bigram_mod[doc] for doc in lista_documentos_tokenizado]
        self.trigram = [trigram_mod[bigram_mod[doc]] for doc in lista_documentos_tokenizado]  

        

        return self.bigram , self.trigram 
    
    def get_n_grams(self):
        return self.bigram , self.trigram 

    def lematizar_documentos(self, lista_documentos_tokenizado):
        """https://spacy.io/api/annotation"""
        documentos_out = []
        for sent in lista_documentos_tokenizado:
            doc = self.nlp(" ".join(sent)) 
            lista_tokens_lematizados = []
            for token in doc :
                lista_tokens_lematizados.append(token.lemma_)
            documentos_out.append(lista_tokens_lematizados)
        return documentos_out  
    

    
    
    def remover_stop_words(self, lista_documentos_tokenizado):
        return [[word for word in simple_preprocess(str(doc)) if word not in self.stop_words] for doc in lista_documentos_tokenizado]
    
    def montar_novo_corpus(self, nova_lista_documentos_lematizada, id2word):
        print(id2word)
        corpus = [id2word.doc2bow(text) for text in nova_lista_documentos_lematizada]
        return corpus
    
    def pre_processar_texto_ou_lista(self, texto_ou_lista, filtro_ner=True, allowed_postags=["NOUN","PROPN", "VERB", "ADJ"]):
        
        if isinstance(texto_ou_lista, str):
            lista_documentos  = [texto_ou_lista]
        else:
            lista_documentos  = texto_ou_lista    
        lista_documentos = self.processamento_inicial(lista_documentos)
        
        if filtro_ner==True:
            lista_documentos = [self.replace_ner_por_label(texto) for texto in lista_documentos]
          
        # if filtro_pos_tag==True:
        #     lista_documentos = [self.filtrar_pos_tag(texto) for texto in lista_documentos]
        lista_documentos = [self.filtrar_pos_tag(texto, allowed_postags) for texto in lista_documentos]  
        
        lista_documentos_tokenizado = self.tokenizar(lista_documentos)
        lista_documentos_tokenizado_stop_words = self.remover_stop_words(lista_documentos_tokenizado)
        lista_documento_bi_gram, lista_documento_tri_gram = self.montar_n_grams(lista_documentos_tokenizado_stop_words)
        lista_documento_lematizada = self.lematizar_documentos(lista_documento_tri_gram)
        #lista_documento_lematizada = lista_documento_bi_gram
        return lista_documento_lematizada
    
    def gerar_modelo_hdp(self, corpus, id2word, texts):
        model_hdp = models.HdpModel(corpus, id2word=id2word)
        coherencemodel = CoherenceModel(model=model_hdp, texts=texts, dictionary=id2word, coherence='c_v')
        self.melhor_modelo = model_hdp
        
        return model_hdp, coherencemodel.get_coherence()
    
    def gerar_multiplos_modelos(self, id2word, corpus, texts, limit, start=2, step=3):
        print("Start: {}".format(start)) 
        print("limit: {}".format(limit))
        print("Step: {}".format(step))       
        self.start = start 
        self.limit = limit
        self.step = step
        
        coherence_values = []
        model_list = []
        for num_topics in range(start, limit, step):
            print("Gerando novo modelo...")
           
            # model = gensim.models.ldamodel.LdaModel(corpus=corpus,
            #                                    id2word=id2word,
            #                                    num_topics=num_topics, 
            #                                    random_state=100,
            #                                    update_every=1,
            #                                    chunksize=100,
            #                                    passes=10,
            #                                    alpha='auto',
            #                                    per_word_topics=True)
            
            lda = LdaMulticore(corpus=corpus, 
                                id2word=id2word,
                                random_state=100, 
                                num_topics=num_topics,
                                workers=3)

            model_list.append(model)
            coherencemodel = CoherenceModel(model=model, texts=texts, dictionary=id2word, coherence='c_v')
            coherence_values.append(coherencemodel.get_coherence())
            
            self.lista_num_topics.append(num_topics)
            self.model_list = model_list
            self.coherence_values = coherence_values
        return model_list, coherence_values
    
    def plotar_coerencia(self):
        x = range(self.start, self.limit, self.step)
        plt.plot(x, self.coherence_values)
        plt.xlabel("Num de Tópicos")
        plt.ylabel("Coherence score")
        plt.legend(("coherence_values"), loc='best')
        plt.show()
        for m, cv in zip(x, self.coherence_values):
            print("Num de Tópicos =", m, " valor coerência: ", round(cv, 4))
        
    def classificar_novo_texto(self, texto, model,id2word):
        lista_lematizada = self.pre_processar_texto_ou_lista(texto)
        novo_corpus = self.montar_novo_corpus(lista_lematizada,id2word)
        doc_bow = novo_corpus[0]
        topicos = model[doc_bow]
        
        #topicos_ordenados = sorted(topicos[0], key=lambda x: x[1], reverse=True)
        topicos_ordenados = sorted(topicos, key=lambda x: x[1], reverse=True)
        melhor_topico = topicos_ordenados[0]
        #print(topicos_ordenados)
        return melhor_topico, topicos_ordenados


    def montar_id2word(self, lista_documento_lematizada):
        id2word = corpora.Dictionary(lista_documento_lematizada)
        
        return id2word

    def montar_dict_models(self):
        dict_models = {
            "modelo": self.model_list,
            "coerencia":self.coherence_values,
            "num_topics": self.lista_num_topics
        }

        return dict_models

    def salvar_modelos(self, diretorio, folder_name):
        dict_models = self.montar_dict_models()
        df_models = pd.DataFrame(dict_models)

        folder_name = "{}\\{}".format(diretorio,folder_name)

        try:
            os.mkdir(folder_name)
        except OSError:
            print ("Erro na criação da pasta")
            return "erro"

       
        df_models['caminho'] = df_models.apply(lambda x: "{}\\#_{}".format(folder_name, str(x['num_topics'])), axis=1)

        for row in df_models.iterrows():
            row[1]['modelo'].save(row[1]['caminho'])


        df_models.drop(['modelo'], axis=1, inplace=True)
        dict_models = df_models.to_dict("records")

        return dict_models


    def retornar_melhor_modelo(self):
        
        dict_models = self.montar_dict_models()

        df_models = pd.DataFrame(dict_models)
        self.melhor_modelo = df_models.sort_values(by=['coerencia'], ascending=False).iloc[0]['modelo']
        melhor_coerencia = df_models.sort_values(by=['coerencia'], ascending=False).iloc[0]['coerencia']
        num_topicos = df_models.sort_values(by=['coerencia'], ascending=False).iloc[0]['num_topics']

        return self.melhor_modelo, melhor_coerencia, num_topicos
        
    def retornar_top_key_words(self, modelo, num_palavras=30):
        dict_palavras_topicos = {}

        for index, topic in modelo.show_topics(num_topics=-1,num_words=num_palavras,formatted=False):
            dict_words = {}
            for i, palavra in enumerate(topic,start=1):
                dict_words["palavra_{}".format(i)] = palavra[0]
                dict_words["prob_{}".format(i)] = float(palavra[1])
                #print("Palavra: {} - Peso: {}".format(palavra[0],palavra[1]))
            dict_words["topico"] = index
            dict_palavras_topicos["topico_"+str(index)] = dict_words     
        df_palavras = pd.DataFrame.from_dict(dict_palavras_topicos, orient='index')
        

        return df_palavras, dict_palavras_topicos


    def persistir_objeto_mongo(self, dict_dados):
        dict_dados['lista_coerencia'] = self.coherence_values
        dict_dados['palavras_melhor_modelo']

    def processar_df_topicos_probabilidade(self, df):
        '''busca os 4 principais tópicos e salva em colunas'''
        df['topico_1'] = df.apply(lambda x: x['lista_topicos'][0][0] ,axis=1)
        df['prob_1'] = df.apply(lambda x: x['lista_topicos'][0][1] ,axis=1)

        try:
            df['topico_2'] = df.apply(lambda x: int(x['lista_topicos'][1][0]) if len(x['lista_topicos']) > 1 else None  ,axis=1)
            df['prob_2'] = df.apply(lambda x: float(x['lista_topicos'][1][1]) if len(x['lista_topicos']) > 1 else None  ,axis=1)
        except:
            df['topico_2'] = None
            df['prob_2'] = None

        try:    
            df['topico_3'] = df.apply(lambda x: int(x['lista_topicos'][2][0]) if len(x['lista_topicos']) > 2 else None ,axis=1)
            df['prob_3'] = df.apply(lambda x: float(x['lista_topicos'][2][1]) if len(x['lista_topicos']) > 2 else None ,axis=1)
        except:
            df['topico_3'] = None
            df['prob_3'] = None

        try:
            df['topico_4'] = df.apply(lambda x: int(x['lista_topicos'][3][0]) if len(x['lista_topicos']) > 3 else None ,axis=1)
            df['prob_4'] = df.apply(lambda x: float(x['lista_topicos'][3][1]) if len(x['lista_topicos']) > 3 else None ,axis=1)
        except:
            df['topico_4'] = None
            df['prob_4'] = None


        return df