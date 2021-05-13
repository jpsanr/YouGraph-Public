from flask import Flask, render_template,  request, jsonify
from acessos import read, get_conn, persistir_uma_linha, persistir_multiplas_linhas, replace_df
from DAOs.dao_Canal import Canal
from models.youtube_extractor import Youtube_Extractor 
from connections.mysql_connector import MySQL_Connector
from models.topic_modeling import Topic_Modeling
from connections.mongodb_connector import Mongo_Connector
from connections.neo4j_connector import Neo4j_Connector
import os
from datetime import datetime
from gensim import corpora, models, similarities
from models.graph_generator import Graph_Generator
from models.tuple_extractor import Tuple_Extractor
import pickle
import numpy as np
import pandas as pd


mallet_path = "D:\Projetos\Mestrado\mallet-2.0.8"
os.environ['MALLET_HOME'] = mallet_path

mallet_path = "{}\\bin\mallet".format(mallet_path)


connector = MySQL_Connector("conn_orfeu")
#connector = MySQL_Connector("conn_orfeu_localhost")

nome_modelo = "09-11-2020 18_33_23"
folder_name = nome_modelo 


path = os.getcwd()
path_modelo = "{}\modelos_lda\{}".format(path, folder_name)
print(path_modelo)



def topicos_one_hot_enconding(lista_topicos): 
    dicionario_topicos = {}
    for item in lista_topicos:
        
        for key, valor in item: 
            dicionario_topicos.setdefault("topico_{}".format(key), []).append(valor) 
    
    df_one_hot = pd.DataFrame.from_dict(dicionario_topicos)
    return df_one_hot


def buscar_videos_transcripions(conn, idioma, transcription_type="", limit="", nome_modelo="09-11-2020 18_33_23"):
    transcription_language = "transcription_pt" if idioma =="pt-br" else "transcription_en"
    #query = "SELECT  video_id, {} as texto FROM video_transcriptions WHERE transcription_type <> 'Erro' order by 1  limit 8643, 11524   ".format(transcription_language)

    # if transcription_type != "":
    #     sub_query = " AND transcription_type = '{}'".format(transcription_type)
    #     query += sub_query  
    
    # if limit != "":
    #     sub_query = " LIMIT {}".format(limit)
    #     query += sub_query
    query = '''SELECT vt.video_id, transcription_pt as texto from video_transcriptions vt
LEFT JOIN (SELECT video_id from video_classifications where model = "{}") vc using (video_id) where vc.video_id is null and transcription_type <> 'Erro' limit 0,50'''.format(nome_modelo)

    df_videos_trans = read(conn, query)
    if df_videos_trans.empty != True:
        df_videos_trans.columns=['video_id', "texto"]

    return df_videos_trans


conn = connector.return_conn("influencer_br")

stop_words_list = ["aqui", "gente", "oi", "então", "ai", "aí", "ufa", "coisa", "[Aplausos]", "[Música]", "né","tá", "vídeo",
                   "casar", "coisa","canal", "tipo", "vezes", "mano", "meio", "ea", "eo", "só", "ae", "eis", "caraca", "caramba"
                  ]

topic_modeling = Topic_Modeling(language="pt-br", stop_words_list=stop_words_list)
print(" Pronto.")


print("-> Lendo Pickle")
with (open("{}\\dict_corpus.pickle".format(path_modelo), "rb")) as openfile:
    while True:
        try:
            dict_corpus = pickle.load(openfile)
        except EOFError:
            break
        


corpus = dict_corpus['corpus']
id2word = dict_corpus['id2word']

print("carregando modelo")
modelo = models.LdaModel.load("{}\#_{}".format(path_modelo,50))


df_palavras, _ = topic_modeling.retornar_top_key_words(modelo,30)
print(df_palavras.head())


print("-> Salvando")
df_palavras['model'] = nome_modelo
replace_df(df_palavras,"top_key_words", conn)


while True:
  
    conn = connector.return_conn("influencer_br")
    print(" --> Carregando Dados do Banco")
    df_base = buscar_videos_transcripions(conn, "pt-br", nome_modelo)
    print(" Pronto.")


    if df_base.empty:
        break
    
    df_base['lista_topicos'] = df_base.apply(
    lambda x: topic_modeling.classificar_novo_texto(x['texto'],modelo,id2word)[1], axis=1)


    df_processado = topic_modeling.processar_df_topicos_probabilidade(df_base)

    df_processado['model'] = nome_modelo
    df_processado.fillna(-1,  inplace=True)
    #print(df_processado.head())

    df_topicos_one_hot = topicos_one_hot_enconding(df_base["lista_topicos"])
    
    df_topicos_one_hot['video_id'] = df_processado['video_id']
    
    df_processado.drop(['texto'], axis=1, inplace=True)
    df_processado.drop(['lista_topicos'], axis=1, inplace=True)
    
    
    df_topicos_one_hot['model'] = nome_modelo
    
    replace_df(df_topicos_one_hot,"video_classifications_one_hot_encoding", conn)
    replace_df(df_processado,"video_classifications", conn)
    conn.close()



print("FIMMMMMMMMMM")





