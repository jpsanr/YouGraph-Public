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




connector = MySQL_Connector("conn_orfeu")


def buscar_videos_transcripions(conn, idioma, transcription_type="", limit=""):
    transcription_language = "transcription_pt" if idioma =="pt-br" else "transcription_en"
    query = "SELECT  video_id, {} as texto FROM video_transcriptions WHERE transcription_type <> 'Erro'".format(transcription_language)

    if transcription_type != "":
        sub_query = " AND transcription_type = '{}'".format(transcription_type)
        query += sub_query
    
    if limit != "":
        sub_query = " LIMIT {}".format(limit)
        query += sub_query

    df_videos_trans = read(conn, query)
    df_videos_trans.columns=['video_id', "texto"]

    return df_videos_trans










conn = connector.return_conn("influencer_br")
date_string = datetime.now().strftime("%d-%m-%Y %H_%M_%S")


stop_words_list = ["aqui", "gente", "oi", "então", "ai", "aí", "ufa", "coisa" ]
topic_modeling = Topic_Modeling(language="pt-br",stop_words_list=stop_words_list)
print(" Pronto.")

print(" --> Carregando Dados do Banco")
df_videos_trans = buscar_videos_transcripions(conn, "pt-br")
print(" Pronto.")


print(df_videos_trans.head())



lista_documentos = df_videos_trans['texto'].tolist()

print("     --> Gerando Corpus")
allowed_postags = ["NOUN","PROPN"]

print("Lematizando")
lista_documento_lematizada = topic_modeling.pre_processar_texto_ou_lista(lista_documentos,filtro_ner=False,allowed_postags=allowed_postags)
print("Id2word")
id2word = topic_modeling.montar_id2word(lista_documento_lematizada)
print("Gerando corpus")
corpus = topic_modeling.montar_novo_corpus(lista_documento_lematizada, id2word)
    

path = os.getcwd()
path_modelos = "{}\modelos_lda".format(path)


dict_corpus = {
    "lista_documentos":lista_documento_lematizada,
    "id2word":id2word,
    "corpus":corpus

}

save_path = "{}\\{}".format(path_modelos, date_string)
os.mkdir(save_path) 
with open("{}\\dict_corpus.pickle".format(save_path), 'wb') as handle: #SALVANDO DICT EM UM PICKLE
    pickle.dump(dict_corpus, handle, protocol=pickle.HIGHEST_PROTOCOL)





# print(" --> Iniciando Treinamento")
# melhor_modelo, melhor_coerencia, num_topicos_modelo, palavras_dict, n_grams, df_palavras = pipeline_topic_modeling(topic_modeling, 
#                                                                                                 df_videos_trans , 
#                                                                                                 dict_post['filtro_ner'],
#                                                                                                 dict_post['num_topicos_inicial'],
#                                                                                                     dict_post['step'],
#                                                                                                     dict_post['num_max_topicos'],
#                                                                                                     dict_post['corpus_name'],
#                                                                                                     date_string)