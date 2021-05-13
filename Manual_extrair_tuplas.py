from connections.mysql_connector import MySQL_Connector
from models.topic_modeling import Topic_Modeling
from connections.neo4j_connector import Neo4j_Connector
import os
from datetime import datetime
from gensim import corpora, models, similarities
from models.graph_generator import Graph_Generator
from models.tuple_extractor import Tuple_Extractor
from acessos import read, get_conn, persistir_uma_linha, persistir_multiplas_linhas, replace_df
import pandas as pd
import re

import credentials

import warnings
warnings.filterwarnings('ignore')

connector = MySQL_Connector("conn_orfeu")
conn = connector.return_conn("influencer_br")

neo4j_client = Neo4j_Connector(credentials.neo4j_uri, credentials.neo4j_user, credentials.neo4j_password)
graph_generator = Graph_Generator(neo4j_client)

tuple_extractor = Tuple_Extractor()

def gerar_tuplas(texto):
    tuple_extractor.extrair_tupla(texto)
    df_tuplas = tuple_extractor.get_ultimas_tuplas_geradas()
    
    return df_tuplas

def gerar_df_doc(texto, video_id=""):
    if video_id !="":
        data =  {'nome': [texto], 'type': ["DOC"], 'id': [video_id]}
    else:
        data =  {'nome': [texto], 'type': ["DOC"]}
    
    df_doc = pd.DataFrame(data=data)
    return df_doc

def pre_processar_tuplas(df_tuplas):
    df_tuplas['arg1'] = df_tuplas.apply(lambda x: graph_generator.pre_process_text(x['arg1']), axis=1) 
    df_tuplas['arg2'] = df_tuplas.apply(lambda x: graph_generator.pre_process_text(x['arg2']), axis=1) 
    df_tuplas['rel'] = df_tuplas.apply(lambda x: graph_generator.pre_process_text(x['rel'], rel=True), axis=1) 
    
    return df_tuplas

def get_df_sentencas(df_tuplas):
    df_sentencas = pd.DataFrame()
    df_sentencas['nome'] = df_tuplas['sentenca']#.to_frame()
    df_sentencas['type'] = "SENTENÇA"
    
    return df_sentencas

def processar_df_tuplas(df_tuplas):
    
    df_arg1 = df_tuplas[['arg1']]
    df_arg1['type'] = "ARG1"
    df_arg1 = df_arg1.rename(columns={"arg1": 'nome'})
    
    
    df_arg2 = df_tuplas[['arg2']]
    df_arg2['type'] = "ARG2"
    df_arg2 = df_arg2.rename(columns={"arg2": 'nome'})
    
    return df_arg1, df_arg2

def gerar_relacao_doc_sentenca(texto, df_sentencas):
    #ATENÇÃO usar somente após a criação dos nós sentenças e doc
    for sentenca in df_sentencas.iterrows():
        neo4j_client.criar_relacao_de_para(texto, sentenca[1]['nome'], "DOC", sentenca[1]['type'], "extraiu")

def gerar_relacao_arg1_rel_arg2(df_tuplas):
    #ATENÇÃO usar somente após a criação dos nós arg1, arg2 e rel
    for linha in df_tuplas.iterrows():
        relacao = "RELAÇÃO_TUPLA {rel:'" + linha[1]["rel"] + "'}"
        neo4j_client.criar_relacao_de_para(linha[1]["arg1"], linha[1]['arg2'], "ARG1", "ARG2", relacao)
        

def gerar_relacao_sentenca_arg1(df_tuplas): #estou chamando de relação cabeça
    for linha in df_tuplas.iterrows():
        neo4j_client.criar_relacao_de_para(linha[1]["sentenca"], linha[1]['arg1'], "SENTENÇA", "ARG1", "extraiu")
        
def processar_texto_entrada(texto): #Remover emojis tag [Musica]... e as aspas
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U0001F1F2-\U0001F1F4"  # Macau flag
        u"\U0001F1E6-\U0001F1FF"  # flags
        u"\U0001F600-\U0001F64F"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U0001F1F2"
        u"\U0001F1F4"
        u"\U0001F620"
        u"\u200d"
        u"\u2640-\u2642"
        "]+", flags=re.UNICODE)
    
    texto = re.sub(r"\[.*?\]\s*", "", texto)
    texto = texto.replace("'", "")
    texto = texto.replace('"', "")
    texto = texto.replace('\n', " ")
    texto = emoji_pattern.sub(r'', texto)
    
    return texto

def buscar_videos_transcripions(conn, idioma, transcription_type="",start="", limit=""):
    transcription_language = "transcription_pt" if idioma =="pt-br" else "transcription_en"
    query = "SELECT  video_id, {} as texto FROM video_transcriptions WHERE transcription_type <> 'Erro'".format(transcription_language)

    if transcription_type != "":
        sub_query = " AND transcription_type = '{}'".format(transcription_type)
        query += sub_query
    
    if limit != "":
        sub_query = " LIMIT {} ,{}".format(start, limit)
        query += sub_query

    df_videos_trans = read(conn, query)
    df_videos_trans.columns=['video_id', "texto"]

    return df_videos_trans

start = 120
limit = 50
while True:

	neo4j_client = Neo4j_Connector('bolt://orfeu.ppgia.pucpr.br:12102', 'neo4j', 'WQnSbTcDa$X4')
	graph_generator = Graph_Generator(neo4j_client)


	df_transc = buscar_videos_transcripions(conn, "pt-br", "manual",start, limit=limit)
	start = start + limit

	if df_transc.empty:
	    break
	print(" --> Buscando transcrições")
	df_transc['texto'] = df_transc.apply(lambda x: processar_texto_entrada(x["texto"]), axis=1) 
	print(df_transc)

	query_neo = '''
	    MATCH (n:DOC)
	    RETURN DISTINCT n.id as id
	'''
	print(" --> Buscando ids neo4j")
	result_neo = neo4j_client.run_raw_query(query_neo)

	lista_ids = []
	for linha in result_neo:
	    lista_ids.append(linha['id'])

	print(" --> Iterando...")
	for linha in df_transc.iterrows():
	    texto = linha[1]['texto']
	    video_id = linha[1]['video_id']
	    
	    if video_id not in lista_ids:
	        try:
	            df_doc = gerar_df_doc(texto, video_id=video_id)

	            df_tuplas = gerar_tuplas(texto)
	            df_tuplas = pre_processar_tuplas(df_tuplas)

	            df_sentencas = get_df_sentencas(df_tuplas)

	            # persistindo documento
	            neo4j_client.add_nodes(df_doc)

	            # persistindo sentenças
	            neo4j_client.add_nodes(df_sentencas)

	            #persistindo relação doc-sentença
	            gerar_relacao_doc_sentenca(texto, df_sentencas)

	            #Persistindo argumentos 
	            df_arg1, df_arg2= processar_df_tuplas(df_tuplas)
	            neo4j_client.add_nodes(df_arg1)
	            neo4j_client.add_nodes(df_arg2)


	            #persistindo relação cabeça sentenca-arg1
	            gerar_relacao_sentenca_arg1(df_tuplas)

	            #persistindo relacao arg1-rel-arg2
	            gerar_relacao_arg1_rel_arg2(df_tuplas)
	        except:
	            print("Erro no video: {}".format(video_id))
