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

import credentials

class Corpus_Assesor_Singleton: #Gambiarra singleton para armazenar os corpus entre transações stateless

    def set_data(self, dict_data):
        self.dict_data = dict_data

    def get_data(self):
        return self.dict_data


app = Flask(__name__)

@app.route('/')
def index():
    return render_template('template.html',page="config")

@app.route('/config', methods=['GET', 'POST'])
def view_config():
    return render_template('template.html', page="config")

@app.route('/dados_base', methods=['GET', 'POST'])
def dados_base():
    return render_template('template.html', page="dados_base")

@app.route('/coletar_dados', methods=['GET', 'POST'])
def view_coletar_dados():
    conn = connector.return_conn(banco_padrao)

    df_canais = buscar_todos_canais(conn)
    
    conn.close()
    return render_template('template.html', page="coletar_dados/coletar_dados", canais=df_canais)

@app.route('/add_novo_canal', methods=['GET', 'POST'])
def add_novo_canal():
    conn = connector.return_conn(banco_padrao)
    
    if request.method == 'POST':  
        dict_post= {
            "url_channel":          request.form.get('url_channel'),
            "channel_id":           request.form.get('channel_id'),
            "category":             request.form.get('category'),   
            "gender":               request.form.get('gender'),
            "channel_name":         request.form.get('channel_name'),
            "country":              request.form.get('country'),
            "year":                 request.form.get('year'),
            "channel_description":  request.form.get('channel_description'),
            "comment_count":        request.form.get('comment_count'),
            "video_count":          request.form.get('video_count'),
            "subscriber_count":     request.form.get('subscriber_count'),
            "aux_status":           request.form.get('aux_status'),
            "view_count":           request.form.get('view_count')
    }

  
    canal = Canal(dict_post,conn)
    canal.adicionar_canal()
    conn.close()
    return "sucesso"

@app.route('/extrair_videos', methods=['GET', 'POST'])
def extrair_videos():
    conn = connector.return_conn(banco_padrao)

    if request.method == 'POST':  
        channel_id = request.form.get('channel_id')
        extraction_order = request.form.get('extraction_order')
        num_videos_extracao = int(request.form.get('num_videos_extracao'))

        page_token = None
        while num_videos_extracao >= 0:
            print("Processando o Page Token {}".format(page_token))
            df_final, page_token = youtube_extractor.gerar_df_videos_completo(channel_id, 
                                                                            extraction_order, 
                                                                            page_token=page_token,
                                                                            max_results=max_results)
            
            if df_final != None:
            
                youtube_extractor.persistir_videos(df_final, conn)

                df_transcricao =  youtube_extractor.montar_df_transcricao(df_final, primeiro_idioma="pt", segundo_idioma="en")

                youtube_extractor.persistir_transcricoes(df_transcricao, conn)

            if page_token == "":
                print("--> Todos os vídeos do canal foram extraídos!")
                break

            youtube_extractor.update_channel_status_and_token(channel_id, page_token, 0,conn)
            num_videos_extracao = num_videos_extracao - max_results
            print("Faltam {} para processar!".format(num_videos_extracao))
        youtube_extractor.update_channel_status_and_token(channel_id, page_token, 1,conn)

    conn.close()
    return "sucesso"

@app.route('/get_token_api', methods=['GET', 'POST'])
def get_token_api():
    status_auth = youtube_extractor.get_token_acesso()
    return status_auth

@app.route('/atualizar_canais', methods=['GET', 'POST'])
def atualizar_canais():
    conn = connector.return_conn(banco_padrao)

    youtube_extractor.atualizar_dados_canais(conn)

    conn.close()
    return "sucesso" 


##  ROTAS TOPIC MODELING ##

@app.route('/tm', methods=['GET', 'POST'])
def view_tm():
    conn = connector.return_conn()

    df_corpus = buscar_corpus_disponiveis(conn)

    return render_template('template.html', page="modelagem_topicos/tm", corpus_disponiveis=df_corpus)


@app.route('/treinar_modelo', methods=['GET', 'POST'])
def treinar_modelo():
    
    date_string = datetime.now().strftime("%d-%m-%Y %H_%M_%S")
    
    if request.method == 'POST':  
        dict_post = {
            "corpus_name":               request.form.get('corpus'),
            "idioma":               request.form.get('idioma'),
            "lista_stop_words":     request.form.getlist('lista_stop_words[]'),
            "lista_pos_tag":        request.form.getlist('lista_pos_tag[]'),
            "min_count":            int(request.form.get('min_count')),
            "threshold":            int(request.form.get('threshold')),
            "corencia":             int(request.form.get('corencia')),
            "filtro_ner":           True if request.form.get('filtro_ner') == "sim" else False,
            "num_topicos_inicial":  int(request.form.get('num_topicos_inicial')),
            "step":                 int(request.form.get('step')),
            "num_max_topicos":      int(request.form.get('num_max_topicos')),
            "date": date_string
    }

    conn = connector.return_conn(dict_post['corpus_name'])
    
    
    print(" --> Carregando Modelos")
    topic_modeling = Topic_Modeling(language=dict_post['idioma'])
    print(" Pronto.")

    print(" --> Carregando Dados do Banco")
    df_videos_trans = buscar_videos_transcripions(conn, dict_post['idioma'], limit=40)
    print(" Pronto.")

    print(" --> Iniciando Treinamento")
    melhor_modelo, melhor_coerencia, num_topicos_modelo, palavras_dict, n_grams, df_palavras = pipeline_topic_modeling(topic_modeling, 
                                                                                                    df_videos_trans , 
                                                                                                    dict_post['filtro_ner'],
                                                                                                    dict_post['num_topicos_inicial'],
                                                                                                    dict_post['step'],
                                                                                                    dict_post['num_max_topicos'],
                                                                                                    dict_post['corpus_name'],
                                                                                                    date_string,
                                                                                                    dict_post['lista_pos_tag'])

    print(" Pronto.")

    dict_post["melhor_coerencia"] = melhor_coerencia
    dict_post["num_topicos_melhor_modelo"] = num_topicos_modelo
    dict_post["palavras_melhor_modelo"] = palavras_dict
    dict_post['n_grams'] = n_grams


    path = os.getcwd()
    path_modelos = "{}\modelos_lda".format(path)
    dict_modelos_caminho = topic_modeling.salvar_modelos(path_modelos, date_string)

    dict_post['modelos'] = dict_modelos_caminho

    # with open("{}\\{}\\backup.pickle".format(path_modelos, date_string), 'wb') as handle: #SALVANDO DICT EM UM PICKLE
    #    pickle.dump(dict_post, handle, protocol=pickle.HIGHEST_PROTOCOL)


    # with open("{}\\{}\\corpus.pickle".format(path_modelos, date_string), 'wb') as handle: #SALVANDO DICT EM UM PICKLE
    #    pickle.dump(corpus_assesor.get_data(), handle, protocol=pickle.HIGHEST_PROTOCOL)

    print(" --> Salvando Dados no mongo")
    mongo_client.persistir_dados(dict_post, "modelos")


    conn = connector.return_conn(dict_post['corpus_name'])
    replace_df(df_palavras,"top_key_words", conn)
    conn.close()

    return {"melhor_coerencia": float(melhor_coerencia), "num_topicos": int(num_topicos_modelo), "data_modelo":date_string}

@app.route('/classificar_corpus', methods=['GET', 'POST'])
def classificar_corpus():
    
    dict_dados_corpus = corpus_assesor.get_data()

    df_base = dict_dados_corpus['df_corpus']
    topic_modeling = dict_dados_corpus['topic_modeling'] 
    corpus_name = dict_dados_corpus["corpus_name"]
  
    if request.method == 'POST':
        date = request.form.get('date')
        num_topicos = request.form.get('num_topicos')

    path = os.getcwd()
    path_modelo = "{}\modelos_lda\{}".format(path, date)

    modelo = models.LdaModel.load("{}\#_{}".format(path_modelo,num_topicos))
    id2word = models.LdaModel.load("{}\#_{}.id2word".format(path_modelo,num_topicos))


    df_base['lista_topicos'] = df_base.apply(
    lambda x: topic_modeling.classificar_novo_texto(
        x['texto'],modelo,id2word)[1], axis=1)


    df_processado = topic_modeling.processar_df_topicos_probabilidade(df_base)
    df_processado.drop(['texto'], axis=1, inplace=True)
    df_processado.drop(['lista_topicos'], axis=1, inplace=True)
    df_processado.fillna(-1,  inplace=True)
    df_processado['model'] = date
   
    conn = connector.return_conn(corpus_name)
    replace_df(df_processado,"video_classifications", conn)
    conn.close()
    
    return "sucesso"

##  FIM TOPIC MODELING ##

## ROTAS GRAFO DE CONHECIMENTO ##

@app.route('/carregar_modelos_tm', methods=['GET', 'POST'])
def carregar_modelos_tm():
    if request.method == 'POST':  
        base_dados = request.form.get('corpus')
       
    conn = connector.return_conn(base_dados)

    df_modelos = buscar_modelos_disponiveis(conn)
   
    return df_modelos.to_dict('index')


@app.route('/grafo', methods=['GET', 'POST'])
def view_grafo():
    conn = connector.return_conn()

    df_corpus = buscar_corpus_disponiveis(conn)
   
    return render_template('template.html', page="grafos/grafo", corpus_disponiveis=df_corpus)

@app.route('/gerar_grafo_ner', methods=['GET', 'POST'])
def gerar_grafo_ner():
    print("  --> Extraindo NERs")
    if request.method == 'POST':  
        base_dados = request.form.get('corpus')
        transcription_type = request.form.get("transcription_type")
        idioma = request.form.get("idioma")
    conn = connector.return_conn(base_dados)

    print(" --> Buscando transcrições no banco...")
    df_video_trans = buscar_videos_transcripions(conn, idioma, transcription_type)
    

    # graph_generator = Graph_Generator(neo4j_client)

    graph_generator.persistir_ners(df_video_trans)

    return "sucesso"

@app.route('/gerar_grafo_metadado', methods=['GET', 'POST'])
def gerar_grafo_metadado():
    
    if request.method == 'POST':  
        base_dados = request.form.get('corpus')
        transcription_type = request.form.get("transcription_type")
    conn = connector.return_conn(base_dados)

    print(" --> Consultando Mysql Canais")
    df_channels = read(conn, "SELECT * FROM channels")
    df_channels['nome'] = df_channels['channel_name']
    df_channels['type'] = "canal"

    
    df_channels.drop(['updated_at'], axis=1, inplace=True)
    df_channels.drop(['next_page_token'], axis=1, inplace=True)
    
    print(" --> Consultando Mysql Videos")
    query ='''
        SELECT * from videos vi 
        INNER JOIN video_transcriptions vt using(video_id) 
            WHERE transcription_type<>"Erro" AND transcription_type = '{}'
    '''.format(transcription_type) 

    df_videos_transcriptions = read(conn, query)
    df_videos_transcriptions['nome'] = df_videos_transcriptions['video_id']
    df_videos_transcriptions['type'] = "video"

    df_videos_transcriptions.drop(['updated_at'], axis=1, inplace=True)
    df_videos_transcriptions['published_at'] = df_videos_transcriptions['published_at'].astype(str)

    print(" --> Persistindo Canais no Neo4j")
    neo4j_client.add_nodes(df_channels)
    print("Pronto!")

    print(" --> Persistindo Vídeos no Neo4j")
    neo4j_client.add_nodes(df_videos_transcriptions)
    print("Pronto!")

    print(" --> Criando Relações")
    for row in df_videos_transcriptions.iterrows():
        neo4j_client.criar_relacao_de_para(row[1]['channel_name'], row[1]['video_id'], "canal", "video", "publicou")
    print("Pronto!")

    return "sucesso"


@app.route('/gerar_grafo_topicos_latentes', methods=['GET', 'POST'])
def gerar_grafo_topicos_latentes():
    if request.method == 'POST':  
        base_dados = request.form.get('corpus')
        transcription_type = request.form.get("transcription_type")
        model = request.form.get("model")
    conn = connector.return_conn(base_dados)

    df_key_words = buscar_topicos_lda(conn, model)
    df_video_classifications = buscar_videos_classifications(conn, model)

    graph_generator.persistir_topicos_latentes(df_key_words, df_video_classifications)
    return "sucesso"

@app.route('/grafo_tuplas_conhecimento', methods=['GET', 'POST'])
def gerar_grafo_tuplas_conhecimento():
    
    if request.method == 'POST':  
        base_dados = request.form.get('corpus')
        transcription_type = request.form.get("transcription_type")
        idioma = request.form.get("idioma")
    conn = connector.return_conn(base_dados)

    print(" --> Buscando transcrições no banco...")
    df_video_trans = buscar_videos_transcripions(conn, idioma, transcription_type)
    
    graph_generator.persistir_tuplas_conhecimento(df_video_trans)


    return "sucesso"

## FIM ROTAS GRAFOS ##

def buscar_topicos_lda(conn, model):
    query = "SELECT * , CONCAT(model, '-topico_' , topico) as nome FROM top_key_words WHERE model = '{}'".format(model)
    print(query)
    df_key_words = read(conn, query)
    return df_key_words

 
def buscar_videos_classifications(conn, model):
    query = "SELECT * from video_classifications WHERE model='{}'".format(model)
    df_video_classifications = read(conn, query)

    return df_video_classifications

def buscar_modelos_disponiveis(conn):

    query = "SELECT distinct model from video_classifications;"
    df_modelos = read(conn, query)
    return df_modelos


def buscar_corpus_disponiveis(conn):
    query= "SHOW DATABASES LIKE '%%influencer%%'"
    df_corpus = read(conn, query)
    df_corpus.columns=['base']
    return df_corpus

def buscar_todos_canais(conn):
    query = "SELECT url_channel, channel_id, channel_name FROM channels"
    df_todos_canais = read(conn, query)
    return df_todos_canais

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

def pipeline_topic_modeling(topic_modeling, df_base, filtro_ner, num_topicos_inicial, step, num_max_topicos, corpus_name, date_string, allowed_postags):
    
    print("     --> lista_documentos")
    lista_documentos = df_base['texto'].tolist()
    
    ### Pré Processamento
    print("     --> Gerando Corpus")
    lista_documento_lematizada = topic_modeling.pre_processar_texto_ou_lista(lista_documentos,filtro_ner=filtro_ner, allowed_postags=allowed_postags)
    id2word = topic_modeling.montar_id2word(lista_documento_lematizada)
    corpus = topic_modeling.montar_novo_corpus(lista_documento_lematizada, id2word)
    

    ####  
    
    print("     --> Treinando!!")
    model_list, coherence_values, _ = topic_modeling.gerar_multiplos_modelos(id2word, 
                                                                            corpus, 
                                                                            lista_documento_lematizada, 
                                                                            limit=num_max_topicos, 
                                                                            start=num_topicos_inicial, 
                                                                            step=step)

    print("     --> Coerencias...")
    
    print(coherence_values)

    melhor_modelo, melhor_coerencia, num_topicos_modelo = topic_modeling.retornar_melhor_modelo()



    df_palavras, dict_palavras_topicos = topic_modeling.retornar_top_key_words(melhor_modelo)
    df_palavras['model'] = date_string
    
    gram_tuple = topic_modeling.get_n_grams()
    n_grams = {
        "bi_gram": gram_tuple[0],
        "tri_gram": gram_tuple[1] 
    }

    dict_dados_corpus = {
        "lista_documento_lematizada": lista_documento_lematizada,
        "id2word": id2word,
        "corpus": corpus,
        "df_corpus": df_base,
        "topic_modeling": topic_modeling,
        "corpus_name": corpus_name 
    }
    corpus_assesor.set_data(dict_dados_corpus)

    return melhor_modelo, melhor_coerencia, num_topicos_modelo, dict_palavras_topicos, n_grams, df_palavras

if __name__ == '__main__':
    banco_padrao = "influencer_br"
    max_results=50


    connector = MySQL_Connector("conn_orfeu")
    #connector = MySQL_Connector("conn_orfeu_localhost")

    youtube_extractor = Youtube_Extractor()

    mongo_client = Mongo_Connector(credentials.mongo_user, credentials.mongo_password)

    corpus_assesor = Corpus_Assesor_Singleton()

    neo4j_client = Neo4j_Connector(credentials.neo4j_uri, credentials.neo4j_user, credentials.neo4j_password)
    # neo4j_client = Neo4j_Connector(credentials.neo4j_localhost_uri, credentials.neo4j_localhost_user, credentials.neo4j_localhost_password)
    graph_generator = Graph_Generator(neo4j_client)

    

    print("Ready to use! :D")
    app.run(debug=True)
    #app.run(debug=False)


