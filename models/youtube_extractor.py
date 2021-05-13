import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from tqdm import tqdm
import pandas as pd
import json
from youtube_transcript_api import YouTubeTranscriptApi
from acessos import read, get_conn, persistir_uma_linha, persistir_multiplas_linhas, persistir_banco, replace_df
import os
from os import walk
from textblob import TextBlob


class Youtube_Extractor():
    
    def __init__(self):
        self.lista_credenciais = self.obter_lista_credenciais()
             
    def get_token_acesso(self):
        SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
        API_SERVICE_NAME = 'youtube'
        API_VERSION = 'v3'
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

        CLIENT_SECRETS_FILE = self.lista_credenciais[0]


        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        credentials = flow.run_local_server()
        status_auth = "Falha"
        try: 
            self.service = build(API_SERVICE_NAME, API_VERSION, credentials=credentials)
            status_auth = "Sucesso"
        except Exception as e:
            print("Algo deu errado na autenticação")
            print(e)
        return status_auth
        
    def obter_lista_credenciais(self):
        dir_atual = os. getcwd()
        dir_credencial = "{}\credentials".format(dir_atual)
    
        lista_arquivos = []
        for (dirpath, dirnames, filenames) in walk(dir_credencial):
            for filename in filenames:
                arquivo_dir = "{}\{}".format(dir_credencial,filename)
                lista_arquivos.append(arquivo_dir)
            break

        return lista_arquivos

    def buscar_videos_snippet(self, channel_id, key_words=None, max_results=50, extraction_order="date", page_token=None):
        
        query_results = self.service.search().list(
            part = 'snippet',
            q = key_words,
            channelId = channel_id,
            order = extraction_order, #'relevance' or 'date'
            maxResults = max_results,
            type = 'video', # Channels might appear in search results
            relevanceLanguage = 'en',
            safeSearch = 'moderate',
            pageToken = page_token
            ).execute()

        return query_results

    def processar_conteudo_video(self, df_videos):
        lista_videos_conteudo = []

        for row in df_videos.iterrows():
            conteudo_dict = self.buscar_video_content_details(row[1]['video_id'])
            lista_videos_conteudo.append(conteudo_dict)

        df_conteudo = pd.DataFrame(lista_videos_conteudo)
        return df_conteudo

    def buscar_video_content_details(self, video_id):
                
        results = self.service.videos().list(
                part='snippet,contentDetails,statistics',
                id = video_id
            ).execute()
    
        dict_results = {
            "video_id": video_id,
            "comment_count": (results['items'][0]['statistics']["commentCount"]) if( "commentCount" in results['items'][0]['statistics']) else  -999,
            "favorite_count":(results['items'][0]['statistics']['favoriteCount']) if( 'favoriteCount' in results['items'][0]['statistics']) else  -999,
            "view_count": results['items'][0]['statistics']['viewCount'] if( "viewCount" in results['items'][0]['statistics']) else  -999,
            'like_count': results['items'][0]['statistics']['likeCount'] if( "likeCount" in results['items'][0]['statistics']) else  -999,
            'dislike_count': results['items'][0]['statistics']['dislikeCount'] if( "dislikeCount" in results['items'][0]['statistics']) else  -999,
            'video_duration': results['items'][0]['contentDetails']['duration'] if( "duration" in results['items'][0]['contentDetails']) else  -999,
            'published_at': results['items'][0]['snippet']['publishedAt']
        }
        list_results = [dict_results]
        df_video_content = pd.DataFrame(list_results)
        
        return dict_results

    def processar_json_videos(self, channelId, query_results):

        video_id = []
        channel = []
        video_title = []
        video_desc = []
        channel_id = []   
        
        for item in query_results['items']:
            video_id.append(item['id']['videoId'])
            channel.append(item['snippet']['channelTitle'])
            video_title.append(item['snippet']['title'])
            video_desc.append(item['snippet']['description'])
            channel_id.append(channelId)    

        output_dict = {
        'video_id': video_id,
        'channel_id': channel_id,    
        'channel_name': channel,
        'video_title': video_title,
        'video_desc': video_desc,
        }
        
        page_token = (query_results["nextPageToken"] if( "nextPageToken" in query_results) else  "")
        


        df_videos = pd.DataFrame(output_dict, columns = output_dict.keys())
        return df_videos, page_token

    # def processar_transcricoes(self, df_videos, calcular_valencia=False, calcular_subjetividade=False, language_code="pt"):
        
    #     df_videos['transcription'] = df_videos.apply(lambda x: self.get_video_transcription(x['video_id'], language_code) , axis=1)
    #     return df_videos

    # def get_video_transcription(self, video_id, language_code): 
        
    #     try:
    #         json_bruto = YouTubeTranscriptApi.get_transcript(video_id, languages=[language_code])
    #         transcricao = self.process_json_transcription(json_bruto)
    #     except Exception as e:
    #         transcricao = "** ERRO NA TRANSCRIÇÃO **"
    #         print("Erro no download da transcrição")
    #         #print(e)
    #     return transcricao

    def montar_df_transcricao(self, df_videos, primeiro_idioma="pt", segundo_idioma="en"):
  
        list_dict = []
        for row in df_videos.iterrows():
            video_id = row[1]['video_id']
            #transcription_pt = row[1]['transcription_pt']

            print("Processando ID {}".format(video_id))

            dict = self.processar_transcricao(video_id, primeiro_idioma="pt", segundo_idioma="en")
            list_dict.append(dict)

            df_transcricoes = pd.DataFrame(list_dict)
        
        return df_transcricoes

    def processar_transcricao(self, video_id, primeiro_idioma="pt", segundo_idioma="en"):

        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id, [primeiro_idioma])
            transcript = transcript_list.find_transcript([primeiro_idioma])
            transcricao = self.process_json_transcription(transcript.fetch())
            transcription_type = "automatica" if transcript.is_generated == True else "manual"
        except:
            transcricao = "** ERRO NA TRANSCRIÇÃO **"
            transcription_type = "Erro"

        coluna_primeiro_idioma = "transcription_{}".format(primeiro_idioma)
        coluna_segundo_idioma = "transcription_{}".format(segundo_idioma)

        try:
            transcricao_traduzida = self.process_json_transcription(transcript.translate(segundo_idioma).fetch())
        except:
            transcricao_traduzida = "*** Sem Tradução ***"

        dict_data = {
            "video_id": video_id,
            "transcription_type": transcription_type,
            "transcription_original_language": primeiro_idioma,
            coluna_primeiro_idioma: transcricao,
            coluna_segundo_idioma: transcricao_traduzida,

        }
        
        return dict_data


    def process_json_transcription(self, json):
        transcription_text = ""
        for elemento_texto in json:
            transcription_text +=" {}".format(elemento_texto['text'])
        return transcription_text
    
    def persistir_videos(self, df, conn):
        replace_df(df,"videos", conn)

    def persistir_transcricoes(self, df, conn):
        replace_df(df,"video_transcriptions", conn)
        

    def gerar_df_videos_completo(self, channel_id, extraction_order, max_results, page_token=None):
        print("--> Buscando Snippets dos Vídeos")
        json_videos = self.buscar_videos_snippet(channel_id, max_results=max_results, extraction_order=extraction_order, page_token=page_token)
        print("--> Processando Json dos Snippets")
        df_videos, page_token = self.processar_json_videos(channel_id, json_videos) 
        # print("--> Gerando Transcrições")
        # df_videos = self.processar_transcricoes(df_videos)
        

        if df_videos.empty is False: #dataframe não é vazio
            print("--> Gerando DF de conteudo")
            df_conteudo = self.processar_conteudo_video(df_videos)
        
            print("--> Mergeando os DFs")
            df_final = pd.merge(df_videos, df_conteudo, on='video_id')
        else:
            print("DF vazio e nada será feito.")
            df_final = df_videos #passa um df vazio

        return df_final, page_token

    def update_channel_status_and_token(self, channel_id, page_token, aux_status,conn):
        query = "UPDATE `channels` SET `aux_status` = %s, `next_page_token` = %s WHERE (`channel_id` = %s)"
        
        data_channel_next_page = {'aux_status':[aux_status], 'next_page_token':[page_token], 'channel_id':[channel_id]} 
        df_channel_next_page = pd.DataFrame(data_channel_next_page) 
        
        persistir_banco(df_channel_next_page, query, conn)

    def atualizar_dados_canais(self, conn):
        df_canais = self.buscar_lista_canais(conn)

        for c in df_canais.iterrows():
            channel_id = c[1]['channel_id']
            print("---> Processando canal: {}".format(channel_id))
            df_channel = self.get_channel_data(channel_id)
            self.update_channel_data(df_channel, conn)

        
    
    def buscar_lista_canais(self, conn):
        query = "SELECT channel_name, channel_id FROM channels"
        return read(conn,query)

    def get_channel_data(self, channel_id):
        results = self.service.channels().list(
            part='snippet,contentDetails,statistics',
            id = channel_id
        ).execute()

        dict_results = {
            "channel_id": channel_id,
            "channel_description": results['items'][0]['snippet']['description'],
            "comment_count": (results['items'][0]['statistics']["commentCount"]) if( "commentCount" in results['items'][0]['statistics']) else  -999,
            "video_count":results['items'][0]['statistics']['videoCount'],
            "view_count": results['items'][0]['statistics']['viewCount'],
            'subscriber_count': results['items'][0]['statistics']['subscriberCount']
        }
        list_results = [dict_results]
        df = pd.DataFrame(list_results)
        return df


    def update_channel_data(self, df_channel, conn):
        query = "UPDATE `channels` SET `channel_description` = %s, `comment_count` = %s, `video_count` = %s, `view_count` = %s, `subscriber_count` = %s WHERE (`channel_id` = %s)"
        df_channel = df_channel[[ 'channel_description', 'comment_count','video_count', 'view_count', 'subscriber_count', 'channel_id']]
        persistir_banco(df_channel, query, conn)


    def process_sentiment(self, text):
        tb = TextBlob(text)
        return tb.sentiment.polarity, tb.sentiment.subjectivity



