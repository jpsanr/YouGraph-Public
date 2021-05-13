from acessos import read, get_conn, persistir_uma_linha, persistir_multiplas_linhas
import pandas as pd

class Canal():
    
    def __init__(self, dados_canal, conn):
        self.df_canal = pd.DataFrame([dados_canal])
        
        self.conn = conn
    

    def adicionar_canal(self):
        query = "INSERT INTO `channels` (`url_channel`, `channel_id`, `category`, `gender`, `channel_name`, `country`, `year`, `channel_description`,`comment_count`, `video_count`, `view_count`, `subscriber_count`, `aux_status`, `updated_at`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp());"
        
        self.df_canal = self.df_canal[['url_channel', 'channel_id', 'category', 'gender', 'channel_name', 'country',
                                        'year', 'channel_description','comment_count', 'video_count', 'view_count',
                                        'subscriber_count', 'aux_status'
                                    ]]

        persistir_uma_linha(self.df_canal, query, self.conn)
