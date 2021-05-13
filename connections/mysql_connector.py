from acessos import read, get_conn, persistir_uma_linha, persistir_multiplas_linhas, replace_df
import credentials

class MySQL_Connector:

    def __init__(self, conn_name):
        self.conn_name = conn_name 

    def return_conn(self, db=""):
        conn = getattr(self, self.conn_name)(db)
        return conn  

    def conn_influencer_teste(self):
        hostname = "localhost"
        #db = "influencer_br"
        db = "influencer_teste"
        user = "root"
        password = ""

        return get_conn(hostname,db,user,password)

    def conn_influencer_br(self):
        hostname = "localhost"
        db = "influencer_br"
        user = "root"
        password = ""

        return get_conn(hostname,db,user,password)

    def conn_orfeu(self, db=""):
        hostname = credentials.orfeu_hostname 
        db = db
        user = credentials.orfeu_user 
        password = credentials.orfeu_password 
        port = credentials.orfeu_port

        return get_conn(hostname,db,user,password, port)

    def conn_orfeu_localhost(self, db=""):
        #hostname = "localhost"
        hostname = credentials.orfeu_localhost_hostname
        db = db
        user = credentials.orfeu_localhost_user
        password =  credentials.orfeu_localhost_password
        port = credentials.orfeu_localhost_port

        return get_conn(hostname,db,user,password, port)