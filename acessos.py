import pymysql.cursors
import pandas as pd
#from enum import Enum
#from dateutil import relativedelta
#from datetime import datetime


def get_conn(hostname, db, user, password, port=3306):
    return pymysql.connect(host=hostname, database=db, user=user, password=password, cursorclass=pymysql.cursors.DictCursor, charset='utf8', port=port)


def read(conn, query, data=(), nome=None):
    if nome is not None:
        print(("---------Iniciando query {}---------").format(nome))

    db_cursor = conn.cursor()
    db_cursor.execute(query,data)

    if nome is not None:
        print(("---------Finalizada query {}---------").format(nome))

    result = db_cursor.fetchall()

    return pd.DataFrame(result)



def persistir_uma_linha(df, querie, conn):
    print("Iniciando Persistencia")
    try:
        with conn.cursor() as cursor:
            cursor.execute(querie, tuple(df.values[0]))
            conn.commit()
            print("Sucesso na inserção")
            
            return 1
    except Exception as e:
            print("Infelizmente algo deu errado na inserção (persistir_uma_linha)")
            print(e)   
            return -1
        
def persistir_multiplas_linhas(df, querie, conn): 
    print("Iniciando Persistencia")
    try:
        with conn.cursor() as cursor:
            cursor.executemany(querie, df.values.tolist())
            conn.commit()
        print("Sucesso na inserção")
        return 1
    except Exception as e:
        print("Infelizmente algo deu errado na inserção (persistir_multiplas_linhas)")
        print(e)                
        return -1
    
    
def persistir_banco(df, querie, conn): #Mantido para retrocompatibilidade
    print("Iniciando Persistencia")
    try:
        with conn.cursor() as cursor:
            cursor.executemany(querie, df.values.tolist())
            conn.commit()
        print("Sucesso na inserção")
    except Exception as e:
        print("Infelizmente algo deu errado na inserção")
        print(e)        
        

#Exemplo
# dict_set = {"status":1, "ano":2019}
# dict_where = {"video_id": "123"}
# update_banco(dict_set, dict_where, "teste", conn)

def update_banco(set_clause={}, where_clause={}, table_name=None ,conn=None):
    status = -1 

    if table_name is None or conn is None:
        print("Favor informe uma conn ou uma tabela")
        return status
    if len(set_clause) <= 0:
        print("Informe os campos Set")
        return status
    if len(where_clause) <= 0:
        print("Informe ao menos uma clausula Where")
        return status

    texto_set = ""
    for key in set_clause:
        aux_texto = "{}= '{}',".format(key, set_clause[key])
        texto_set = texto_set + aux_texto
    
    
    texto_where = ""
    for key in where_clause:
        aux_texto = "{}= '{}',".format(key, where_clause[key])
        texto_where = texto_where + aux_texto
    
    texto_set = "".join(texto_set.rsplit(",", 1))
    texto_where = "".join(texto_where.rsplit(",", 1))



    query = "UPDATE video SET {} WHERE {};".format(texto_set, texto_where)
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
            print("Sucesso no UPDATE")
            
            return 1
    except Exception as e:
            print("Infelizmente algo deu errado na UPDATE")
            print(e)  
            print("**")
            print(query) 
            return -1




def replace_df(df, table_name ,conn):
    lista_campos = list(df)
    separador=", "
    
    str_campos = separador.join(lista_campos)

    str_valores = "%s"
    for i in range(1, len(lista_campos)):
        str_valores += ", %s"

    querie = "REPLACE INTO {} ({}) VALUES ({})".format(table_name, str_campos, str_valores)
    #print(querie)
    if len(df) > 1:
        status = persistir_multiplas_linhas(df, querie, conn)
    else:
        status = persistir_uma_linha(df, querie, conn)
 
    
    return status


