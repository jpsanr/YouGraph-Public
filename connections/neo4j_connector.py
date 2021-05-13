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

import os

import credentials

class Neo4jObject():
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password),encrypted= False)
    

    def close(self):
        self._driver.close()


    def execute(self, query, parameters=None,  **kwparameters ):
        with self._driver.session() as graphDB_session:
            return graphDB_session.run(query,parameters=parameters,  **kwparameters)

        
class Neo4j_Connector(Neo4jObject):
    def __init__(self, uri, user, password):
        self.neo4jObj = Neo4jObject.__init__(self,uri, user, password)

    def clean_db(self):
        self.execute("MATCH (n) DETACH DELETE n")
        
    def run_raw_query(self, query):
        lista_records = []
        session = self._driver.session()
        
        try:
            result = session.run(query)
        
            for r in result: #retorna um objeto "record"
                
                lista_records.append(r.data())
        except Exception as e:
            print(query)
            print("** ERRO **")
            print(e)


        session.close()
        self._driver.close()
        return lista_records


    def search_node(self, node_type, nome):
        lista_resultados = []
        
        query ="match (n:{node_type}) WHERE n.nome='{nome}'  return n".format(node_type=node_type, nome=nome)
        session = self._driver.session()
        
        try:
            result = session.run(query)
        
            for r in result: #retorna um objeto "record"
                lista_resultados.append(r.data())
        except Exception as e:
            print(query)
            print("** ERRO **")
            print(e)


        session.close()
        self._driver.close()

        return lista_resultados
    
    def search_relationship(self,  nome_1, nome_2, tipo_no_1, tipo_no_2, relacao):
        lista_resultados = []

        # print("**** DEBUG ****")
        # print("Nome 1: {} \n Nome 2: {} \n Tipo no 1: {} \n Tipo no 2: {} \n Relacao: {}".format(nome_1, nome_2, tipo_no_1, tipo_no_2, relacao))

        # print("****")

        query = '''
            MATCH (n1:{tipo_no_1} {abre_chave}nome:"{nome_1}"{fecha_chave})-[r:{relacao}]->(n2:{tipo_no_2}{abre_chave}nome: "{nome_2}"{fecha_chave})
            return n1,r,n2
        '''.format(nome_1=nome_1, nome_2=nome_2, tipo_no_1=tipo_no_1, tipo_no_2=tipo_no_2, relacao=relacao, fecha_chave="}",abre_chave="{")


        # print(query)

        session = self._driver.session()
        
        try:
            result = session.run(query)
        
            for r in result: #retorna um objeto "record"
                lista_resultados.append(r.data())
        except Exception as e:
            print(query)
            print("** ERRO **")
            print(e)


        session.close()
        self._driver.close()


        return lista_resultados

    def update_node_property(self, node_type, node_name, dict_node_properties):
        '''TO DO '''
        # status = 0
        # # query= '''
        # #     match(n:{node_type} {{nome:"{node_name}"}})
        # #     SET $props
        # #     return n
        # # '''.format(node_type=node_type, node_name=node_name)
        # query = '''

        # MATCH (n { name: 'Andy' })
        # SET n = $props
        # RETURN n.name, n.position, n.age, n.hungry
                
        # '''

        # try:
        #     self.execute("CREATE (a:Person { name: $name}) RETURN id(a) AS node_id", name="Andy")
        #     self.execute(query, props={"name" : "Marcos","idade":30})  
        # except Exception as e:
        #     status = -1
        #     print(e)
        # return status, query


    def add_nodes(self, df):
        lista_erros = []
        for row in df.iterrows():
            status = -1 
            dict_date = row[1].to_dict() #Converte Serie em dict
            tipo_no = dict_date['type'] 
            nome_no = dict_date['nome']

            lista_resultados = self.search_node(tipo_no, nome_no)

            if len(lista_resultados) == 0:
                #print("Elemento não existe e será persistido!")
                #del dict_date['type']
                query= "CREATE (n:{tipo_no} $props) RETURN n".format(tipo_no=tipo_no)
                try:
                    self.execute(query, props=dict_date)  
                    status = 0 
                except Exception as e:
                    lista_erros.append(row[0])
                    print(e)
        return lista_erros
           
    def criar_relacao_de_para(self, nome_1, nome_2, tipo_no_1, tipo_no_2, relacao):
        
        status = -1
        relacao_existe = True if len(self.search_relationship(nome_1, nome_2, tipo_no_1, tipo_no_2, relacao)) > 0 else False

        if relacao_existe == False:

            query = '''
                MATCH (a:{tipo_no_1}),(b:{tipo_no_2})
                WHERE a.nome = "{nome_1}" AND b.nome = "{nome_2}"
                CREATE (a)-[r:{relacao}]->(b)
                RETURN type(r)
            '''.format(nome_1=nome_1, nome_2=nome_2, tipo_no_1=tipo_no_1, tipo_no_2=tipo_no_2, relacao=relacao)

            # print(query)
            # print("*******************")
            try:
                self.execute(query)  
                status = 0 
            except Exception as e:
                print(e)
                status = -1
                print("****")
                print(query)
        
        else:
            print("Relação não persisitida, pois ela já existe no banco")

        return status

if __name__ == '__main__':
    uri = credentials.neo4j_uri
    user = credentials.neo4j_user
    password = credentials.neo4j_password
    neo4j_connector = Neo4j_Connector(uri, user, password)


    df = pd.DataFrame(np.array([['Person',"João", 24, "Masculino"], ['Person', "Pedro", 52, "Masculino"], ["Funcionario","Maria", 81, "Feminino"]]),
                   columns=['type', 'nome', 'idade', 'sexo'])


    #neo4j_connector.clean_db()
    neo4j_connector.add_nodes(df)
    #lista_resultados = neo4j_connector.search_node("Person", "Joãov")
    