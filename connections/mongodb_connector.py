import pymongo
import datetime
import pandas as pd 
import numpy as np

class Mongo_Connector:

    def __init__(self, dbname, password):
        client = pymongo.MongoClient("mongodb+srv://influencer:{}@pdm.sygia.mongodb.net/{}?retryWrites=true&w=majority".format(password,
                                                                                                                                dbname))
        self.db = client['influencer']

    def correct_encoding(self, dictionary):
        new = {}
        for key1, val1 in dictionary.items():
            # Nested dictionaries
            if isinstance(val1, dict):
                val1 = self.correct_encoding(val1)

            if isinstance(val1, np.bool_):
                val1 = bool(val1)

            if isinstance(val1, np.int64):
                val1 = int(val1)

            if isinstance(val1, np.float64):
                val1 = float(val1)

            new[key1] = val1

        return new

    def get_db(self):
        return self.db

    def persistir_dados(self, lista_ou_dict, collection):
        result = None

        collection = self.db[collection]

        if isinstance(lista_ou_dict, dict): #insert one 
            dict_dados = lista_ou_dict
            dict_dados = self.correct_encoding(dict_dados)
            result = collection.insert_one(dict_dados).inserted_id
            
        elif isinstance(lista_ou_dict, list):
            lista_dados = lista_ou_dict

            if len(lista_dados) > 1: #insert many
                result = collection.insert_many(lista_dados)

            else: #insert one 
                post_id = collection.insert_one(lista_dados[0]).inserted_id
        else:
            print("Erro na inserção: Dado não é dicionário nem lista")

        return result



    

