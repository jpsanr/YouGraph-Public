import pandas as pd
import os 
import math


class Tuple_Extractor:
    '''
        Script responsável pela extração de tuplas de conhecimento a partir do DptOIE;
        O DptOIE foi desenvolvido por Senna e Claro (2020);

        Como o projeto de Sena e Claro foi desenvolvido na linguagem JAVA, foi necessário fazer um port para Python;
            - Esta classe é responsável por este port.
        
        -> ATENÇÃO: Diversas melhorias podem e devem ser feitas nesta classe;
        
    
    '''


    def __init__(self):
        '''Do nothing'''

    
    def extrair_tupla(self, texto):
        status = -1

        print(" -> Salvando texto no input.txt")
        comando_rotina_java = "java -jar DptOIE.jar -sentencesIN .\\input.txt > log_java.txt -SC true -CC true -appositive 1 -appositive 2"
  
        path = os.getcwd() 
        dir_java = os.path.abspath(os.path.join(path, os.pardir))+ "\\Java"

        arquivo =  dir_java + "\\input.txt"
        arquivo = open(arquivo, "r+", encoding="utf-8")
        arquivo.truncate(0) #limpa arquivo
        arquivo.write(texto) #add texto recebido
        arquivo.close()
        
        print("  -> Extraindo Tuplas...")
        try:
            os.chdir(dir_java) #muda diretorio para pasta JAVA
            os.system(comando_rotina_java)
            os.chdir(path) # retorna diretorio para a pasta certa
            print("Pronto :D")
            status = 1
        except Exception as e:
            print(e)

        return status
    
    def get_ultimas_tuplas_geradas(self):
        
        
        path = os.getcwd() 
        dir_java = os.path.abspath(os.path.join(path, os.pardir))+ "\\Java"
        os.chdir(dir_java) #muda diretorio para pasta JAVA
        
        df_tuple = pd.read_csv('extractedFactsByDpOIE.csv', sep=";", encoding="ansi")
        df_tuple = df_tuple.rename(columns={' "SENTENÇA" ': 'sentenca',' "ARG1" ': 'arg1', ' "REL" ': 'rel',' "ARG2" ': 'arg2'})

        self.processar_sentence_Nan(df_tuple) #adciona as sentenças corretas quando elas forem Nan

        df_tuple = df_tuple.drop([' "ID SENTENÇA" ',
         ' "ID EXTRAÇÃO" ', 
         ' "COERÊNCIA" ', 
         ' "MINIMALIDADE" ', 
         ' "MÓDULO SUJEITO" ',
         ' "MÓDULO RELAÇÃO" ',
         ' "MÓDULO ARG2"'], axis=1)
        df_tuple = df_tuple.loc[:, ~df_tuple.columns.str.contains('^Unnamed')]
        df_tuple.dropna(inplace=True)

        
        

        os.chdir(path) # retorna diretorio para a pasta certa

        return df_tuple

    def processar_sentence_Nan(self, df_tuple):
        aux_sentenca = ''
        for key, row in df_tuple.iterrows():
            sentenca = row['sentenca']
            
            if isinstance(sentenca, str)== False and math.isnan(sentenca):
                sentenca = aux_sentenca  
                df_tuple.loc[key, "sentenca"] = sentenca 
            
            else:
                aux_sentenca = sentenca