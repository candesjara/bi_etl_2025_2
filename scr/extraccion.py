import pandas as pd
import requests
from pymongo import MongoClient
from pymongo.errors import ConfigurationError
import mysql.connector
from mysql.connector import Error

class Extraccion:
    def __init__(self):
        pass
    
    def extraccion_csv(self,ruta="",separador=""):
        df = pd.read_csv(ruta,sep=separador)
        if self.validar_df(df):
            return df
    
    def extraccion_xlsx(self,ruta=""):
        df = pd.read_excel(ruta)
        if self.validar_df(df):
            return df
        
    def leer_api(self, url):
        try:
           response = requests.get(url)
           
           if response.status_code == 200:
               data = response.json()
               df = pd.DataFrame(data)
               return df
           else:
               print(f"Error en la solicitud. Código de estado: {response.status_code}")
               return None
        except requests.exceptions.RequestException as e:
            print(f"Error al realizar la solicitud: {e}")
            return None
        
    def conectar_mongodb(self, uri,database):
        try:
            client = MongoClient(uri)
            db = client[database]
            db.list_collection_names()
            
            print(f"Conexión exitosa a la base de datos: {database}")
            return db
        except ConnectionError as e:
            print("Error al conectar la base de datos :",e)
            return None
    
    def conectar_mysql():
        try:
            conexion = mysql.connector.connect(
                host='localhost',
                user='root',
                password='123456',
                database = '085_activos'
            )
            if conexion.is_connected():
                return conexion
        except Error as e:
            print(f"Error al conectarse a la base de datos MySQL: {e}")
            return None
    
    def validar_df(self,df=pd.DataFrame()):
        if len(df) > 0:
            return True
        return False

    

