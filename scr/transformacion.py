import json
import pandas as pd
from sqlalchemy import create_engine,inspect
from sqlalchemy.exc import NoSuchTableError
from datetime import datetime
import logging
import sqlite3

class Logs:
    def __init__(self):
        self.fecha_hora = datetime.now().strftime('%Y%m%d_%H%M%S')
        logging.basicConfig(filename=f'logs/log_{self.fecha_hora}.txt', level=logging.INFO,filemode='a',  # Añade 'filemode' con valor 'a' para asegurar el append
                            format='%(asctime)s - %(levelname)s - %(message)s')

    def log(self, mensaje, nivel):
        if nivel == 'info':
            logging.info(mensaje)
        elif nivel == 'error':
            logging.error(mensaje)

class Extraccion:
    def __init__(self):
        self.logs = Logs()

    def cargar_desde_archivo(self,ruta_archivo):
        try:
            self.logs.log(f'Intentar cargar la ruta {ruta_archivo}', 'info')
            if ruta_archivo.endswith('.csv'):
                self.logs.log(f'valida extension  .csv {ruta_archivo}', 'info')
                df = pd.read_csv(ruta_archivo,index_col=False)
            elif ruta_archivo.endswith('.xlsx'):
                self.logs.log(f'valida extension  .xlsx {ruta_archivo}', 'info')
                df = pd.read_excel(ruta_archivo)
                self.logs.log(f'valida extension  .txt {ruta_archivo}', 'info')
            elif ruta_archivo.endswith('.txt'):
                df = pd.read_csv(ruta_archivo, delimiter = "\t",index_col=False)
            return df
        except Exception as e:
            hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logs.log(f'Error al cargar el archivo {ruta_archivo}: {str(e)}', 'error')

class Carga:
    def __init__(self, db_string):
        self.engine = create_engine(db_string)
        self.logs = Logs()

    def cargar_a_bd(self, df, tabla, accion='replace'):
        try:
            if df.empty:
                raise ValueError("El DataFrame está vacío. No se puede cargar en la base de datos.")

            inspector = inspect(self.engine)

            if accion not in ['replace', 'append']:
                raise ValueError('La acción debe ser "replace" o "append"')

            if inspector.has_table(tabla):
                self.logs.log(f'Tabla {tabla} existe. Procediendo con la acción: {accion}', 'info')
                if accion == 'replace':
                    df.to_sql(tabla, con=self.engine, if_exists='replace', index=False)
                    self.logs.log(f'Tabla {tabla} reemplazada exitosamente.', 'info')
                elif accion == 'append':
                    df.to_sql(tabla, con=self.engine, if_exists='append', index=False)
                    self.logs.log(f'Datos añadidos a la tabla {tabla} exitosamente.', 'info')
            else:
                self.logs.log(f'Tabla {tabla} no existe. Creando tabla...', 'info')
                df.to_sql(tabla, con=self.engine, if_exists='replace', index=False)
                self.logs.log(f'Tabla {tabla} creada exitosamente.', 'info')
        except Exception as e:
            self.logs.log(f'Error al cargar datos en la tabla {tabla} con acción {accion}: {str(e)}', 'error')
            print(f'Error al cargar datos en la tabla {tabla} con acción {accion}: {str(e)}')

class Transformacion:
    def __init__(self, db_string):
        self.extraccion = Extraccion()
        self.carga = Carga(db_string)
        self.logs = Logs()
        self.dfs=[]

    def jupyter_class(self,dfs=[]):
        self.dfs= dfs
        df_final = pd.DataFrame()
        
        ### data_population_world.csv
        self.dfs[0]=pd.melt(self.dfs[0],
                         id_vars=['Country Name','Country Code','Indicator Name','Indicator Code'],
                         value_vars=self.dfs[0].iloc[:,4:-1].columns,var_name='year',
                         value_name=('total'))
        
        #1. Renombrar las columnas del primer DataFrame 
        self.dfs[0].columns =['country_name', 'country_code', 'indicator_name', 'indicator_code',
       'year','population']
        
        #2. Eliminar filas con valores nulos en la columna 'population'
        self.dfs[0] = self.dfs[0].dropna(subset=['population'])
        
        #3. Filtrar el DataFrame para conservar solo columnas específicas
        self.dfs[0] = self.dfs[0][['country_name', 'country_code', 'indicator_name', 'indicator_code','year','population']]
        
        ### metadata_countries.csv
        #4. Renombrar las columnas del segundo DataFrame
        self.dfs[1].columns =['country_name','country_code','region','income_group','n']
        
        #5. Llenar valores nulos en la columna 'region'
        self.dfs[1]['region'] =self.dfs[1]['region'].fillna('Sin region')
        
        #6. Unir los DataFrames
        df_final = self.dfs[0].merge(self.dfs[1],left_on = 'country_code',
                                           right_on = 'country_code',how='inner')
        
        #7. Filtrar las columnas del DataFrame final
        df_final = df_final [['country_name_x','country_code','region','income_group','year','population']]
        
        #8. Calcular la tasa de cambio de población
        df_final['rate_pop'] = df_final.groupby(['country_name_x'],group_keys=False)['population'].pct_change()*100
        
        #9. Convertir la población a millones
        df_final['pop_millon'] = df_final['population']/1000000
        
        #10. Redondear valores
        df_final['pop_millon'] = df_final['pop_millon'].round(1)
        
        df_final['rate_pop_millon'] = df_final['rate_pop'].round(1)
        
        #country_list.csv
        #11. Unir con el tercer DataFrame
        df_final = df_final.merge(self.dfs[2],left_on = 'country_code',
                                           right_on = 'alpha-3',how='inner')
        #12. Filtrar el DataFrame final
        df_final = df_final[df_final['country_code'].isin(df_final['country_code'])]
        
        #13. Seleccionar y renombrar columnas
        df_final = df_final[['country_name_x','country_code','region_y','sub-region',
                             'income_group','year','population','pop_millon','rate_pop','rate_pop_millon']]
        
        df_final.columns= ['country_code','country_name','region_name','sub_region_name',
                           'income_group','year','population','pop_millon','rate_pop','rate_pop_millon']
        
        #14. Retornar el DataFrame final
        return df_final
        

class Main:
    def __init__(self,db_string):
        self.transformacion = Transformacion(db_string)
        self.logs = Logs()

    def ejecutar(self,config_file):
        try:
            with open(config_file) as f:
                config = json.load(f)
            archivos = config['archivos']
            tabla = config['tabla']
            self.dfs=[]
            for archivo in archivos:
                df = self.transformacion.extraccion.cargar_desde_archivo(archivo)
                self.dfs.append(df)
            df_final =self.transformacion.jupyter_class(self.dfs)
            if df_final is not None:            
                if "unnamed_0" in df_final.columns:
                    df_final = df_final.drop(df_final.columns[0], axis=1)
                df_final.to_excel('datos_mundo.xlsx', index=False)
                self.transformacion.carga.cargar_a_bd(df_final,tabla) 
        except Exception as e:
            self.logs.log(f'Error al ejecutar el proceso ETL Ejecutar: {str(e)}', 'error')

if __name__ == "__main__":
    db_string = 'sqlite:///etl_itm.db'    
    config_file = 'D:/ITM_ETL/2024_02/src/static/config.json'  # Reemplaza con la ruta a tu archivo de configuración
    main = Main(db_string)
    main.ejecutar(config_file)
    