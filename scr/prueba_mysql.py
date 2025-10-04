import pandas as pd
from extraccion import Extraccion

conexion = Extraccion.conectar_mysql()

cursor = conexion.cursor()
consulta = "SELECT * FROM assets"
cursor.execute(consulta)
resultados = cursor.fetchall()

columnas = [i[0] for i in cursor.description]

df = pd.DataFrame(resultados, columns=columnas)

print(df)