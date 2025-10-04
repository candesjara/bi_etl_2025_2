from extraccion import Extraccion

url="D:/Proyectos/etl/scr/data/country_list.csv"
separador = ","

df = Extraccion().extraccion_csv(url,separador)
print(df)