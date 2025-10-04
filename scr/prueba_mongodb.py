from extraccion import Extraccion
import pandas as pd

uri = "mongodb://localhost:27017/"
nombre_db = "bi_mx"
nombre_coleccion = "mx"

db = Extraccion.conectar_mongodb(uri,nombre_db)
coleccion = db[nombre_coleccion]
documentos = list(coleccion.find().limit(10))
print(documentos)

