import pandas as pd
from extraccion import Extraccion

url = 'https://www.datos.gov.co/resource/p6dx-8zbt.json' 
df=Extraccion.leer_api(url)
print(df)