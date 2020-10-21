# Script d'actualització de dades
import pandas as pd
from google.cloud import storage
from io import StringIO

def update(request):
	# Dades
	covid = pd.read_json('https://analisi.transparenciacatalunya.cat/resource/jj6z-iyrp.json?MunicipiCodi=08211', dtype={'data':str, 'numcasos':int})

	# Selecció de columnes
	covid = covid.loc[:,['data','resultatcoviddescripcio', 'sexedescripcio', 'numcasos']]
	
	# Data
	covid['data'] = pd.to_datetime(covid['data'], format="%Y-%m-%dT%H:%M:%S.%f")
	covid['data'] = covid['data'].apply(lambda x: x.strftime("%Y-%m-%d"))

	# Resultat
	covid.loc[covid.resultatcoviddescripcio!='Sospitós','resultatcoviddescripcio'] = 'Positiu'

	covid = covid.rename(columns={'resultatcoviddescripcio':'resultat', 'sexedescripcio':'sexe', 'numcasos':'casos'})

	# Exportació de dades
	f = StringIO()
	covid.to_csv(f, index=False)
	f.seek(0)
	storage_client = storage.Client.from_service_account_json('datascience-290812-7413488fdd43.json')
	bucket = storage_client.get_bucket('datascience-290812')
	blob = bucket.blob('covid19catalunya/casos_santfeliu.csv')
	blob.upload_from_string(f.getvalue(), content_type='text/csv')

	return "Dades actualitzades"