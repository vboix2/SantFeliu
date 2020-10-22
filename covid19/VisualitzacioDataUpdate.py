import pandas as pd
from google.cloud import storage
from io import StringIO

def update(request):

	# ------------- Dades de Catalunya per Municipi

	# Dades
	covid = pd.read_json('https://analisi.transparenciacatalunya.cat/resource/jj6z-iyrp.json?$limit=500000', dtype={'data':str, 'numcasos':int})

	# Selecció de columnes
	covid = covid.loc[:,['data','municipidescripcio','comarcadescripcio','resultatcoviddescripcio','sexedescripcio','numcasos']]
	
	# Data
	covid['data'] = pd.to_datetime(covid['data'], format="%Y-%m-%dT%H:%M:%S.%f")
	covid['data'] = covid['data'].apply(lambda x: x.strftime("%Y-%m-%d"))

	# Resultat
	covid.loc[covid.resultatcoviddescripcio!='Sospitós','resultatcoviddescripcio'] = 'Positiu'

	covid = covid.rename(columns={'municipidescripcio':'municipi','comarcadescripcio':'comarca','resultatcoviddescripcio':'resultat','sexedescripcio':'sexe','numcasos':'casos'})

	# Exportació de dades
	f = StringIO()
	covid.to_csv(f, index=False)
	f.seek(0)
	storage_client = storage.Client.from_service_account_json('datascience-290812-7413488fdd43.json')
	bucket = storage_client.get_bucket('datascience-290812')
	blob = bucket.blob('covid19catalunya/casos_municipi.csv')
	blob.upload_from_string(f.getvalue(), content_type='text/csv')

	# ------------- Dades de Sant Feliu de Llobregat

	# Dades
	covid = pd.read_json('https://analisi.transparenciacatalunya.cat/resource/jj6z-iyrp.json?MunicipiCodi=08211', dtype={'data':str, 'numcasos':int})

	# Selecció de columnes
	covid = covid.loc[:,['data','resultatcoviddescripcio', 'sexedescripcio', 'numcasos']]
	
	# Data
	covid['data'] = pd.to_datetime(covid['data'], format="%Y-%m-%dT%H:%M:%S.%f")
	covid['any'] = covid['data'].apply(lambda x: x.year)	
	covid['mes'] = covid['data'].apply(lambda x: x.month)
	covid['setmana'] = covid['data'].apply(lambda x: x.strftime("%V"))
	covid['dia'] = covid['data'].apply(lambda x: x.day)
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

	# ---------------- Centres educatius

	# Dades covid

	covid = pd.read_json('https://analisi.transparenciacatalunya.cat/resource/fk8v-uqfv.json?$limit=300000', dtype={'alumn_positiu_acum':int,'personal_positiu_acum':int, 'altres_positiu_acum':int, 'codcentre':str})

	COLUMNS = ['datageneracio','codcentre','alumn_positiu_acum','personal_positiu_acum','altres_positiu_acum']
	covid = covid.loc[:,COLUMNS].rename(columns={'codcentre':'codi_centre','datageneracio':'data'})

	covid['data'] = pd.to_datetime(covid['data'], format="%Y-%m-%dT%H:%M:%S.%f")
	covid['data'] = covid['data'].apply(lambda x: x.strftime("%Y-%m-%d"))

	# Dades centres

	centres = pd.read_json('https://analisi.transparenciacatalunya.cat/resource/3u9c-b74b.json', dtype={'codi_centre':str})
	centres = centres.loc[centres.nom_municipi=="Sant Feliu de Llobregat", ['codi_centre','nom_municipi']]

	# Unim dades

	dades = pd.merge(covid, centres, on="codi_centre")

	# Agrupem dades

	dades = dades.groupby(['data'], as_index=False).agg({'alumn_positiu_acum':sum, 'personal_positiu_acum':sum, 'altres_positiu_acum':sum})

	# Calculem els nous positius diaris (desfem els valors acumulats)

	for i in range(len(dades)-1,0,-1):
		dades.loc[i,'alumn_positiu_acum'] = dades.loc[i,'alumn_positiu_acum'] - dades.loc[i-1,'alumn_positiu_acum']
		dades.loc[i,'personal_positiu_acum'] = dades.loc[i,'personal_positiu_acum'] - dades.loc[i-1,'personal_positiu_acum']
		dades.loc[i,'altres_positiu_acum'] = dades.loc[i,'altres_positiu_acum'] - dades.loc[i-1,'altres_positiu_acum']

	dades = dades.rename(columns={'alumn_positiu_acum':'pos_alumn', 'personal_positiu_acum':'pos_pers', 'altres_positiu_acum':'pos_altres'})

	# Exportació de dades
	f = StringIO()
	dades.to_csv(f, index=False)
	f.seek(0)
	storage_client = storage.Client.from_service_account_json('datascience-290812-7413488fdd43.json')
	bucket = storage_client.get_bucket('datascience-290812')
	blob = bucket.blob('covid19catalunya/casos_centres.csv')
	blob.upload_from_string(f.getvalue(), content_type='text/csv')

	return "Dades actualitzades"
