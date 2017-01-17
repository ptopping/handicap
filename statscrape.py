#Load Python Libraries
from urllib2 import urlopen
import pandas as pd
import numpy as np
import json
import dateutil.parser
import datetime

#Parse Datetime string into python ISO 8601 datetime object
zulu_con = lambda x: dateutil.parser.parse(x)

#Load JSON object into Python
class NHL_Stats(object):

	def __init__(self,URL):
		self.URL = URL

	def statsDF(self):
		'''
		Input = NHL.com stats URL
		Output = Pandas DataFrame with time conversion
		'''
		response = urlopen(self.URL)
		data = response.read().decode("utf-8")
		json_obj = json.loads(data)
		val = json_obj.values()
		lev = val[1]
		df = pd.DataFrame(lev)
		df['gameDate'] = df['gameDate'].apply(zulu_con).astype('datetime64[ns, MST]')
		converted = pd.DataFrame.to_csv(df.rename_axis('dfIndex'), encoding = 'utf-8')
		return converted

	def scheduleDF(self):
		'''
		Input = NHL.com schedule URL
		Output = Pandas DataFrame of daily schedule
		'''
		response = urlopen(self.URL)
		data = response.read().decode("utf-8")
		json_obj = json.loads(data)
		list_2 = []
		dict_1 = json_obj.get('dates')[0]
		list_1 = dict_1.get('games')[::]
		df1 = pd.DataFrame(list_1)
		dict_2 = dict(df1['teams'])
		for k in dict_2:
			list_2.append(dict_2.get(k))
		df2 = pd.DataFrame(list_2)
		df_away = pd.DataFrame(list(df2['away']))
		df_home = pd.DataFrame(list(df2['home']))
		df_away2 = pd.DataFrame(list(df_away['team']))
		df_home2 = pd.DataFrame(list(df_home['team']))
		df_fin = pd.DataFrame({'gameDate' : df1['gameDate'], 'visitor' : df_away2['name'], 'home' : df_home2['name']})
		df_fin['gameDate'] = df_fin['gameDate'].apply(zulu_con).astype('datetime64[ns, MST]')
		converted = pd.DataFrame.to_csv(df_fin.rename_axis('dfIndex'), encoding = 'utf-8')
		return converted

def load_url(URL):
	'''
	Input = NHL.com  URL
	Output = JSON object
	'''
 	return json_obj

#Create Pandas DataFrame from NHL Stats pages

#Load NHL Schedule API

#Convert DataFrame to csv for export
def create_csv(frame):
	'''
	Input = Pandas DataFrame
	Output = csv text
	'''
	converted = pd.DataFrame.to_csv(frame, encoding = 'utf-8')
	stat_table = open ('%s.csv' % frame,'w')
	stat_table.write(converted)
	stat_table


#Merge two DataFrames to one
def df_join(df1, df2):
	'''
	df1 = First DataFrame
	df2 = Second DataFrame
	Output = Merged DataFrame
	'''
	colsdf1 = df1.columns
	colsdf2 = df2.columns
	listcols1 = colsdf1.tolist()
	listcols2 = colsdf2.tolist()
	cols = []
	for obj in listcols1:
		if obj in listcols2:
			cols.append(obj)
	merged = pd.merge(df1, df2, on = cols, how = 'left')
	return merged
