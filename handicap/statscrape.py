#Import Python Libraries
# from urllib2 import urlopen
import pandas as pd
import requests
import re
import json
# import dateutil.parser
# import datetime
from bs4 import BeautifulSoup
# import requests

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
		r = requests.get(self.URL)
		df = pd.DataFrame(json.loads(r.text)['data'])
		return df

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

class NFLStats(object):
	"""
	Data structure for ETL process of NFL Game stats

	Parameters

	season_type : Str
		Must be one of three values: 'pre', 'reg', 'post' which is equivalent to Preseason, Regular Season and Post Season respectively
	season : Int
		Calendar year of the NFL season, only years 2001 and later are available
	week : Int
		Integer with a range of 1-17
	data : List-like
	"""
	def __init__(self,season_type,season,week):
		self.season_type = season_type
		self.season = season
		self.week = week
		self.data = []
		# self.gamedetails = self.create_df(frame='gamedetails')
		# self.scoringsummary = self.create_df(frame='scoringsummary')
		# self.drives = self.create_df(frame='drives')
		# self.plays = self.create_df(frame='plays')
		# self.game = self.create_df(frame='game')
		# self.gameplayer = self.create_df(frame='gameplayer')
		# self.teamstats = self.create_df(frame='teamstats')
		# self.standings = self.create_df(frame='standings')
		if self.season_type == 'pre':
			self.url = 'http://www.nfl.com/schedules/{}/PRE{}'.format(self.season,self.week)
		if self.season_type == 'reg':
			self.url = 'http://www.nfl.com/schedules/{}/REG{}'.format(self.season,self.week)
		if self.season_type == 'post':
			self.url = 'http://www.nfl.com/schedules/{}/POST'.format(self.season)

	def scrape(self):
		"""
		Scrapes NFL.com schedule and Game Center data and returns it to parent class
		"""

		r = requests.get(self.url)
		soup = BeautifulSoup(r.text,'html.parser')
		games = soup.select('.gc-btn')

		for g in games:
			url = g.get('href')
			r = requests.get(url)
			soup = BeautifulSoup(r.text,'html.parser')
			m = re.search('\{(.*?})*',soup.find('script',string=re.compile('INITIAL')).string)

			self.data.append(json.loads(m.group(0))['instance'])
			
	def create(self, frame):
		"""
		Creates a dataframe from previously scraped data	

		frame : 'Str'
			select which dataframe to return.  Choose from 'game', 'gameDetails', 'scoringSummaries', 'drives', 'plays', 'playStats', 'gamePlayerStats',
			'teamStats',  standings
		"""	

		# GameDetails DataFrame
		if frame in ['gameDetails', 'scoringSummaries', 'drives', 'plays', 'playStats']:
			df = pd.io.json.json_normalize(data=[d.get('gameDetails') for d in self.data], sep='_')
			
			if frame == 'gameDetails':
				cols = [c for c in df.columns if not df[c].apply(isinstance, args=[list]).any()]
				df = df[cols].dropna(how='all', axis=1)
				df.columns = df.columns.str.upper()
				bools = df.select_dtypes(include='bool').columns
				for b in bools:
					df[b] = df[b].astype(int)
				df.sort_index(axis=1, inplace=True)	

			if frame in ['scoringSummaries', 'drives']:
				df2 = pd.DataFrame()
				for i in range(len(df)):
					df3 = pd.io.json.json_normalize(data=df[frame][i], sep='_')
					df3['id'] = df.loc[i,'id'] 
					df2 = df2.append(df3, sort=False)
				df = df2
				df.dropna(how='all', axis=1, inplace=True)
				df.columns = df.columns.str.upper()
				bools = df.select_dtypes(include='bool').columns
				for b in bools:
					df[b] = df[b].astype(int)
				df.sort_index(axis=1, inplace=True)

			if frame in ['plays', 'playStats']:
				df2 = pd.DataFrame()
				for i in range(len(df)):
					df3 = pd.io.json.json_normalize(data=df['plays'][i], sep='_')
					df3['id'] = df.loc[i,'id'] 
					df2 = df2.append(df3, sort=False)
				df2.reset_index(drop=True, inplace=True)
				cols = [c for c in df2.columns if not df2[c].apply(isinstance, args=[list]).any()]
					
				if frame == 'plays':
					df = df2[cols]	

				if frame == 'playStats':
					df4 = pd.DataFrame()
					for i in range(len(df2)):
						try:
							df5 = pd.io.json.json_normalize(data=df2['playStats'][i], sep='_')
							df5['id'] = df2.loc[i,'id']
							df5['playId'] = df2.loc[i,'playId']
							df4 = df4.append(df5, sort=False)
						except AttributeError:
							df5 = pd.DataFrame({'id':{i:df2.loc[i,'id']}, 'playId':{i:df2.loc[i,'playId']}})
							df4 = df4.append(df5, sort=False)
					df = df4
			
				df.reset_index(drop=True, inplace=True)
				df.dropna(how='all', axis=1, inplace=True)
				bools = df.select_dtypes(include='bool').columns
				for b in bools:
					df[b] = df[b].astype(int)
				df.columns = df.columns.str.upper()
				df.sort_index(axis=1, inplace=True)

			return df	

		# Game DataFrame
		elif frame == 'game':
			df = pd.io.json.json_normalize(data=[d.get('game') for d in self.data], sep='_')
			df.reset_index(drop=True,inplace=True)
			cols = [c for c in df.columns if not df[c].apply(isinstance, args=[list]).any()]
			df = df[cols]
			bools = df.select_dtypes(include='bool').columns
			for b in bools:
				df[b] = df[b].astype(int)
			df.columns = df.columns.str.upper()
			df.dropna(how='all', axis=1, inplace=True)
			df.sort_index(axis=1, inplace=True)
			return df
		
		# Player Stats DataFrame
		elif frame == 'gamePlayerStats':
			df = pd.DataFrame()
			for d in range(len(self.data)):
				df = df.append(pd.io.json.json_normalize(data=self.data[d].get('gamePlayerStats'), sep='_'),sort=False)
			df.reset_index(drop=True,inplace=True)
			bools = df.select_dtypes(include='bool').columns
			for b in bools:
				df[b] = df[b].astype(int)
			df.columns = df.columns.str.upper()
			df.dropna(how='all', axis=1, inplace=True)
			df.sort_index(axis=1, inplace=True)
			return df	

		# Team Stats DataFrame
		elif frame == 'teamStats':
			df = pd.io.json.json_normalize(data=[d.get('teamStats') for d in self.data], sep='_')
			ids = pd.Series([d.get('gameId') for d in self.data], name='gameId')
			df2 = pd.io.json.json_normalize(df.homeTeamStats.apply(pd.Series)[0], sep='_')
			df3 = pd.io.json.json_normalize(df.awayTeamStats.apply(pd.Series)[0], sep='_')
			df = df2.join(df3, lsuffix='_home', rsuffix='_away')
			df = df.join(ids)
			df.reset_index(drop=True,inplace=True)
			bools = df.select_dtypes(include='bool').columns
			for b in bools:
				df[b] = df[b].astype(int)
			df.columns = df.columns.str.upper()
			df.dropna(how='all', axis=1, inplace=True)
			df.sort_index(axis=1, inplace=True)
			return df	

		# Standings DataFrame
		elif frame == 'standings':
			df = pd.io.json.json_normalize(data=[d.get('standings') for d in self.data], sep='_')
			df = pd.io.json.json_normalize(pd.DataFrame(df['edges'][0])['node'], sep='_')
			cols = [c for c in df.columns if not df[c].apply(isinstance, args=[list]).any()]
			df2 = pd.DataFrame()
			for i in range(len(df)):
				df3 = pd.DataFrame(df.loc[i,'teamRecords'])
				df3['week_id'] = df.loc[i,'week_id']
				df2 = df2.append(df3, sort=False)
			df = df[cols].merge(df2, how='left', left_on='week_id', right_on='week_id')
			df.reset_index(drop=True,inplace=True) 
			bools = df.select_dtypes(include='bool').columns
			for b in bools:
				df[b] = df[b].astype(int)
			df.columns = df.columns.str.upper()
			df.dropna(how='all', axis=1, inplace=True)
			df.sort_index(axis=1, inplace=True)
			return df	

		else:
			return ValueError

#Convert DataFrame to csv for export
def create_csv(frame,name):
	'''
	Input = Pandas DataFrame
	Output = csv text
	'''
	converted = frame.to_csv(index=False,sep='|',encoding='utf-8',line_terminator='///x')
	stat_table = open ('d:\\depthandtaxes\\{}.csv'.format(name),'w')
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
