#Load Python Libraries
# from urllib2 import urlopen
import pandas as pd
import numpy as np
import requests
import re
import json
import dateutil.parser
import datetime
from bs4 import BeautifulSoup
import requests

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

class NFLStats(object):
	"""docstring for NFL_scrape"""
	def __init__(self,season_type,season,week):
		self.season_type = season_type
		self.season = season
		self.week = week

	def stats_df(self,datatype):
		df = pd.DataFrame()
		if self.season_type == 'pre':
			url = 'http://www.nfl.com/schedules/{}/PRE{}'.format(self.season,self.week)
		if self.season_type == 'reg':
			url = 'http://www.nfl.com/schedules/{}/REG{}'.format(self.season,self.week)
		if self.season_type == 'post':
			url = 'http://www.nfl.com/schedules/{}/POST'.format(self.season)
		r = requests.get(url)
		soup = BeautifulSoup(r.text,'html.parser')
		games = soup.select('.gc-btn')

		for g in games:
			url = g.get('href')
			r = requests.get(url)
			soup = BeautifulSoup(r.text,'html.parser')
			m = re.search('\{(.*?})*',soup.find('script',string=re.compile('INITIAL')).string)
			gameid = json.loads(m.group(0))['instance']['gameId']
			
			if datatype == 'game':
				df2 = pd.Series(json.loads(m.group(0))['instance']['game']).to_frame().T
				df2 = df2[['id', 'gameTime', 'gsisId', 'awayTeam', 'homeTeam', 'week', 'venue', 'gameDetailId']]
				df3 = df2.awayTeam.apply(pd.Series)
				df3.drop(columns='franchise',inplace=True)
				df3.columns = ['away' + c for c in df3.columns]
				df4 = df2.homeTeam.apply(pd.Series)
				df4.drop(columns='franchise',inplace=True)
				df4.columns = ['home' + c for c in df4.columns]
				df5 = df2.week.apply(pd.Series)
				df5.rename(columns={'id':'weekId'},inplace=True)
				df6 = df2.venue.apply(pd.Series)
				df6.columns = ['venue' + c for c in df6.columns]
				df2 = pd.concat([df2,df3,df4,df5,df6],axis=1)
				df2 = df2[['id', 'gameTime', 'gsisId', 'gameDetailId', 'awayabbreviation', 'awayfullName', 'awayid', 'awaynickName', 'awaycityStateRegion', 'homeabbreviation', 
				'homefullName', 'homeid', 'homenickName', 'homecityStateRegion', 'seasonValue', 'weekId', 'seasonType', 'weekValue', 'weekType', 'venuefullName', 'venuecity', 
				'venuestate']]

			if datatype == 'gamedetails':
				df2 = pd.Series(json.loads(m.group(0))['instance']['gameDetails']).to_frame().T
				df2 = df2[['id', 'attendance','homePointsOvertime', 'homePointsTotal', 'homePointsQ1', 'homePointsQ2','homePointsQ3', 'homePointsQ4','homeTeam', 
				'homeTimeoutsUsed','homeTimeoutsRemaining','stadium', 'startTime','visitorPointsOvertime', 'visitorPointsOvertimeTotal','visitorPointsQ1','visitorPointsQ2', 
				'visitorPointsQ3','visitorPointsQ4', 'visitorPointsTotal', 'visitorTeam','visitorTimeoutsUsed', 'visitorTimeoutsRemaining','homePointsOvertimeTotal']]
				df3 = df2.homeTeam.apply(pd.Series)
				df3.columns = ['home' + c for c in df3.columns]
				df4 = df2.visitorTeam.apply(pd.Series)
				df4.columns = ['visitor' + c for c in df4.columns]
				df2 = pd.concat([df2,df3,df4],axis=1)
				df2 = df2[['id', 'attendance', 'homePointsOvertime', 'homePointsTotal', 'homePointsQ1', 'homePointsQ2', 'homePointsQ3', 'homePointsQ4', 'homeTimeoutsUsed', 
				'homeTimeoutsRemaining', 'stadium', 'startTime', 'visitorPointsOvertime', 'visitorPointsOvertimeTotal', 'visitorPointsQ1', 'visitorPointsQ2', 'visitorPointsQ3', 
				'visitorPointsQ4', 'visitorPointsTotal', 'visitorTimeoutsUsed', 'visitorTimeoutsRemaining', 'homePointsOvertimeTotal', 'homeabbreviation', 'homenickName', 
				'visitorabbreviation', 'visitornickName']]

			if datatype == 'teamstats':
				df2 = pd.DataFrame(json.loads(m.group(0))['instance']['teamStats']['homeTeamStats'])
				df3 = df2.opponentGameStats.apply(pd.Series)
				df3.columns = ['opponent' + c for c in df3.columns]
				df4 = df2.team.apply(pd.Series)
				df5 = df2.teamGameStats.apply(pd.Series)
				df6 = pd.DataFrame(json.loads(m.group(0))['instance']['teamStats']['awayTeamStats'])
				df7 = df6.opponentGameStats.apply(pd.Series)
				df7.columns = ['opponent' + c for c in df7.columns]
				df8 = df6.team.apply(pd.Series)
				df9 = df6.teamGameStats.apply(pd.Series)
				df2 = pd.concat([df3,df4,df5],axis=1)
				df6 = pd.concat([df7,df8,df9],axis=1)
				df2 = df2.append(df6)
				df2['gameId'] = gameid

			if datatype == 'drives':
				df2 = pd.DataFrame(json.loads(m.group(0))['instance']['gameDetails']['drives'])
				df3 = df2.possessionTeam.apply(pd.Series)
				df2 = pd.concat([df2,df3],axis=1)
				df2.drop(columns='possessionTeam',inplace=True)
				df2['gameId'] = gameid

			if datatype == 'plays':
				df2 = pd.DataFrame(json.loads(m.group(0))['instance']['gameDetails']['plays'])
				df3 = df2.possessionTeam.apply(pd.Series)
				df2 = pd.concat([df2,df3[['abbreviation','nickName']]],axis=1)
				df2.drop(columns=['possessionTeam','playStats'],inplace=True)
				df2['gameId'] = gameid

			if datatype =='player':
				df2 = pd.DataFrame(json.loads(m.group(0))['instance']['gamePlayerStats'])
				try:
					df3 = df2.game.apply(pd.Series)
					df3.columns = ['game' + c for c in df3.columns]
					df4 = df2.gameStats.apply(pd.Series)
					df5 = df2.player.apply(pd.Series)
					df6 = df5.currentTeam.apply(pd.Series)
					df7 = df5.person.apply(pd.Series)
					df8 = df2.season.apply(pd.Series)
					df8.columns = ['season' + c for c in df8.columns]
					df9 = df2.week.apply(pd.Series)
					df9.columns = ['week' + c for c in df9.columns]
					df2 = pd.concat([df2,df3,df4,df5,df6,df7,df8,df9],axis=1)
					df2 = df2[['createdDate', 'id', 'lastModifiedDate', 'gameid', 'defensiveAssists', 'defensiveInterceptions', 'defensiveInterceptionsYards',
					'defensiveForcedFumble', 'defensivePassesDefensed', 'defensiveSacks', 'defensiveSafeties', 'defensiveSoloTackles', 'defensiveTotalTackles',
					'defensiveTacklesForALoss', 'touchdownsDefense', 'fumblesLost', 'fumblesTotal', 'kickReturns', 'kickReturnsLong', 'kickReturnsTouchdowns', 'kickReturnsYards',
					'kickingFgAtt', 'kickingFgLong', 'kickingFgMade', 'kickingXkAtt', 'kickingXkMade', 'passingAttempts', 'passingCompletions', 'passingTouchdowns', 'passingYards',
					'passingInterceptions', 'puntReturns', 'puntingAverageYards', 'puntingLong', 'puntingPunts', 'puntingPuntsInside20', 'receivingReceptions', 'receivingTarget',
					'receivingTouchdowns', 'receivingYards', 'rushingAttempts', 'rushingAverageYards', 'rushingTouchdowns', 'rushingYards', 'kickoffReturnsTouchdowns',
					'kickoffReturnsYards', 'puntReturnsLong', 'opponentFumbleRecovery', 'totalPointsScored', 'kickReturnsAverageYards', 'puntReturnsAverageYards', 
					'puntReturnsTouchdowns', 'position', 'jerseyNumber', 'abbreviation', 'nickName', 'displayName', 'firstName', 'lastName', 'seasonid', 'weekid']]
				except:
					pass
			df = df.append(df2)
		return df

# def load_url(URL):
# 	'''
# 	Input = NHL.com  URL
# 	Output = JSON object
# 	'''
#  	return json_obj

#Create Pandas DataFrame from NHL Stats pages

#Load NHL Schedule API

#Convert DataFrame to csv for export
def create_csv(frame,name):
	'''
	Input = Pandas DataFrame
	Output = csv text
	'''
	converted = frame.to_csv(index=False,sep='|',encoding='utf-8')
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
