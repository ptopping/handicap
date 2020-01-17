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
	"""
	def __init__(self,season_type,season,week):
		self.season_type = season_type
		self.season = season
		self.week = week
		self.data = []
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

		data = []
		r = requests.get(self.url)
		soup = BeautifulSoup(r.text,'html.parser')
		games = soup.select('.gc-btn')

		for g in games:
			data = []
			url = g.get('href')
			r = requests.get(url)
			soup = BeautifulSoup(r.text,'html.parser')
			m = re.search('\{(.*?})*',soup.find('script',string=re.compile('INITIAL')).string)
			
			self.data.append(json.loads(m.group(0))['instance'])
			
			# self.items = 
	
	def create_df(self, frame='all'):
		"""
		Creates a dataframe from previously scraped data

		frame : 'Str'
			select which dataframe to return.  Choose from 'all', 'gameDetails', 'gamePlayerStats', 'game', 'teamStats', 'standings'. Defaults to 'all'
		"""

		# Flatten json and create DataFrames
		df = pd.DataFrame(self.data)	
		df = pd.io.json.json_normalize(data=self.data, sep='_')
		idx = [k for k,v in self.data[0].items() if isinstance(v,list)]
		['gameDetails', 'gamePlayerStats', 'game', 'teamStats', 'standings']		
		# Game Dataframe
		gamedf = df.drop(labels=idx, columns=idx)
		
		# Insights DataFrame
		insights = df.insights.apply(pd.DataFrame)
		insightsdf = pd.DataFrame()
		for i in insights.index:
			insightsdf = insightsdf.append(insights[i])
		insightsdf.reset_index(drop=True, inplace=True)
		item_sub = insightsdf['items'].dropna()
		item_sub = pd.io.json.json_normalize(item_sub.apply(pd.Series)[0],sep='_')
		insightsdf = insightsdf.join(item_sub, lsuffix='xxx', rsuffix='')
		insightsdf = insightsdf.drop(columns = 'items')

		# Team Records Dataframe
		teamrecordsdf = df.teamRecords.apply(pd.DataFrame)[0]

		# Player gamestats Dataframe
		gameplayerstats = df.gamePlayerStats.apply(pd.DataFrame)
		gameplayerstatsdf = pd.DataFrame()
		for i in gameplayerstats.index:
			gameplayerstatsdf = gameplayerstatsdf.append(gameplayerstats[i])
		gameplayerstatsdf.reset_index(drop=True, inplace=True)
		cols = [c for c in gameplayerstatsdf.columns if isinstance(gameplayerstatsdf.loc[0,c], dict)]
		for c in cols:
			gameplayerstatsdf = gameplayerstatsdf.join(pd.io.json.json_normalize(gameplayerstatsdf[c],sep='_'),rsuffix='_'+c)
		gameplayerstatsdf = gameplayerstatsdf.drop(columns=cols)

		# Live player Game Stats Dataframe
		liveplayergamestats = df.livePlayerGameStats.apply(pd.DataFrame)
		liveplayergamestatsdf = pd.DataFrame()
		for i in liveplayergamestats.index:
			liveplayergamestatsdf = liveplayergamestatsdf.append(liveplayergamestats[i])
		liveplayergamestatsdf = pd.io.json.json_normalize(liveplayergamestatsdf['gameStats'],sep='_').join(pd.io.json.\
			json_normalize(liveplayergamestatsdf['player'],sep='_'))

		# Team Rankings Dataframe is null for now

		# Videos Records Dataframe Save For Later
		videosrecords = df.videosRecords.apply(pd.DataFrame)
		videosrecordsdf = pd.DataFrame()
		for i in videosrecords.index:
			videosrecordsdf = videosrecordsdf.append(videosrecords[i])
		videosrecordsdf = videosrecordsdf.join(pd.io.json.json_normalize(videosrecordsdf['node'],sep='_'))
		videosrecordsdf = videosrecordsdf.drop(columns='node')

		# Media Objects Dataframe save for later

		# All Players Dataframe is null for now

		if frame == 'all':
			return gamedf, insightsdf, teamrecordsdf, gameplayerstatsdf, liveplayergamestatsdf	

		if frame == 'game':
			return gamedf

		if frame == 'insights':
			return insightsdf

		if frame == 'teamrecords':
			return teamrecordsdf

		if frame == 'gameplayerstats':
			return gameplayerstatsdf

		if frame == 'liveplayergamestats':
			return liveplayergamestatsdf

		if frame == 'teamrankings':
			return teamrankingsdf

		if frame == 'videosrecords':
			return videosrecordsdf

		if frame == 'mediaobjects':
			return mediaobjectsdf

		if frame == 'allplayers':
			return allplayersdf

		# Instance DF
		instanceDF = pd.concat([df[['env', 'gameId', 'isGameBookAvailable']], pd.io.json.json_normalize(df['shieldConfig'],sep='_')],axis=1)
		instanceDF.rename(columns={'_apiPath':'shieldConfig_apiPath'},inplace=True)
       
		# Game DF
		gameDF = pd.io.json.json_normalize(df['game'],sep='_').dropna(axis=1, how='all').sort_index(axis=1)
		gameDF[['gsisId','week_seasonValue','week_weekValue']] = gameDF[['gsisId','week_seasonValue','week_weekValue']].apply(pd.to_numeric,downcast='integer')

		# Game Details DF
		gameDetailsDF = pd.io.json.json_normalize(df['gameDetails'],sep='_').dropna(axis=1, how='all').sort_index(axis=1)
		gameDetailsDF[['distance','down','homePointsOvertimeTotal','homePointsQ1','homePointsQ2','homePointsQ3','homePointsQ4','homePointsTotal',
		'homeTimeoutsRemaining', 'homeTimeoutsUsed', 'visitorPointsOvertimeTotal', 'visitorPointsQ1', 'visitorPointsQ2', 'visitorPointsQ3', 'visitorPointsQ4', 
		'visitorPointsTotal', 'visitorTimeoutsRemaining', 'visitorTimeoutsUsed', 'yardsToGo']] = gameDetailsDF[['distance','down','homePointsOvertimeTotal',
		'homePointsQ1','homePointsQ2','homePointsQ3','homePointsQ4','homePointsTotal', 'homeTimeoutsRemaining', 'homeTimeoutsUsed', 
		'visitorPointsOvertimeTotal', 'visitorPointsQ1', 'visitorPointsQ2', 'visitorPointsQ3', 'visitorPointsQ4', 'visitorPointsTotal', 
		'visitorTimeoutsRemaining', 'visitorTimeoutsUsed', 'yardsToGo']].apply(pd.to_numeric,downcast='integer')

		# Drives DF
		drivesDF = pd.concat([df['gameDetails'].apply(pd.Series)['drives'],df['gameId']],axis=1).set_index('gameId').drives.apply(pd.Series).stack() \
		.reset_index(level=1,drop=True).to_frame('drives').reset_index()
		drivesDF = pd.concat([drivesDF['gameId'],pd.io.json.json_normalize(drivesDF['drives'],sep='_')],axis=1).dropna(axis=1, how='all').sort_index(axis=1)
		drivesDF[['firstDowns','orderSequence','playCount','playIdEnded','playIdStarted','playSeqEnded','playSeqStarted','quarterEnd','quarterStart','yards',
		'yardsPenalized']] = drivesDF[['firstDowns','orderSequence','playCount','playIdEnded','playIdStarted','playSeqEnded','playSeqStarted','quarterEnd',
		'quarterStart','yards', 'yardsPenalized']].apply(pd.to_numeric,downcast='integer')

		# Scoring Summaries DF
		scoringSummariesDF = pd.concat([df['gameDetails'].apply(pd.Series)['scoringSummaries'],df['gameId']],axis=1).set_index('gameId').scoringSummaries\
		.apply(pd.Series).stack().reset_index(level=1,drop=True).to_frame('scoringSummaries').reset_index()
		scoringSummariesDF = pd.concat([scoringSummariesDF['gameId'],pd.io.json.json_normalize(scoringSummariesDF['scoringSummaries'],sep='_')],axis=1)\
		.dropna(axis=1, how='all').sort_index(axis=1)
		scoringSummariesDF[['homeScore','patPlayId','playId','visitorScore']] = scoringSummariesDF[['homeScore','patPlayId','playId',
		'visitorScore']].apply(pd.to_numeric,downcast='integer')

		# Plays DF
		playsDF = pd.concat([df['gameDetails'].apply(pd.Series)['plays'],df['gameId']],axis=1).set_index('gameId').plays.apply(pd.Series)\
		.stack().reset_index(level=1,drop=True).to_frame('plays').reset_index()
		playsDF = pd.concat([playsDF['gameId'],pd.io.json.json_normalize(playsDF['plays'],sep='_')],axis=1).dropna(axis=1, how='all').sort_index(axis=1)
		playsDF[['down','driveNetYards','drivePlayCount','driveSequenceNumber','orderSequence','playId','quarter','yards','yardsToGo']] = playsDF[['down',
		'driveNetYards','drivePlayCount','driveSequenceNumber','orderSequence','playId','quarter','yards','yardsToGo']].apply(pd.to_numeric,downcast='integer')
		
		# Play Stats DF
		playStatsDF = pd.concat([playsDF['playStats'].apply(pd.Series),playsDF[['gameId','playId']]],axis=1).set_index(['gameId','playId']).stack()\
		.reset_index(level=2,drop=True).to_frame('playStats').reset_index()
		playStatsDF = pd.concat([playsDF[['gameId','playId']],pd.io.json.json_normalize(playStatsDF['playStats'],sep='_')],axis=1).dropna(axis=1, how='all')\
		.sort_index(axis=1)
		playStatsDF[['playId','statId','yards']] = playStatsDF[['playId','statId','yards']].apply(pd.to_numeric,downcast='integer')
		
		# Player Stats DF
		playerStatsDF = df['gamePlayerStats'].apply(pd.Series).stack().reset_index(level=1,drop=True).to_frame('gamePlayerStats').reset_index(drop=True)
		playerStatsDF = pd.io.json.json_normalize(playerStatsDF['gamePlayerStats'], sep='_').dropna(axis=1, how='all').sort_index(axis=1)
		playerStatsDF[['gameStats_defensiveAssists', 'gameStats_defensiveForcedFumble', 'gameStats_defensiveInterceptions', 
		'gameStats_defensiveInterceptionsYards', 'gameStats_defensivePassesDefensed', 'gameStats_defensiveSacks', 'gameStats_defensiveSafeties', 
		'gameStats_defensiveSoloTackles', 'gameStats_defensiveTotalTackles', 'gameStats_fumblesLost', 
		'gameStats_fumblesTotal', 'gameStats_kickReturns', 'gameStats_kickReturnsAverageYards', 'gameStats_kickReturnsLong', 'gameStats_kickReturnsTouchdowns', 
		'gameStats_kickReturnsYards', 'gameStats_kickingFgAtt', 'gameStats_kickingFgLong', 'gameStats_kickingFgMade', 'gameStats_kickingXkAtt', 
		'gameStats_kickingXkMade', 'gameStats_kickoffReturnsTouchdowns', 'gameStats_kickoffReturnsYards', 'gameStats_opponentFumbleRecovery', 
		'gameStats_passingAttempts', 'gameStats_passingCompletions', 'gameStats_passingInterceptions', 'gameStats_passingTouchdowns', 'gameStats_passingYards', 
		'gameStats_puntReturns', 'gameStats_puntReturnsAverageYards', 'gameStats_puntReturnsLong', 'gameStats_puntReturnsTouchdowns', 
		'gameStats_puntingAverageYards', 'gameStats_puntingLong', 'gameStats_puntingPunts', 'gameStats_puntingPuntsInside20', 'gameStats_receivingReceptions', 
		'gameStats_receivingTarget', 'gameStats_receivingTouchdowns', 'gameStats_receivingYards', 'gameStats_rushingAttempts', 'gameStats_rushingAverageYards', 
		'gameStats_rushingTouchdowns', 'gameStats_rushingYards', 'gameStats_totalPointsScored', 'gameStats_touchdownsDefense', 
		'player_jerseyNumber']] = playerStatsDF[['gameStats_defensiveAssists', 'gameStats_defensiveForcedFumble', 'gameStats_defensiveInterceptions', 
		'gameStats_defensiveInterceptionsYards', 'gameStats_defensivePassesDefensed', 'gameStats_defensiveSacks', 'gameStats_defensiveSafeties', 
		'gameStats_defensiveSoloTackles', 'gameStats_defensiveTotalTackles', 'gameStats_fumblesLost', 
		'gameStats_fumblesTotal', 'gameStats_kickReturns', 'gameStats_kickReturnsAverageYards', 'gameStats_kickReturnsLong', 'gameStats_kickReturnsTouchdowns', 
		'gameStats_kickReturnsYards', 'gameStats_kickingFgAtt', 'gameStats_kickingFgLong', 'gameStats_kickingFgMade', 'gameStats_kickingXkAtt', 
		'gameStats_kickingXkMade', 'gameStats_kickoffReturnsTouchdowns', 'gameStats_kickoffReturnsYards', 'gameStats_opponentFumbleRecovery', 
		'gameStats_passingAttempts', 'gameStats_passingCompletions', 'gameStats_passingInterceptions', 'gameStats_passingTouchdowns', 'gameStats_passingYards', 
		'gameStats_puntReturns', 'gameStats_puntReturnsAverageYards', 'gameStats_puntReturnsLong', 'gameStats_puntReturnsTouchdowns', 
		'gameStats_puntingAverageYards', 'gameStats_puntingLong', 'gameStats_puntingPunts', 'gameStats_puntingPuntsInside20', 'gameStats_receivingReceptions', 
		'gameStats_receivingTarget', 'gameStats_receivingTouchdowns', 'gameStats_receivingYards', 'gameStats_rushingAttempts', 'gameStats_rushingAverageYards', 
		'gameStats_rushingTouchdowns', 'gameStats_rushingYards', 'gameStats_totalPointsScored', 'gameStats_touchdownsDefense', 
		'player_jerseyNumber']].apply(pd.to_numeric)

		# Remove expanded columns from Game Details DF and Plays DF
		cols = [c for c in gameDetailsDF.columns if not isinstance(gameDetailsDF.loc[0,c],list)]
		gameDetailsDF = gameDetailsDF[cols].sort_index(axis=1)

		cols = [c for c in playsDF.columns if not isinstance(playsDF.loc[0,c],list)]
		playsDF = playsDF[cols].sort_index(axis=1)

		# Standings DF
		standingsDF = df['standings'].apply(pd.Series).edges.apply(pd.Series).stack().apply(pd.Series)
		standingsDF = pd.DataFrame(df['standings'][0]).edges.apply(pd.Series)
		standingsDF = pd.io.json.json_normalize(standingsDF['node'],sep='_')
		cols = [c for c in standingsDF.columns if not isinstance(standingsDF.loc[0,c],list)]
		standingsDF = pd.concat([standingsDF.teamRecords.apply(pd.Series),standingsDF[cols]],axis=1).set_index(cols).stack().reset_index(level=len(cols),\
		drop=True).to_frame('standings').reset_index()
		standingsDF = pd.concat([standingsDF[cols],pd.io.json.json_normalize(standingsDF['standings'])],axis=1).dropna(axis=1, how='all').sort_index(axis=1)
		standingsDF[['conferenceRank', 'overallLoss', 'overallTie', 'overallWin', 'week_weekOrder', 'week_weekValue']] = standingsDF[['conferenceRank', 
		'overallLoss', 'overallTie', 'overallWin', 'week_weekOrder', 'week_weekValue']].apply(pd.to_numeric,downcast='integer')
		standingsDF.drop_duplicates(subset=['teamId','week_id'],inplace=True)

		# Team Stats DF
		teamStatsDF = pd.concat([
			pd.concat([df['gameId'],pd.io.json.json_normalize(df['teamStats'].apply(pd.Series).homeTeamStats.apply(pd.Series).stack(),sep='_')],axis=1),
			pd.concat([df['gameId'],pd.io.json.json_normalize(df['teamStats'].apply(pd.Series).awayTeamStats.apply(pd.Series).stack(),sep='_')],axis=1)
			],axis=0).reset_index(drop=True).dropna(axis=1, how='all').sort_index(axis=1)
		teamStatsDF[['opponentGameStats_down3rdAttempted', 'opponentGameStats_down3rdFdMade', 'opponentGameStats_fumblesLost', 'opponentGameStats_gamesPlayed', 
		'opponentGameStats_passingAverageYards', 'opponentGameStats_passingInterceptions', 'opponentGameStats_passingNetYards', 
		'opponentGameStats_passingSacked', 'opponentGameStats_rushingAverageYards', 'opponentGameStats_rushingYards', 'opponentGameStats_scrimmageYds',
		'opponentGameStats_totalPointsScored', 'teamGameStats_down3rdAttempted', 'teamGameStats_down3rdFdMade', 'teamGameStats_fumblesLost', 
		'teamGameStats_gamesPlayed', 'teamGameStats_passingAverageYards', 'teamGameStats_passingInterceptions', 'teamGameStats_passingNetYards', 
		'teamGameStats_passingSacked', 'teamGameStats_penaltiesTotal', 'teamGameStats_rushingAverageYards', 'teamGameStats_rushingYards', 
		'teamGameStats_scrimmagePlays', 'teamGameStats_scrimmageYds', 'teamGameStats_timeOfPossSeconds', 
		'teamGameStats_totalPointsScored']] = teamStatsDF[['opponentGameStats_down3rdAttempted', 'opponentGameStats_down3rdFdMade', 
		'opponentGameStats_fumblesLost', 'opponentGameStats_gamesPlayed', 'opponentGameStats_passingAverageYards', 'opponentGameStats_passingInterceptions', 
		'opponentGameStats_passingNetYards', 'opponentGameStats_passingSacked', 'opponentGameStats_rushingAverageYards', 'opponentGameStats_rushingYards', 
		'opponentGameStats_scrimmageYds', 'opponentGameStats_totalPointsScored', 'teamGameStats_down3rdAttempted', 'teamGameStats_down3rdFdMade', 
		'teamGameStats_fumblesLost', 'teamGameStats_gamesPlayed', 'teamGameStats_passingAverageYards', 'teamGameStats_passingInterceptions', 
		'teamGameStats_passingNetYards', 'teamGameStats_passingSacked', 'teamGameStats_penaltiesTotal', 'teamGameStats_rushingAverageYards', 
		'teamGameStats_rushingYards', 'teamGameStats_scrimmagePlays', 'teamGameStats_scrimmageYds', 'teamGameStats_timeOfPossSeconds', 
		'teamGameStats_totalPointsScored']].apply(pd.to_numeric)

		return instanceDF, gameDF, gameDetailsDF, drivesDF, scoringSummariesDF, playsDF, playStatsDF, playerStatsDF, standingsDF, teamStatsDF

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
