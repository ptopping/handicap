#Parse Datetime string into python ISO 8601 datetime object
zulu_con = lambda x: dateutil.parser.parse(x)

#Load JSON object into Python
def load_url(URL):
	'''
	Input = NHL.com  URL
	Output = JSON object
	'''
 	response = urlopen(URL)
 	data = response.read().decode("utf-8")
 	json_obj = json.loads(data)
 	return json_obj

#Create Pandas DataFrame from NHL Stats pages
def stats_df(url):
	'''
	Input = JSON obj passed through load_url
	Output = Pandas DataFrame with time conversion
	'''
	val = load_url(url).values()
	lev = val[1]
	df = pd.DataFrame(lev)
	df['gameDate'] = df['gameDate'].apply(zulu_con).astype('datetime64[ns, MST]')
	return df

#Create new dataframe from subcolumn in dataframe
#def df_decompile(df_in,col):
#	'''
#	Input = DataFrame, Column name
#	Output = Truncated dataframe of column
#	'''
#	lst = list(df_in['col'])
#	df_out = pd.DataFrame(lst)
#	return df_out

#Load NHL Schedule API
def schedule_df(url):
	'''
	Input = NHL.com schedule api
	Output = Pandas DataFrame of daily schedule
	'''
	list_2 = []
	dict_1 = load_url(url).get('dates')[0]
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
	return df_fin

#Convert DataFrame to csv for export
def create_csv(frame):
	'''
	Input = Pandas DataFrame
	Output = csv text
	'''
	converted = pd.DataFrame.to_csv(frame, encoding = 'utf-8')
	return converted

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

def dict_to_df(dct):
    key_list = list(dct.keys())
    key_list.remove('schedule')
    list_len = len(key_list)
    ndx = 2
    df = df_join(stats_df(dct.get(key_list[0])),stats_df(dct.get(key_list[1])))
    while ndx < list_len:
        df = df_join(df,stats_df(dct.get(key_list[ndx])))
        ndx += 1
    return df

# Unused functions kept for future reference/versions
# time_mst = lambda x: x - datetime.timedelta(hours=5)
# date_str = lambda x: x.isoformat()
# df['gameDate'].apply(zulu_con).astype('datetime64[ns, MST]').apply(date_str)
# cols = ['faceoffsLost','faceoffsWon','gameDate','gameId','gameLocationCode','gamesPlayed','goalsAgainst', 'goalsFor','opponentTeamAbbrev','points','ppGoalsAgainst','ppGoalsFor','ppOpportunities','shNumTimes','shotsAgainst','shotsFor','teamAbbrev','teamFullName','teamId','wins']
# dfneed = pd.DataFrame(df, columns = cols)
