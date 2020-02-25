# Standard Library imports
import requests
import re
import json

#Third Party Imports
import pandas as pd
from bs4 import BeautifulSoup

class NHL_Stats(object):
    """
    Data structure for ETL process of NHL Game stats and|or schedule

    Parameters

    URL: str
        URL for NHL stat API

    None
    """
    def __init__(self, URL):
        self.URL = URL

    def statsDF(self):
        '''
        Connects to NHL Rest API and pulls data.

        Returns: df
            Pandas DataFrame
        '''
        r = requests.get(self.URL)
        df = pd.DataFrame(json.loads(r.text)['data'])
        return df

    def scheduleDF(self):
        '''
        Connects to NHL Rest API and pulls daily schedule.

        Returns: converted
            Pandas DataFrame of daily schedule
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
        df_fin = pd.DataFrame({'gameDate': df1['gameDate'], 
                               'visitor': df_away2['name'], 
                               'home': df_home2['name']})
        df_fin['gameDate'] = df_fin['gameDate'].apply(zulu_con)\
                                               .astype('datetime64[ns, MST]')
        converted = pd.DataFrame.to_csv(df_fin.rename_axis('dfIndex'),
                                        encoding='utf-8')
        return converted

class NFLStats(object):
    """
    Data structure for ETL process of NFL Game stats

    Parameters

    season_type: Str
        Must be one of three values: 'pre', 'reg', 'post' which is
        equivalent to Preseason, Regular Season and Post Season
        respectively
    season: Int
        Calendar year of the NFL season, only years 2001 and later are
        available
    week: Int
        Integer with a range of 1-17
    data: List-like
        Empty list filled by class methods
    """
    def __init__(self, season_type, season, week):
        self.season_type = season_type
        self.season = season
        self.week = week
        self.data = []
        if self.season_type == 'pre':
            self.url = 'http://www.nfl.com/schedules/{}/PRE{}'\
                       .format(self.season,self.week)
        if self.season_type == 'reg':
            self.url = 'http://www.nfl.com/schedules/{}/REG{}'\
                       .format(self.season,self.week)
        if self.season_type == 'post':
            self.url = 'http://www.nfl.com/schedules/{}/POST'\
                       .format(self.season)

    def scrape(self):
        """
        Scrapes NFL.com schedule and Game Center data and returns it to
        parent class
        """

        r = requests.get(self.url)
        soup = BeautifulSoup(r.text, 'html.parser')
        games = soup.select('.gc-btn')

        for g in games:
            url = g.get('href')
            r = requests.get(url)
            soup = BeautifulSoup(r.text, 'html.parser')
            m = re.search('\{(.*?})*',
                          soup.find('script', string=re.compile('INITIAL'))
                                                       .string)

            self.data.append(json.loads(m.group(0))['instance'])

    def prettify(self, df):
        '''
        Series of repeated functions to trim and organize dataframes

        Parameters

        toplevel: df
            Pandas dataframe

        Returns: df
            Pandas prettified dataframe

        Raises:
            ValueError: If toplevel not of specified value
        '''
        # Find and remove columns which are lists or NULL.  Either
        # types present issues for import into SQL
        cols = [c for c in df.columns if not df[c].apply(isinstance,
                                                         args=[list])
                                                  .any()]
        df = df[cols].dropna(how='all', axis = 1)

        # Oracle doesn't have a BOOL dtype so booleans must be converted
        bools = df.select_dtypes(include='bool').columns
        for b in bools:
            df[b] = df[b].astype(int)

        # Oganize for troubleshooting purposes
        df.columns = df.columns.str.upper()
        df.sort_index(axis=1, inplace=True)
            
        return df

    def create(self, frame):
        """
        Creates a dataframe from previously scraped data

        Parameters

        frame: str
            select which dataframe to return.  Choose from 'game',
            'gameDetails', 'scoringSummaries', 'drives', 'plays',
            'playStats', 'gamePlayerStats', 'teamStats', standings

        Raises: ValueError 
            If frame not one of specificed values

        """

        # Must run self.scrape first to populate self.data
        details = ['gameDetails', 'scoringSummaries', 'drives', 'plays',
                  'playStats']
        if frame in details:
            data = [d.get('gameDetails') for d in self.data]
            df = pd.io.json.json_normalize(data=data, sep='_')

            if frame == 'gameDetails':
                return self.prettify(df)

            if frame in ['scoringSummaries', 'drives']:
                # scoringSummaries and drives are both lists within
                # gameDetails.  Must iterate through gameDetails to
                # extract
                df2 = pd.DataFrame()
                for i in range(len(df)):
                    data=df[frame][i]
                    df3 = pd.io.json.json_normalize(data=data, sep='_')

                    # Which game the drive belongs to
                    df3['id'] = df.loc[i, 'id'] 
                    df2 = df2.append(df3, sort=False)

                return self.prettify(df2)

            if frame in ['plays', 'playStats']:
                # plays is a list within gameDetails with playStats
                # another nested list within plays
                df2 = pd.DataFrame()
                for i in range(len(df)):
                    data=df['plays'][i]
                    df3 = pd.io.json.json_normalize(data=data, sep='_')

                    # Which game the play belongs to
                    df3['id'] = df.loc[i, 'id'] 
                    df2 = df2.append(df3, sort=False)

                df2.reset_index(drop=True, inplace=True)

                if frame == 'plays':
                    # If frame is 'plays then no further work is needed
                    return self.prettify(df2)

                if frame == 'playStats':
                    df4 = pd.DataFrame()

                    for i in range(len(df2)):
                        # The opening play of each game is an empty
                        # value which will throw an AttributeError
                        try:
                            data=df2['playStats'][i]
                            df5 = pd.io.json.json_normalize(data=data,
                                                            sep='_')

                            # Which game and which play the playStat
                            # belongs to
                            df5['id'] = df2.loc[i, 'id']
                            df5['playId'] = df2.loc[i, 'playId']
                            df4 = df4.append(df5, sort=False)

                        except AttributeError:
                            dict_1 = {i:df2.loc[i,'id']}
                            dict_2 = {i:df2.loc[i, 'playId']}
                            df5 = pd.DataFrame({'id': dict_1,
                                                'playId': dict_2})
                            df4 = df4.append(df5, sort=False)

                    return self.prettify(df4)

        # Game DataFrame
        elif frame == 'game':
            data=[d.get('game') for d in self.data]
            df = pd.io.json.json_normalize(data=data, sep='_')
            df.reset_index(drop=True, inplace=True)

            return self.prettify(df)

        # Player Stats DataFrame
        elif frame == 'gamePlayerStats':
            df = pd.DataFrame()
            # gamePlayerStats is a nested list.  Must iterate through
            # it to extract stats
            for d in range(len(self.data)):
                data=self.data[d].get('gamePlayerStats')
                df = df.append(pd.io.json.json_normalize(data=data, sep='_'),
                               sort=False)
            df.reset_index(drop=True, inplace=True)

            return self.prettify(df)

        # Team Stats DataFrame
        elif frame == 'teamStats':
            # Team stats is a nested list with two components,
            # homeTeamStats and awayTeamStats
            data=[d.get('teamStats') for d in self.data]
            df = pd.io.json.json_normalize(data=data, sep='_')
            # Find gameIds for join operations
            ids = pd.Series([d.get('gameId') for d in self.data],
                            name='gameId') 
            # Extract home sats and away stats
            homestats = df.homeTeamStats.apply(pd.Series)[0]
            awaystats = df.awayTeamStats.apply(pd.Series)[0]
            df2 = pd.io.json.json_normalize(homestats, sep='_')
            df3 = pd.io.json.json_normalize(awaystats, sep='_')
            # Join the home stats datafraem and away stats dataframe
            df = df2.join(df3, lsuffix='_home', rsuffix='_away')
            # Join the combined stats dataframe on gameId
            df = df.join(ids)
            df.reset_index(drop=True, inplace=True)

            return self.prettify(df)

        # Standings DataFrame
        elif frame == 'standings':
            # Standings consist of several nested lists
            data=[d.get('standings') for d in self.data]
            df = pd.io.json.json_normalize(data=data, sep='_')
            data = pd.DataFrame(df['edges'][0])['node']
            #Only first result is needed as others are duplicates
            df = pd.io.json.json_normalize(data=data, sep='_')
            df2 = pd.DataFrame()
            
            for i in range(len(df)):
                df3 = pd.DataFrame(df.loc[i, 'teamRecords'])
                df3['week_id'] = df.loc[i, 'week_id']
                df2 = df2.append(df3, sort=False)
            df = df.merge(df2, how='left', left_on='week_id', 
                                right_on='week_id')
            df.reset_index(drop=True, inplace=True) 

            return self.prettify(df)

        else:
            return ValueError

def create_csv(frame, name):
    '''
    Converts DataFrame to csv
    Input = Pandas DataFrame
    Output = csv text
    '''
    converted = frame.to_csv(index=False,sep='|', encoding='utf-8',
                             line_terminator='///x')
    stat_table = open('d:\\depthandtaxes\\{}.csv'.format(name),'w')
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
    merged = pd.merge(df1, df2, on=cols, how='left')
    return merged
