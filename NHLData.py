#Scrape NHL.com stats pages for game data and write to an external SQL Table for import into Oracle Database 11g
#NHL.com data stream URLs to import
data_dict = {
"teamsummary" : "http://www.nhl.com/stats/rest/grouped/team/basic/game/teamsummary?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"franchisesummary" : "http://www.nhl.com/stats/rest/grouped/team/basic/game/franchisesummary?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"powerplay" : "http://www.nhl.com/stats/rest/grouped/team/basic/game/powerplay?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"penaltykill" : "http://www.nhl.com/stats/rest/grouped/team/basic/game/penaltykill?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"penalties" : "http://www.nhl.com/stats/rest/grouped/team/basic/game/penalties?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"realtime" : "http://www.nhl.com/stats/rest/grouped/team/basic/game/realtime?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"faceoffsbystrength" : "http://www.nhl.com/stats/rest/grouped/team/basic/game/faceoffsbystrength?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"shootout" : "http://www.nhl.com/stats/rest/grouped/team/shootout/game/shootout?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"teamsummaryshooting" : "http://www.nhl.com/stats/rest/grouped/team/shooting/game/teamsummaryshooting?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"teampercentages" : "http://www.nhl.com/stats/rest/grouped/team/shooting/game/teampercentages?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"teamscoring" : "http://www.nhl.com/stats/rest/grouped/team/core/game/teamscoring?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"faceoffsbyzone" : "http://www.nhl.com/stats/rest/grouped/team/core/game/faceoffsbyzone?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"shottype" : "http://www.nhl.com/stats/rest/grouped/team/core/game/shottype?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
"schedule" : "https://statsapi.web.nhl.com/api/v1/schedule"}


# ?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22

#Python libraries to import
from urllib2 import urlopen
import pandas as pd
import numpy as np
import json
import dateutil.parser
import datetime
import HockeyLoad as hl

df_dict = {
'ts' : hl.stats_df(data_dict.get('teamsummary')),
'fs' : hl.stats_df(data_dict.get('franchisesummary')),
'pp' : hl.stats_df(data_dict.get('powerplay')),
'pk' : hl.stats_df(data_dict.get('penaltykill')),
'pen' : hl.stats_df(data_dict.get('penalties')),
'rt' : hl.stats_df(data_dict.get('realtime')),
'fos' : hl.stats_df(data_dict.get('faceoffsbystrength')),
'so' : hl.stats_df(data_dict.get('shootout')),
'shoot' : hl.stats_df(data_dict.get('teamsummaryshooting')),
'tp' : hl.stats_df(data_dict.get('teampercentages')),
'score' : hl.stats_df(data_dict.get('teamscoring')),
'foz' : hl.stats_df(data_dict.get('faceoffsbyzone')),
'st' : hl.stats_df(data_dict.get('shottype'))}

merge1 = hl.df_join(df_dict.get('ts'),df_dict.get('fs'))
merge2 = hl.df_join(merge1,df_dict.get('pp'))
merge3 = hl.df_join(merge2,df_dict.get('pk'))
merge4 = hl.df_join(merge3,df_dict.get('pen'))
merge5 = hl.df_join(merge4,df_dict.get('rt'))
merge6 = hl.df_join(merge5,df_dict.get('fos'))
merge7 = hl.df_join(merge6,df_dict.get('shoot'))
merge8 = hl.df_join(merge7,df_dict.get('tp'))
merge9 = hl.df_join(merge8,df_dict.get('score'))
merge10 = hl.df_join(merge9,df_dict.get('foz'))
merge11 = hl.df_join(merge10,df_dict.get('st'))
NHL_csv = hl.create_csv(hl.df_join(merge11,df_dict.get('so')).rename_axis('dfIndex'))
NHL_sch = hl.create_csv(hl.schedule_df(data_dict.get('schedule')).rename_axis('dfIndex'))


#Write to external SQL table
file_text = open ('NHL_Stats16_17.csv', 'w')
file_text.write(NHL_csv)
file_text.close()
file_text = open ('NHL_Schedule.csv', 'w')
file_text.write(NHL_sch)
file_text.close()
