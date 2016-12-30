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
import json
import dateutil.parser
import datetime
import HockeyLoad as hl

NHL_csv = hl.create_csv(hl.dict_to_df(data_dict).rename_axis('dfIndex'))
NHL_sch = hl.create_csv(hl.schedule_df(data_dict.get('schedule')).rename_axis('dfIndex'))


#Write to external SQL table
file_text = open ('NHL_Stats16_17.csv', 'w')
file_text.write(NHL_csv)
file_text.close()
file_text = open ('NHL_Schedule.csv', 'w')
file_text.write(NHL_sch)
file_text.close()
