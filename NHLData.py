#Import Data Libraries
import pandas as pd
import statscrape as ss

#Create URL Dictionary
data_dict = {
'teamsummary' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/teamsummary",
'franchisesummary' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/franchisesummary",
'powerplay' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/powerplay",
'penaltykill' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/penaltykill",
'penalties' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/penalties",
'realtime' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/realtime",
'faceoffsbystrength' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/faceoffsbystrength",
'shootout' : "http://www.nhl.com/stats/rest/grouped/team/shootout/game/shootout",
'teamsummaryshooting' : "http://www.nhl.com/stats/rest/grouped/team/shooting/game/teamsummaryshooting",
'teampercentages' : "http://www.nhl.com/stats/rest/grouped/team/shooting/game/teampercentages",
'teamscoring' : "http://www.nhl.com/stats/rest/grouped/team/core/game/teamscoring",
'faceoffsbyzone' : "http://www.nhl.com/stats/rest/grouped/team/core/game/faceoffsbyzone",
'shottype' : "http://www.nhl.com/stats/rest/grouped/team/core/game/shottype"
}

data_dict17 = {
'teamsummary17' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/teamsummary?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'franchisesummary17' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/franchisesummary?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'powerplay17' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/powerplay?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'penaltykill17' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/penaltykill?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'penalties17' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/penalties?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'realtime17' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/realtime?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'faceoffsbystrength17' : "http://www.nhl.com/stats/rest/grouped/team/basic/game/faceoffsbystrength?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'shootout17' : "http://www.nhl.com/stats/rest/grouped/team/shootout/game/shootout?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'teamsummaryshooting17' : "http://www.nhl.com/stats/rest/grouped/team/shooting/game/teamsummaryshooting?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'teampercentages17' : "http://www.nhl.com/stats/rest/grouped/team/shooting/game/teampercentages?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'teamscoring17' : "http://www.nhl.com/stats/rest/grouped/team/core/game/teamscoring?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'faceoffsbyzone17' : "http://www.nhl.com/stats/rest/grouped/team/core/game/faceoffsbyzone?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'shottype17' : "http://www.nhl.com/stats/rest/grouped/team/core/game/shottype?cayenneExp=gameDate%3E=%222016-08-01T06:00:00.000Z%22",
'schedule' : "https://statsapi.web.nhl.com/api/v1/schedule"
}

#Create list of Dictionary keys
key_list = list(data_dict.keys())
key_list17 = list(data_dict17.keys())

daily_sch = open('schedule.csv', 'w')
daily_sch.write(ss.NHL_Stats(data_dict17.get('schedule')).schedule_df())
daily_sch.close()

#Remove schedule
key_list17.remove('schedule')

for page in key_list:
    stat_table = open('%s.csv' % page,'w')
    stat_table.write(ss.NHL_Stats(data_dict.get(page)).statsDF())
    stat_table.close()

for page in key_list17:
    stat_table = open('%s.csv' % page,'w')
    stat_table.write(ss.NHL_Stats(data_dict.get(page)).statsDF())
    stat_table.close()
