#Import Data Libraries
import pandas as pd
import statscrape as ss

#Addresses for NHL Historical Stats
teamsummary = "https://api.nhle.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=teamsummary&sort=[{%22property%22:%22points%22,%22direction%22:%22DESC%22},{%22property%22:%22wins%22,%22direction%22:%22DESC%22}]&cayenneExp=leagueId=133%20and%20gameDate%3E=%222019-10-02%22%20and%20gameDate%3C=%222020-04-04%2023:59:59%22%20and%20gameTypeId=2"
franchisesummary = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=franchisesummary&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId>=2"
powerplay = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=powerplay&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId>=2"
penaltykill = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=penaltykill&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId>=2"
penalties = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=penalties&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId>=2"
realtime = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=realtime&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId%3E=2"
faceoffsbystrength = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=faceoffsbystrength&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId%3E=2"
shootout = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=shootout&isGame=true&reportName=shootout&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId%3E=2"
teamgoalsbytype = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=teamgoalsbytype&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId>=2"
teamdaysbetweengames = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=teamdaysbetweengames&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId%3E=2"
teamsummaryshooting = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=shooting&isGame=true&reportName=teamsummaryshooting&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId%3E=2"
teampercentages = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=shooting&isGame=true&reportName=teampercentages&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId%3E=2"
teamscoring = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=core&isGame=true&reportName=teamscoring&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId>=2"
faceoffsbyzone = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=core&isGame=true&reportName=faceoffsbyzone&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId%3E=2"
shottype = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=core&isGame=true&reportName=shottype&cayenneExp=gameDate%3E=%222011-09-01%22%20and%20gameDate%3C=%222017-10-01%22%20and%20gameTypeId%3E=2"

#stats = [teamsummary,franchisesummary,powerplay,penaltykill,penalties,realtime,faceoffsbystrength,shootout,teamgoalsbytype,teamdaysbetweengames,teamsummaryshooting,teampercentages,teamscoring,faceoffsbyzone,shottype]

#Addressesses for NHL Current Year Stats
#teamsummary = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=teamsummary&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId>=2"
#franchisesummary = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=franchisesummary&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId>=2"
#powerplay = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=powerplay&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId>=2"
#penaltykill = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=penaltykill&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId>=2"
#penalties = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=penalties&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId>=2"
#realtime = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=realtime&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId%3E=2"
#faceoffsbystrength = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=faceoffsbystrength&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId%3E=2"
#shootout = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=shootout&isGame=true&reportName=shootout&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId%3E=2"
#teamgoalsbytype = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=teamgoalsbytype&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId>=2"
#teamdaysbetweengames = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=basic&isGame=true&reportName=teamdaysbetweengames&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId%3E=2"
#teamsummaryshooting = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=shooting&isGame=true&reportName=teamsummaryshooting&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId%3E=2"
#teampercentages = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=shooting&isGame=true&reportName=teampercentages&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId%3E=2"
#teamscoring = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=core&isGame=true&reportName=teamscoring&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId>=2"
#faceoffsbyzone = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=core&isGame=true&reportName=faceoffsbyzone&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId%3E=2"
#shottype = "http://www.nhl.com/stats/rest/team?isAggregate=false&reportType=core&isGame=true&reportName=shottype&cayenneExp=gameDate%3E=%222017-09-01%22%20and%20gameDate%3C=%222018-10-01%22%20and%20gameTypeId%3E=2"

schedule = "https://statsapi.web.nhl.com/api/v1/schedule"

#Create CSV of Data Tables --TO DO, Function to Iterate
daily_sch = open('schedule.csv', 'w')
daily_sch.write(ss.NHL_Stats(schedule).scheduleDF())
daily_sch.close()

join1 = ss.df_join(ss.NHL_Stats(teamsummary).statsDF(),ss.NHL_Stats(franchisesummary).statsDF())
join2 = ss.df_join(join1,ss.NHL_Stats(powerplay).statsDF())
join3 = ss.df_join(join2,ss.NHL_Stats(penaltykill).statsDF())
join4 = ss.df_join(join3,ss.NHL_Stats(penalties).statsDF())
join5 = ss.df_join(join4,ss.NHL_Stats(realtime).statsDF())
join6 = ss.df_join(join5,ss.NHL_Stats(faceoffsbystrength).statsDF())
join7 = ss.df_join(join6,ss.NHL_Stats(teamgoalsbytype).statsDF())
join8 = ss.df_join(join7,ss.NHL_Stats(teamdaysbetweengames).statsDF())
join9 = ss.df_join(join8,ss.NHL_Stats(teamsummaryshooting).statsDF())
join10 = ss.df_join(join9,ss.NHL_Stats(teampercentages).statsDF())
join11 = ss.df_join(join10,ss.NHL_Stats(teamscoring).statsDF())
join12 = ss.df_join(join11,ss.NHL_Stats(faceoffsbyzone).statsDF())
join13 = ss.df_join(join12,ss.NHL_Stats(shottype).statsDF())
join14 = ss.df_join(join13,ss.NHL_Stats(shootout).statsDF())

ts = open('NHLStats.csv', 'w')
ts.write(pd.DataFrame.to_csv(join14.rename_axis('dfIndex'), encoding = 'utf-8'))
ts.close()

