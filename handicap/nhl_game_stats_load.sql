DROP TABLE nhl_game_teamsummary_load;

CREATE TABLE nhl_game_teamsummary_load
	(
	faceoffWinPctg NUMBER,
	faceoffsLost NUMBER,
	faceoffsWon NUMBER,
	gameDate VARCHAR2(26),
	gameId INTEGER,
	gameLocationCode VARCHAR2(3),
	gamesPlayed NUMBER,
	goalsAgainst NUMBER,
	goalsFor NUMBER,
	losses NUMBER,
	opponentTeamAbbrev VARCHAR2(4),
	otLosses NUMBER,
	penaltyKillPctg NUMBER,
	points NUMBER,
	ppGoalsAgainst NUMBER,
	ppGoalsFor NUMBER,
	ppOpportunities NUMBER,
	ppPctg NUMBER,
	shNumTimes NUMBER,
	shootoutGamesLost NUMBER,
	shootoutGamesWon NUMBER,
	shotsAgainst NUMBER,
	shotsFor NUMBER,
	teamAbbrev VARCHAR2(4),
	teamFullName VARCHAR2(50),
	teamId INTEGER,
	ties NUMBER,
	wins VARCHAR2(2)
	)
ORGANIZATION EXTERNAL
	(
	TYPE ORACLE_LOADER
	DEFAULT DIRECTORY ext_dat_load
	ACCESS PARAMETERS
		(
		RECORDS DELIMITED BY NEWLINE skip=1
		fields terminated by '|'
		OPTIONALLY ENCLOSED BY '"' AND '"'
		MISSING FIELD VALUES ARE NULL
		)
	LOCATION ('nhl_teamsummary.csv')
	)
REJECT LIMIT UNLIMITED;