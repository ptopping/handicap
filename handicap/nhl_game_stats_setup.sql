DROP TABLE nhl_game_teamsummary;

CREATE TABLE nhl_game_teamsummary
	(
	faceoffWinPctg NUMBER,
	faceoffsLost NUMBER,
	faceoffsWon NUMBER,
	gameDate TIMESTAMP,
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
	wins NUMBER
	);
