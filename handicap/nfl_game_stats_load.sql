DROP TABLE nfl_game_drives_load PURGE;
DROP TABLE nfl_game_load PURGE;
DROP TABLE nfl_game_details_load PURGE;
DROP TABLE nfl_game_player_load PURGE;
DROP TABLE nfl_game_teamstats_load PURGE;
DROP TABLE nfl_game_playbyplay_load PURGE;

CREATE TABLE nfl_game_drives_load
	(
	endTransition varchar2(26),
	endYardLine varchar2(9),
	endedWithScore varchar2(26),
	firstDowns number,
	gameClockEnd varchar2(26),
	gameClockStart varchar2(26),
	howEndedDescription varchar2(26),
	howStartedDescription varchar2(26),
	inside20 varchar2(6),
	orderSequence number,
	playCount number,
	playIdEnded number,
	playIdStarted number,
	playSeqEnded number,
	playSeqStarted number,
	quarterEnd number,
	quarterStart number,
	realStartTime varchar2(26),
	startTransition varchar2(26),
	startYardLine varchar2(26),
	timeOfPossession varchar2(26),
	yards number,
	yardsPenalized number,
	abbreviation varchar2(4),
	nickName varchar2(26),
	gameId varchar2(50)
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
	LOCATION ('nfl_drives.csv')
	)
REJECT LIMIT UNLIMITED;

INSERT INTO nfl_game_drives (
	endTransition, endYardLine, endedWithScore, firstDowns, gameClockEnd, gameClockStart, howEndedDescription, howStartedDescription, inside20, orderSequence, playCount,
	playIdEnded, playIdStarted, playSeqEnded, playSeqStarted, quarterEnd, quarterStart, realStartTime, startTransition, startYardLine, timeOfPossession, yards, yardsPenalized,
	abbreviation, nickName, gameId	
)	(	SELECT endTransition, endYardLine, endedWithScore, firstDowns, TO_TIMESTAMP(gameClockEnd, 'MI:SS'), TO_TIMESTAMP(gameClockStart, 'MI:SS'), howEndedDescription,
	howStartedDescription, inside20, orderSequence, playCount, playIdEnded, playIdStarted, playSeqEnded, playSeqStarted, quarterEnd, quarterStart, realStartTime, startTransition,
	startYardLine, TO_DSINTERVAL('0 00:'||timeOfPossession), yards, yardsPenalized, abbreviation, nickName, gameId
FROM nfl_game_drives_load );

CREATE TABLE nfl_game_load
	(
	id VARCHAR2(50), 
	gameTime VARCHAR2(26), 
	gsisId INTEGER, 
	gameDetailId VARCHAR2(50), 
	awayabbreviation VARCHAR2(4),
	awayfullName VARCHAR2(26), 
	awayid VARCHAR2(50), 
	awaynickName VARCHAR2(26), 
	awaycityStateRegion VARCHAR2(26),
	homeabbreviation VARCHAR2(4), 
	homefullName VARCHAR2(26), 
	homeid VARCHAR2(50), 
	homenickName VARCHAR2(26),
	homecityStateRegion VARCHAR2(26), 
	seasonValue INTEGER, 
	weekId VARCHAR2(50), 
	seasonType VARCHAR2(4),
	weekValue INTEGER, 
	weekType VARCHAR2(4), 
	venuefullName VARCHAR2(50), 
	venuecity VARCHAR2(26), 
	venuestate VARCHAR2(4)
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
	LOCATION ('nfl_game.csv')
	)
REJECT LIMIT UNLIMITED;

CREATE TABLE nfl_game_details_load
	(
	id VARCHAR2(50),
	attendance VARCHAR2(10),
	homePointsOvertime VARCHAR2(26),
	homePointsTotal INTEGER,
	homePointsQ1 INTEGER,
	homePointsQ2 INTEGER,
	homePointsQ3 INTEGER,
	homePointsQ4 INTEGER,
	homeTimeoutsUsed INTEGER,
	homeTimeoutsRemaining INTEGER,
	stadium VARCHAR2(50),
	startTime VARCHAR2(10),
	visitorPointsOvertime VARCHAR2(26),
	visitorPointsOvertimeTotal INTEGER,
	visitorPointsQ1 INTEGER,
	visitorPointsQ2 INTEGER,
	visitorPointsQ3 INTEGER,
	visitorPointsQ4 INTEGER,
	visitorPointsTotal INTEGER,
	visitorTimeoutsUsed INTEGER,
	visitorTimeoutsRemaining INTEGER,
	homePointsOvertimeTotal INTEGER,
	homeabbreviation VARCHAR2(4),
	homenickName VARCHAR2(26),
	visitorabbreviation VARCHAR2(4),
	visitornickName VARCHAR2(26)
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
	LOCATION ('nfl_gamedetails.csv')
	)
REJECT LIMIT UNLIMITED;

CREATE TABLE nfl_game_player_load
	(
	createdDate VARCHAR2(26),
	id VARCHAR2(50),
	lastModifiedDate VARCHAR2(26),
	gameid VARCHAR2(50),
	defensiveAssists NUMBER,
	defensiveInterceptions NUMBER,
	defensiveInterceptionsYards NUMBER,
	defensiveForcedFumble NUMBER,
	defensivePassesDefensed NUMBER,
	defensiveSacks NUMBER,
	defensiveSafeties NUMBER,
	defensiveSoloTackles NUMBER,
	defensiveTotalTackles NUMBER,
	defensiveTacklesForALoss NUMBER,
	touchdownsDefense NUMBER,
	fumblesLost NUMBER,
	fumblesTotal NUMBER,
	kickReturns NUMBER,
	kickReturnsLong NUMBER,
	kickReturnsTouchdowns NUMBER,
	kickReturnsYards NUMBER,
	kickingFgAtt NUMBER,
	kickingFgLong NUMBER,
	kickingFgMade NUMBER,
	kickingXkAtt NUMBER,
	kickingXkMade NUMBER,
	passingAttempts NUMBER,
	passingCompletions NUMBER,
	passingTouchdowns NUMBER,
	passingYards NUMBER,
	passingInterceptions NUMBER,
	puntReturns NUMBER,
	puntingAverageYards NUMBER,
	puntingLong NUMBER,
	puntingPunts NUMBER,
	puntingPuntsInside20 NUMBER,
	receivingReceptions NUMBER,
	receivingTarget NUMBER,
	receivingTouchdowns NUMBER,
	receivingYards NUMBER,
	rushingAttempts NUMBER,
	rushingAverageYards NUMBER,
	rushingTouchdowns NUMBER,
	rushingYards NUMBER,
	kickoffReturnsTouchdowns NUMBER,
	kickoffReturnsYards NUMBER,
	puntReturnsLong NUMBER,
	opponentFumbleRecovery NUMBER,
	totalPointsScored NUMBER,
	kickReturnsAverageYards NUMBER,
	puntReturnsAverageYards NUMBER,
	puntReturnsTouchdowns NUMBER,
	position VARCHAR2(10),
	jerseyNumber INTEGER,
	abbreviation VARCHAR2(4),
	nickName VARCHAR2(26),
	displayName VARCHAR2(128),
	firstName VARCHAR2(50),
	lastName VARCHAR2(50),
	seasonid VARCHAR2(50),
	weekid VARCHAR2(50)
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
	LOCATION ('nfl_player.csv')
	)
REJECT LIMIT UNLIMITED;

CREATE TABLE nfl_game_playbyplay_load
	(
	clockTime varchar2(26),
	down number,
	driveNetYards number,
	drivePlayCount number,
	driveSequenceNumber number,
	driveTimeOfPossession varchar2(26),
	endClockTime timestamp,
	endYardLine varchar2(10),
	firstDown varchar2(10),
	goalToGo varchar2(10),
	isBigPlay varchar2(10),
	latestPlay varchar2(10),
	nextPlayIsGoalToGo varchar2(10),
	nextPlayType varchar2(26),
	orderSequence number,
	penaltyOnPlay varchar2(10),
	playClock number,
	playDeleted varchar2(10),
	playDescription varchar2(256),
	playDescriptionWithJerseyNumbers varchar2(256),
	playId number,
	playReviewStatus varchar2(10),
	playType varchar2(26),
	prePlayByPlay varchar2(26),
	quarter number,
	scoringPlay varchar2(10),
	scoringPlayType varchar2(10),
	scoringTeam varchar2(50),
	shortDescription varchar2(128),
	specialTeamsPlay varchar2(10),
	stPlayType varchar2(26),
	timeOfDay varchar2(26),
	yardLine varchar2(10),
	yards number,
	yardsToGo number,
	abbreviation varchar2(4),
	nickName varchar2(26),
	gameId varchar2(50)
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
	LOCATION ('nfl_plays.csv')
	)
REJECT LIMIT UNLIMITED;

INSERT INTO nfl_game_playbyplay (	
	clockTime, down, driveNetYards, drivePlayCount, driveSequenceNumber, driveTimeOfPossession, endClockTime, endYardLine, firstDown, goalToGo, isBigPlay, latestPlay, 
	nextPlayIsGoalToGo, nextPlayType, orderSequence, penaltyOnPlay, playClock, playDeleted, playDescription, playDescriptionWithJerseyNumbers, playId, playReviewStatus,	
	playType, prePlayByPlay, quarter, scoringPlay, scoringPlayType, scoringTeam, shortDescription, specialTeamsPlay, stPlayType, timeOfDay, yardLine, yards, yardsToGo, 
	abbreviation, nickName, gameId
)	(	SELECT	TO_TIMESTAMP(clockTime, 'MI:SS'), down,	driveNetYards, drivePlayCount, driveSequenceNumber, 
	CASE WHEN driveTimeOfPossession IS NULL THEN null
		ELSE TO_DSINTERVAL('0 00:'||driveTimeOfPossession) END, 
	TO_TIMESTAMP(endClockTime, 'MI:SS'), endYardLine, firstDown, goalToGo, isBigPlay, latestPlay, nextPlayIsGoalToGo, nextPlayType, orderSequence, penaltyOnPlay, playClock,
	playDeleted, playDescription, playDescriptionWithJerseyNumbers, playId, playReviewStatus, playType, prePlayByPlay, quarter, scoringPlay, scoringPlayType, scoringTeam, 
	shortDescription, specialTeamsPlay, stPlayType, TO_TIMESTAMP(timeOfDay, 'HH24:MI:SS'), yardLine, yards, yardsToGo, abbreviation, nickName, gameId
FROM nfl_game_playbyplay_load);

CREATE TABLE nfl_game_teamstats_load
	(
	opponentpassingNetYards number, 
	opponentscrimmageYds number,
	opponentpassingAverageYards number,
	opponentrushingAverageYards number,
	opponenttotalPointsScored number,
	opponentfumblesLost number,
	opponentpassingInterceptions number,
	opponentpassingSacked number,
	opponentdown3rdAttempted number,
	opponentdown3rdFdMade number,
	opponentgamesPlayed number,
	opponentrushingYards number,
	abbreviation varchar2(4),
	passingNetYards number,
	scrimmageYds number,
	passingAverageYards number,
	rushingAverageYards number,
	scrimmagePlays number,
	totalPointsScored number,
	fumblesLost number,
	rushingYards number,
	passingInterceptions number,
	passingSacked number,
	down3rdAttempted number,
	down3rdFdMade number,
	timeOfPossSeconds number,
	penaltiesTotal number,
	gamesPlayed number,
	gameId varchar2(50)
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
	LOCATION ('nfl_teamstats.csv')
	)
REJECT LIMIT UNLIMITED;

INSERT INTO nfl_game_teamstats (	
	opponentpassingNetYards, opponentscrimmageYds, opponentpassingAverageYards, opponentrushingAverageYards, opponenttotalPointsScored, opponentfumblesLost,
	opponentpassingInterceptions, opponentpassingSacked, opponentdown3rdAttempted, opponentdown3rdFdMade, opponentgamesPlayed, opponentrushingYards, abbreviation, 
	passingNetYards, scrimmageYds, passingAverageYards, rushingAverageYards, scrimmagePlays, totalPointsScored, fumblesLost, rushingYards, passingInterceptions, passingSacked,
	down3rdAttempted, down3rdFdMade, timeOfPossSeconds, penaltiesTotal, gamesPlayed, gameId
)	(	SELECT	opponentpassingNetYards, opponentscrimmageYds, opponentpassingAverageYards, opponentrushingAverageYards, opponenttotalPointsScored, opponentfumblesLost,
	opponentpassingInterceptions, opponentpassingSacked, opponentdown3rdAttempted, opponentdown3rdFdMade, opponentgamesPlayed, opponentrushingYards, abbreviation, 
	passingNetYards, scrimmageYds, passingAverageYards, rushingAverageYards, scrimmagePlays, totalPointsScored, fumblesLost, rushingYards, passingInterceptions, passingSacked,
	down3rdAttempted, down3rdFdMade, timeOfPossSeconds, penaltiesTotal, gamesPlayed, gameId
FROM nfl_game_teamstats_load);
