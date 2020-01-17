DROP TABLE nfl_game_instance_load PURGE;
DROP TABLE nfl_game_load PURGE;
DROP TABLE nfl_game_details_load PURGE;
DROP TABLE nfl_game_drives_load PURGE;
DROP TABLE nfl_game_scores_load PURGE;
DROP TABLE nfl_game_plays_load PURGE;
DROP TABLE nfl_game_playstats_load PURGE;
DROP TABLE nfl_game_player_load PURGE;
DROP TABLE nfl_season_standings_load PURGE;
DROP TABLE nfl_game_teamstats_load PURGE;


CREATE TABLE nfl_game_instance_load
	(
	env VARCHAR2(6), 
	gameId VARCHAR2(37), 
	isGameBookAvailable VARCHAR2(6), 
	shieldConfig_apiPath VARCHAR2(20)
    )
ORGANIZATION EXTERNAL
	(
	TYPE ORACLE_LOADER
	DEFAULT DIRECTORY ext_dat_load
	ACCESS PARAMETERS
		(
		RECORDS DELIMITED BY '///x' skip=1
		fields terminated by '|'
		OPTIONALLY ENCLOSED BY '"' AND '"'
		MISSING FIELD VALUES ARE NULL
		)
	LOCATION ('nfl_game_instance.csv')
	)
REJECT LIMIT UNLIMITED;

CREATE TABLE nfl_game_load
	(
	awayTeam_abbreviation VARCHAR2(4),
	awayTeam_cityStateRegion VARCHAR2(21),
	awayTeam_franchise_currentLogo_url VARCHAR2(100),
	awayTeam_franchise_id VARCHAR2(37),
	awayTeam_fullName VARCHAR2(21),
	awayTeam_id VARCHAR2(37),
	awayTeam_nickName VARCHAR2(13),
	gameDetailId VARCHAR2(37),
	gameTime VARCHAR2(24),
	gsisId INTEGER,
	homeTeam_abbreviation VARCHAR2(4),
	homeTeam_cityStateRegion VARCHAR2(21),
	homeTeam_franchise_currentLogo_url VARCHAR2(100),
	homeTeam_franchise_id VARCHAR2(37),
	homeTeam_fullName VARCHAR2(21),
	homeTeam_id VARCHAR2(37),
	homeTeam_nickName VARCHAR2(13),
	id VARCHAR2(37),
	networkChannels VARCHAR2(32),
	ticketUrl VARCHAR2(256),
	venue_city VARCHAR2(16),
	venue_fullName VARCHAR2(42),
	venue_state VARCHAR2(3),
	week_id VARCHAR2(37),
	week_seasonType VARCHAR2(5),
	week_seasonValue INTEGER,
	week_weekType VARCHAR2(5),
	week_weekValue INTEGER
	)
ORGANIZATION EXTERNAL
	(
	TYPE ORACLE_LOADER
	DEFAULT DIRECTORY ext_dat_load
	ACCESS PARAMETERS
		(
		RECORDS DELIMITED BY '///x' skip=1
		fields terminated by '|'
		OPTIONALLY ENCLOSED BY '"' AND '"'
		MISSING FIELD VALUES ARE NULL
		)
	LOCATION ('nfl_game.csv')
	)
REJECT LIMIT UNLIMITED;

CREATE TABLE nfl_game_details_load
	(
	attendance VARCHAR2(8),
	distance INTEGER,
	down INTEGER,
	gameClock VARCHAR2(6),
	goalToGo VARCHAR2(6),
	homePointsOvertimeTotal INTEGER,
	homePointsQ1 INTEGER,
	homePointsQ2 INTEGER,
	homePointsQ3 INTEGER,
	homePointsQ4 INTEGER,
	homePointsTotal INTEGER,
	homeTeam_abbreviation VARCHAR2(4),
	homeTeam_nickName VARCHAR2(13),
	homeTimeoutsRemaining INTEGER,
	homeTimeoutsUsed INTEGER,
	id VARCHAR2(37),
	phase VARCHAR2(15),
	playReview VARCHAR2(6),
	possessionTeam_abbreviation VARCHAR2(4),
	possessionTeam_nickName VARCHAR2(13),
	redzone VARCHAR2(6),
	stadium VARCHAR2(36),
	startTime VARCHAR2(9),
	visitorPointsOvertimeTotal INTEGER,
	visitorPointsQ1 INTEGER,
	visitorPointsQ2 INTEGER,
	visitorPointsQ3 INTEGER,
	visitorPointsQ4 INTEGER,
	visitorPointsTotal INTEGER,
	visitorTeam_abbreviation VARCHAR2(4),
	visitorTeam_nickName VARCHAR2(13),
	visitorTimeoutsRemaining INTEGER,
	visitorTimeoutsUsed INTEGER,
	weather_shortDescription VARCHAR2(124),
	yardsToGo INTEGER
	)
ORGANIZATION EXTERNAL
	(
	TYPE ORACLE_LOADER
	DEFAULT DIRECTORY ext_dat_load
	ACCESS PARAMETERS
		(
		RECORDS DELIMITED BY '///x' skip=1
		fields terminated by '|'
		OPTIONALLY ENCLOSED BY '"' AND '"'
		MISSING FIELD VALUES ARE NULL
		)
	LOCATION ('nfl_game_details.csv')
	)
REJECT LIMIT UNLIMITED;

CREATE TABLE nfl_game_drives_load
	(
	endTransition VARCHAR2(18),
	endYardLine VARCHAR2(7),
	endedWithScore VARCHAR2(6),
	firstDowns INTEGER,
	gameClockEnd VARCHAR2(6),
	gameClockStart VARCHAR2(6),
	gameId VARCHAR2(37),
	howEndedDescription VARCHAR2(21),
	howStartedDescription VARCHAR2(20),
	inside20 VARCHAR2(6),
	orderSequence INTEGER,
	playCount INTEGER,
	playIdEnded INTEGER,
	playIdStarted INTEGER,
	playSeqEnded INTEGER,
	playSeqStarted INTEGER,
	possessionTeam_abbreviation VARCHAR2(4),
	possessionTeam_franchise_currentLogo_url VARCHAR2(100),
	possessionTeam_nickName VARCHAR2(13),
	quarterEnd INTEGER,
	quarterStart INTEGER,
	startTransition VARCHAR2(19),
	startYardLine VARCHAR2(7),
	timeOfPossession VARCHAR2(6),
	yards INTEGER,
	yardsPenalized INTEGER
	)
ORGANIZATION EXTERNAL
	(
	TYPE ORACLE_LOADER
	DEFAULT DIRECTORY ext_dat_load
	ACCESS PARAMETERS
		(
		RECORDS DELIMITED BY '///x' skip=1
		fields terminated by '|'
		OPTIONALLY ENCLOSED BY '"' AND '"'
		MISSING FIELD VALUES ARE NULL
		)
	LOCATION ('nfl_game_drives.csv')
	)
REJECT LIMIT UNLIMITED;

CREATE TABLE nfl_game_scores_load
	(
	gameId VARCHAR2(37),
	homeScore INTEGER,
	patPlayId INTEGER,
	playDescription VARCHAR2(96),
	playId INTEGER,
	visitorScore INTEGER
	)
ORGANIZATION EXTERNAL
	(
	TYPE ORACLE_LOADER
	DEFAULT DIRECTORY ext_dat_load
	ACCESS PARAMETERS
		(
		RECORDS DELIMITED BY '///x' skip=1
		fields terminated by '|'
		OPTIONALLY ENCLOSED BY '"' AND '"'
		MISSING FIELD VALUES ARE NULL
		)
	LOCATION ('nfl_game_scores.csv')
	)
REJECT LIMIT UNLIMITED;

CREATE TABLE nfl_game_plays_load
	(
	clockTime VARCHAR2(6),
	down INTEGER,
	driveNetYards INTEGER,
	drivePlayCount INTEGER,
	driveSequenceNumber INTEGER,
	driveTimeOfPossession VARCHAR2(6),
	endClockTime VARCHAR2(6),
	endYardLine VARCHAR2(8),
	firstDown VARCHAR2(6),
	gameId VARCHAR2(37),
	goalToGo VARCHAR2(6),
	isBigPlay VARCHAR2(6),
	nextPlayIsGoalToGo VARCHAR2(6),
	nextPlayType VARCHAR2(20),
	orderSequence INTEGER,
	penaltyOnPlay VARCHAR2(6),
	playClock INTEGER,
	playDeleted VARCHAR2(6),
	playDescription VARCHAR2(1055),
	playDescriptionWithJerseyNumbers VARCHAR2(1079),
	playId INTEGER,
	playType VARCHAR2(12),
	possessionTeam_abbreviation VARCHAR2(4),
	possessionTeam_franchise_currentLogo_url VARCHAR2(100),
	possessionTeam_nickName VARCHAR2(13),
	prePlayByPlay VARCHAR2(20),
	quarter INTEGER,
	scoringPlay VARCHAR2(6),
	scoringPlayType VARCHAR2(5),
	scoringTeam_abbreviation VARCHAR2(4),
	scoringTeam_id VARCHAR2(37),
	scoringTeam_nickName VARCHAR2(13),
	shortDescription VARCHAR2(1055),
	specialTeamsPlay VARCHAR2(6),
	stPlayType VARCHAR2(8),
	timeOfDay VARCHAR2(9),
	yardLine VARCHAR2(7),
	yards INTEGER,
	yardsToGo INTEGER
	)
ORGANIZATION EXTERNAL
	(
	TYPE ORACLE_LOADER
	DEFAULT DIRECTORY ext_dat_load
	ACCESS PARAMETERS
		(
		RECORDS DELIMITED BY '///x' skip=1
		fields terminated by '|'
		OPTIONALLY ENCLOSED BY '"' AND '"'
		MISSING FIELD VALUES ARE NULL
		)
	LOCATION ('nfl_game_plays.csv')
	)
REJECT LIMIT UNLIMITED;

CREATE TABLE nfl_game_playstats_load
	(
	gameId VARCHAR2(37),
	gsisPlayer_id VARCHAR2(37),
	playId INTEGER,
	playerName VARCHAR2(22),
	statId INTEGER,
	team_abbreviation VARCHAR2(4),
	team_id VARCHAR2(37),
	yards INTEGER
	)
ORGANIZATION EXTERNAL
	(
	TYPE ORACLE_LOADER
	DEFAULT DIRECTORY ext_dat_load
	ACCESS PARAMETERS
		(
		RECORDS DELIMITED BY '///x' skip=1
		fields terminated by '|'
		OPTIONALLY ENCLOSED BY '"' AND '"'
		MISSING FIELD VALUES ARE NULL
		)
	LOCATION ('nfl_game_playstats.csv')
	)
REJECT LIMIT UNLIMITED;

CREATE TABLE nfl_game_player_load
	(
	createdDate VARCHAR2(25),
	gameStats_defensiveAssists BINARY_DOUBLE,
	gameStats_defensiveForcedFumble BINARY_DOUBLE,
	gameStats_defensiveInterceptions BINARY_DOUBLE,
	gameStats_defensiveInterceptionsYards BINARY_DOUBLE,
	gameStats_defensivePassesDefensed BINARY_DOUBLE,
	gameStats_defensiveSacks BINARY_DOUBLE,
	gameStats_defensiveSafeties BINARY_DOUBLE,
	gameStats_defensiveSoloTackles BINARY_DOUBLE,
	gameStats_defensiveTotalTackles BINARY_DOUBLE,
	gameStats_fumblesLost BINARY_DOUBLE,
	gameStats_fumblesTotal BINARY_DOUBLE,
	gameStats_kickReturns BINARY_DOUBLE,
	gameStats_kickReturnsAverageYards BINARY_DOUBLE,
	gameStats_kickReturnsLong BINARY_DOUBLE,
	gameStats_kickReturnsTouchdowns BINARY_DOUBLE,
	gameStats_kickReturnsYards BINARY_DOUBLE,
	gameStats_kickingFgAtt BINARY_DOUBLE,
	gameStats_kickingFgLong BINARY_DOUBLE,
	gameStats_kickingFgMade BINARY_DOUBLE,
	gameStats_kickingXkAtt BINARY_DOUBLE,
	gameStats_kickingXkMade BINARY_DOUBLE,
	gameStats_kickoffReturnsTouchdowns BINARY_DOUBLE,
	gameStats_kickoffReturnsYards BINARY_DOUBLE,
	gameStats_opponentFumbleRecovery BINARY_DOUBLE,
	gameStats_passingAttempts BINARY_DOUBLE,
	gameStats_passingCompletions BINARY_DOUBLE,
	gameStats_passingInterceptions BINARY_DOUBLE,
	gameStats_passingTouchdowns BINARY_DOUBLE,
	gameStats_passingYards BINARY_DOUBLE,
	gameStats_puntReturns BINARY_DOUBLE,
	gameStats_puntReturnsAverageYards BINARY_DOUBLE,
	gameStats_puntReturnsLong BINARY_DOUBLE,
	gameStats_puntReturnsTouchdowns BINARY_DOUBLE,
	gameStats_puntingAverageYards BINARY_DOUBLE,
	gameStats_puntingLong BINARY_DOUBLE,
	gameStats_puntingPunts BINARY_DOUBLE,
	gameStats_puntingPuntsInside20 BINARY_DOUBLE,
	gameStats_receivingReceptions BINARY_DOUBLE,
	gameStats_receivingTarget BINARY_DOUBLE,
	gameStats_receivingTouchdowns BINARY_DOUBLE,
	gameStats_receivingYards BINARY_DOUBLE,
	gameStats_rushingAttempts BINARY_DOUBLE,
	gameStats_rushingAverageYards BINARY_DOUBLE,
	gameStats_rushingTouchdowns BINARY_DOUBLE,
	gameStats_rushingYards BINARY_DOUBLE,
	gameStats_totalPointsScored BINARY_DOUBLE,
	gameStats_touchdownsDefense BINARY_DOUBLE,
	game_id VARCHAR2(37),
	id VARCHAR2(37),
	lastModifiedDate VARCHAR2(25),
	player_currentTeam_abbreviation VARCHAR2(4),
	player_currentTeam_nickName VARCHAR2(11),
	player_jerseyNumber INTEGER,
	player_person_displayName VARCHAR2(30),
	player_person_firstName VARCHAR2(18),
	player_person_headshot_asset_url VARCHAR2(97),
	player_person_lastName VARCHAR2(20),
	player_position VARCHAR2(4),
	season_id VARCHAR2(37),
	week_id VARCHAR2(37)	
	)
ORGANIZATION EXTERNAL
	(
	TYPE ORACLE_LOADER
	DEFAULT DIRECTORY ext_dat_load
	ACCESS PARAMETERS
		(
		RECORDS DELIMITED BY '///x' skip=1
		fields terminated by '|'
		OPTIONALLY ENCLOSED BY '"' AND '"'
		MISSING FIELD VALUES ARE NULL
		)
	LOCATION ('nfl_game_player.csv')
	)
REJECT LIMIT UNLIMITED;


CREATE TABLE nfl_season_standings_load
	(
	conferenceRank INTEGER,
	division VARCHAR2(12),
	fullName VARCHAR2(21),
	id VARCHAR2(37),
	nickName VARCHAR2(11),
	overallLoss INTEGER,
	overallTie INTEGER,
	overallWin INTEGER,
	teamId VARCHAR2(37),
	week_id VARCHAR2(37),
	week_seasonType VARCHAR2(4),
	week_season_season INTEGER,
	week_weekOrder INTEGER,
	week_weekType VARCHAR2(4),
	week_weekValue INTEGER
	)
ORGANIZATION EXTERNAL
	(
	TYPE ORACLE_LOADER
	DEFAULT DIRECTORY ext_dat_load
	ACCESS PARAMETERS
		(
		RECORDS DELIMITED BY '///x' skip=1
		fields terminated by '|'
		OPTIONALLY ENCLOSED BY '"' AND '"'
		MISSING FIELD VALUES ARE NULL
		)
	LOCATION ('nfl_season_standings.csv')
	)
REJECT LIMIT UNLIMITED;

CREATE TABLE nfl_game_teamstats_load
	(
	gameId VARCHAR2(37),
	opponentGameStats_down3rdAttempted INTEGER,
	opponentGameStats_down3rdFdMade INTEGER,
	opponentGameStats_fumblesLost INTEGER,
	opponentGameStats_gamesPlayed INTEGER,
	opponentGameStats_passingAverageYards BINARY_DOUBLE,
	opponentGameStats_passingInterceptions INTEGER,
	opponentGameStats_passingNetYards INTEGER,
	opponentGameStats_passingSacked INTEGER,
	opponentGameStats_rushingAverageYards BINARY_DOUBLE,
	opponentGameStats_rushingYards INTEGER,
	opponentGameStats_scrimmageYds INTEGER,
	opponentGameStats_totalPointsScored INTEGER,
	teamGameStats_down3rdAttempted INTEGER,
	teamGameStats_down3rdFdMade INTEGER,
	teamGameStats_fumblesLost INTEGER,
	teamGameStats_gamesPlayed INTEGER,
	teamGameStats_passingAverageYards BINARY_DOUBLE,
	teamGameStats_passingInterceptions INTEGER,
	teamGameStats_passingNetYards INTEGER,
	teamGameStats_passingSacked INTEGER,
	teamGameStats_penaltiesTotal INTEGER,
	teamGameStats_rushingAverageYards BINARY_DOUBLE,
	teamGameStats_rushingYards INTEGER,
	teamGameStats_scrimmagePlays INTEGER,
	teamGameStats_scrimmageYds INTEGER,
	teamGameStats_timeOfPossSeconds INTEGER,
	teamGameStats_totalPointsScored INTEGER,
	team_abbreviation VARCHAR2(4)
	)
ORGANIZATION EXTERNAL
	(
	TYPE ORACLE_LOADER
	DEFAULT DIRECTORY ext_dat_load
	ACCESS PARAMETERS
		(
		RECORDS DELIMITED BY '///x' skip=1
		fields terminated by '|'
		OPTIONALLY ENCLOSED BY '"' AND '"'
		MISSING FIELD VALUES ARE NULL
		)
	LOCATION ('nfl_game_teamstats.csv')
	)
REJECT LIMIT UNLIMITED;

