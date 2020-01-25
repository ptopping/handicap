MERGE INTO nfl_game_details gd
USING ( SELECT 	attendance, distance, down, TO_TIMESTAMP(gameClock, 'MI:SS')gameClock, goalToGo, 
	homePointsOvertimeTotal, homePointsQ1, homePointsQ2, homePointsQ3, homePointsQ4, homePointsTotal, homeTeam_abbreviation, 
	homeTeam_nickName, homeTimeoutsRemaining, homeTimeoutsUsed, id, phase, playReview, possessionTeam_abbreviation, possessionTeam_nickName, redzone, 
	stadium, TO_TIMESTAMP(startTime, 'HH24:MI:SS') startTime, visitorPointsOvertimeTotal, visitorPointsQ1, visitorPointsQ2, 
	visitorPointsQ3, visitorPointsQ4, visitorPointsTotal, visitorTeam_abbreviation, visitorTeam_nickName, visitorTimeoutsRemaining, visitorTimeoutsUsed, 
	weather_shortDescription, weather_shortDescription_Humidity, weather_shortDescription_Indoors, weather_shortDescription_Temp, 
	weather_shortDescription_wind_direction, weather_shortDescription_wind_speed, yardsToGo FROM nfl_game_details_load) gdl ON (
	gd.Id = gdl.Id
)
WHEN NOT MATCHED THEN
	INSERT ( 
		gd.attendance, gd.distance, gd.down, gd.gameClock, gd.goalToGo, gd.homePointsOvertimeTotal, gd.homePointsQ1, gd.homePointsQ2, gd.homePointsQ3, 
		gd.homePointsQ4, gd.homePointsTotal, gd.homeTeam_abbreviation, gd.homeTeam_nickName, gd.homeTimeoutsRemaining, gd.homeTimeoutsUsed, gd.id, gd.phase, 
		gd.playReview, gd.possessionTeam_abbreviation, gd.possessionTeam_nickName, gd.redzone, gd.stadium, gd.startTime, gd.visitorPointsOvertimeTotal, 
		gd.visitorPointsQ1, gd.visitorPointsQ2, gd.visitorPointsQ3, gd.visitorPointsQ4, gd.visitorPointsTotal, gd.visitorTeam_abbreviation, 
		gd.visitorTeam_nickName, gd.visitorTimeoutsRemaining, gd.visitorTimeoutsUsed, gd.weather_shortDescription, 
		-- gd.weather_shortDescription_Humidity, 
		-- gd.weather_shortDescription_Indoors, gd.weather_shortDescription_Temp, gd.weather_shortDescription_wind_direction, 
		-- gd.weather_shortDescription_wind_speed, 
		gd.yardsToGo
	)	VALUES ( 
		gdl.attendance, gdl.distance, gdl.down, gdl.gameClock, gdl.goalToGo, gdl.homePointsOvertimeTotal, gdl.homePointsQ1, gdl.homePointsQ2, 
		gdl.homePointsQ3, gdl.homePointsQ4, gdl.homePointsTotal, gdl.homeTeam_abbreviation, gdl.homeTeam_nickName, gdl.homeTimeoutsRemaining, 
		gdl.homeTimeoutsUsed, gdl.id, gdl.phase, gdl.playReview, gdl.possessionTeam_abbreviation, gdl.possessionTeam_nickName, gdl.redzone, 
		gdl.stadium, gdl.startTime, gdl.visitorPointsOvertimeTotal, gdl.visitorPointsQ1, gdl.visitorPointsQ2, gdl.visitorPointsQ3, gdl.visitorPointsQ4, 
		gdl.visitorPointsTotal, gdl.visitorTeam_abbreviation, gdl.visitorTeam_nickName, gdl.visitorTimeoutsRemaining, gdl.visitorTimeoutsUsed, 
		gdl.weather_shortDescription, 
		-- gdl.weather_shortDescription_Humidity, gdl.weather_shortDescription_Indoors, gdl.weather_shortDescription_Temp, 
		-- gdl.weather_shortDescription_wind_direction, gdl.weather_shortDescription_wind_speed, 
		gdl.yardsToGo
	);

MERGE INTO nfl_game_scores gs
USING ( SELECT 	homeScore, id, patPlayId, playDescription, playId, visitorScore FROM nfl_game_scores_load) gsl ON (
	gs.id = gsl.id AND gs.playId = gsl.playId
)
WHEN NOT MATCHED THEN
	INSERT (
		gs.homeScore, gs.id, gs.patPlayId, gs.playDescription, gs.playId, gs.visitorScore
	)	VALUES (
		gsl.homeScore, gsl.id, gsl.patPlayId, gsl.playDescription, gsl.playId, gsl.visitorScore
	);

MERGE INTO nfl_game_drives gd
USING ( SELECT endTransition, endYardLine, endedWithScore, firstDowns, TO_TIMESTAMP(gameClockEnd, 'MI:SS') gameClockEnd, 
	TO_TIMESTAMP(gameClockStart, 'MI:SS') gameClockStart, howEndedDescription, howStartedDescription, id, inside20, orderSequence, playCount, playIdEnded, 
	playIdStarted, playSeqEnded, playSeqStarted, possessionTeam_abbreviation, possessionTeam_franchise_currentLogo_url ,possessionTeam_nickName, quarterEnd, quarterStart, 
	startTransition, startYardLine, 
	TO_DSINTERVAL('0 00:'||timeOfPossession) timeOfPossession, yards, yardsPenalized FROM nfl_game_drives_load) gdl ON (
	gd.orderSequence = gdl.orderSequence AND gd.id = gdl.id
)
WHEN NOT MATCHED THEN
	INSERT (
		gd.endTransition, gd.endYardLine, gd.endedWithScore, gd.firstDowns, gd.gameClockEnd, gd.gameClockStart, gd.howEndedDescription, 
		gd.howStartedDescription, gd.id, gd.inside20, gd.orderSequence, gd.playCount, gd.playIdEnded, gd.playIdStarted, gd.playSeqEnded, gd.playSeqStarted, 
		gd.possessionTeam_abbreviation, gd.possessionTeam_franchise_currentLogo_url, gd.possessionTeam_nickName, gd.quarterEnd, gd.quarterStart, gd.startTransition, gd.startYardLine, 
		gd.timeOfPossession, gd.yards, gd.yardsPenalized
	)	VALUES (
		gdl.endTransition, gdl.endYardLine, gdl.endedWithScore, gdl.firstDowns, gdl.gameClockEnd, gdl.gameClockStart, gdl.howEndedDescription, 
		gdl.howStartedDescription, gdl.id, gdl.inside20, gdl.orderSequence, gdl.playCount, gdl.playIdEnded, gdl.playIdStarted, gdl.playSeqEnded, gdl.playSeqStarted, 
		gdl.possessionTeam_abbreviation, gdl.possessionTeam_franchise_currentLogo_url, gdl.possessionTeam_nickName, gdl.quarterEnd, gdl.quarterStart, gdl.startTransition, 
		gdl.startYardLine, gdl.timeOfPossession, gdl.yards, gdl.yardsPenalized
	);

MERGE INTO nfl_game g
USING ( SELECT 	awayTeam_abbreviation, awayTeam_cityStateRegion, awayTeam_franchise_currentLogo_url, awayTeam_franchise_id, 
	awayTeam_fullName, awayTeam_id, awayTeam_nickName, gameDetailId, TO_TIMESTAMP(gameTime, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') gameTime, gsisId, 
	homeTeam_abbreviation, homeTeam_cityStateRegion, homeTeam_franchise_currentLogo_url, homeTeam_franchise_id, homeTeam_fullName, homeTeam_id, 
	homeTeam_nickName, id, networkChannels, ticketUrl, venue_city, venue_fullName, venue_state, week_id, week_seasonType, week_seasonValue, week_weekType, 
	week_weekValue FROM nfl_game_load) gl ON (
	g.id = gl.id
)
WHEN NOT MATCHED THEN
	INSERT (  
		g.awayTeam_abbreviation, g.awayTeam_cityStateRegion, g.awayTeam_franchise_currentLogo_url, g.awayTeam_franchise_id, g.awayTeam_fullName, 
		g.awayTeam_id, g.awayTeam_nickName, g.gameDetailId, g.gameTime, g.gsisId, g.homeTeam_abbreviation, g.homeTeam_cityStateRegion, 
		g.homeTeam_franchise_currentLogo_url, g.homeTeam_franchise_id, g.homeTeam_fullName, g.homeTeam_id, g.homeTeam_nickName, g.id, g.networkChannels, 
		g.ticketUrl, g.venue_city, g.venue_fullName, g.venue_state, g.week_id, g.week_seasonType, g.week_seasonValue, g.week_weekType, 
		g.week_weekValue
	)	VALUES (
		gl.awayTeam_abbreviation, gl.awayTeam_cityStateRegion, gl.awayTeam_franchise_currentLogo_url, gl.awayTeam_franchise_id, gl.awayTeam_fullName, 
		gl.awayTeam_id, gl.awayTeam_nickName, gl.gameDetailId, gl.gameTime, gl.gsisId, gl.homeTeam_abbreviation, gl.homeTeam_cityStateRegion, 
		gl.homeTeam_franchise_currentLogo_url, gl.homeTeam_franchise_id, gl.homeTeam_fullName, gl.homeTeam_id, gl.homeTeam_nickName, gl.id, 
		gl.networkChannels, gl.ticketUrl, gl.venue_city, gl.venue_fullName, gl.venue_state, gl.week_id, gl.week_seasonType, gl.week_seasonValue, 
		gl.week_weekType, gl.week_weekValue
	);



MERGE INTO nfl_game_plays gp
USING ( SELECT  TO_TIMESTAMP(clockTime, 'MI:SS') clockTime,down, driveNetYards, drivePlayCount, driveSequenceNumber, 
	CASE WHEN driveTimeOfPossession IS NULL THEN NULL ELSE TO_DSINTERVAL('0 00:'||driveTimeOfPossession) END driveTimeOfPossession, 
	TO_TIMESTAMP(endClockTime, 'MI:SS') endClockTime, endYardLine, firstDown, gameId, goalToGo, isBigPlay, nextPlayIsGoalToGo, nextPlayType, 
	orderSequence, penaltyOnPlay, playClock, playDeleted, playDescription, playDescriptionWithJerseyNumbers, playId, playType, 
	possessionTeam_abbreviation, possessionTeam_franchise_currentLogo_url, possessionTeam_nickName, 
	prePlayByPlay, quarter, scoringPlay, scoringPlayType, scoringTeam_abbreviation, scoringTeam_id, scoringTeam_nickName, shortDescription, 
	specialTeamsPlay, stPlayType, TO_TIMESTAMP(timeOfDay, 'HH24:MI:SS') timeOfDay, yardLine, yards, yardsToGo FROM nfl_game_plays_load) gpl ON (
	gp.gameId = gpl.gameId AND gp.playId = gpl.playId
)
WHEN NOT MATCHED THEN
	INSERT ( 
		gp.clockTime, gp.down, gp.driveNetYards, gp.drivePlayCount, gp.driveSequenceNumber, gp.driveTimeOfPossession, gp.endClockTime, gp.endYardLine, 
		gp.firstDown, gp.gameId, gp.goalToGo, gp.isBigPlay, gp.nextPlayIsGoalToGo, gp.nextPlayType, gp.orderSequence, gp.penaltyOnPlay, 
		gp.playClock, gp.playDeleted, gp.playDescription, gp.playDescriptionWithJerseyNumbers, gp.playId, gp.playType, 
		gp.possessionTeam_abbreviation, gp.possessionTeam_franchise_currentLogo_url, gp.possessionTeam_nickName, 
		gp.prePlayByPlay, gp.quarter, gp.scoringPlay, gp.scoringPlayType, gp.scoringTeam_abbreviation, gp.scoringTeam_id, 
		gp.scoringTeam_nickName, gp.shortDescription, gp.specialTeamsPlay, gp.stPlayType, gp.timeOfDay, gp.yardLine, gp.yards, gp.yardsToGo    
	)	VALUES ( 
		gpl.clockTime, gpl.down, gpl.driveNetYards, gpl.drivePlayCount, gpl.driveSequenceNumber, gpl.driveTimeOfPossession, gpl.endClockTime, gpl.endYardLine, 
		gpl.firstDown, gpl.gameId, gpl.goalToGo, gpl.isBigPlay, gpl.nextPlayIsGoalToGo, gpl.nextPlayType, gpl.orderSequence, gpl.penaltyOnPlay, 
		gpl.playClock, gpl.playDeleted, gpl.playDescription, gpl.playDescriptionWithJerseyNumbers, gpl.playId, gpl.playType, 
		gpl.possessionTeam_abbreviation, gpl.possessionTeam_franchise_currentLogo_url, 
		gpl.possessionTeam_nickName, gpl.prePlayByPlay, gpl.quarter, gpl.scoringPlay, gpl.scoringPlayType, gpl.scoringTeam_abbreviation, 
		gpl.scoringTeam_id, gpl.scoringTeam_nickName, gpl.shortDescription, gpl.specialTeamsPlay, gpl.stPlayType, gpl.timeOfDay, gpl.yardLine, gpl.yards, 
		gpl.yardsToGo
	);

MERGE INTO nfl_game_playstats gps
USING ( SELECT 	gameId, gsisPlayer_id, playId, playerName, statId, team_abbreviation, team_id, yards FROM nfl_game_playstats_load) gpsl ON (
	gps.gameId = gpsl.gameId AND gps.gsisPlayer_id = gpsl.gsisPlayer_id AND gps.playId = gpsl.playId AND gps.statId = gpsl.statId
)
WHEN NOT MATCHED THEN
	INSERT ( 
		gps.gameId, gps.gsisPlayer_id, gps.playId, gps.playerName, gps.statId, gps.team_abbreviation, gps.team_id, gps.yards
	)	VALUES ( 
		gpsl.gameId, gpsl.gsisPlayer_id, gpsl.playId, gpsl.playerName, gpsl.statId, gpsl.team_abbreviation, gpsl.team_id, gpsl.yards
	);


MERGE INTO nfl_game_player gp
USING ( SELECT 	TO_TIMESTAMP(createdDate, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') createdDate,  gameStats_defensiveAssists, gameStats_defensiveForcedFumble, 
	gameStats_defensiveInterceptions, gameStats_defensiveInterceptionsYards, gameStats_defensivePassesDefensed, gameStats_defensiveSacks, 
	gameStats_defensiveSafeties, gameStats_defensiveSoloTackles, gameStats_defensiveTotalTackles, gameStats_fumblesLost, 
	gameStats_fumblesTotal, gameStats_kickReturns, gameStats_kickReturnsAverageYards, gameStats_kickReturnsLong, gameStats_kickReturnsTouchdowns, 
	gameStats_kickReturnsYards, gameStats_kickingFgAtt, gameStats_kickingFgLong, gameStats_kickingFgMade, gameStats_kickingXkAtt, gameStats_kickingXkMade, 
	gameStats_kickoffReturnsTouchdowns, gameStats_kickoffReturnsYards, gameStats_opponentFumbleRecovery, gameStats_passingAttempts, 
	gameStats_passingCompletions, gameStats_passingInterceptions, gameStats_passingTouchdowns, gameStats_passingYards, gameStats_puntReturns, 
	gameStats_puntReturnsAverageYards, gameStats_puntReturnsLong, gameStats_puntReturnsTouchdowns, gameStats_puntingAverageYards, gameStats_puntingLong, 
	gameStats_puntingPunts, gameStats_puntingPuntsInside20, gameStats_receivingReceptions, gameStats_receivingTarget, gameStats_receivingTouchdowns, 
	gameStats_receivingYards, gameStats_rushingAttempts, gameStats_rushingAverageYards, gameStats_rushingTouchdowns, gameStats_rushingYards, 
	gameStats_totalPointsScored, gameStats_touchdownsDefense, game_id, id, TO_TIMESTAMP(lastModifiedDate, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') lastModifiedDate,
	player_currentTeam_abbreviation, player_currentTeam_nickName, player_jerseyNumber, player_person_displayName, player_person_firstName, 
	player_person_headshot_asset_url, player_person_lastName, player_position, season_id, week_id FROM nfl_game_player_load) gpl ON (
	gp.id = gpl.id AND gp.game_id = gpl.game_id
)
WHEN NOT MATCHED THEN
	INSERT ( 
		gp.createdDate, gp.gameStats_defensiveAssists, gp.gameStats_defensiveForcedFumble, gp.gameStats_defensiveInterceptions, 
		gp.gameStats_defensiveInterceptionsYards, gp.gameStats_defensivePassesDefensed, gp.gameStats_defensiveSacks, gp.gameStats_defensiveSafeties, 
		gp.gameStats_defensiveSoloTackles, gp.gameStats_defensiveTotalTackles, gp.gameStats_fumblesLost, 
		gp.gameStats_fumblesTotal, gp.gameStats_kickReturns, gp.gameStats_kickReturnsAverageYards, gp.gameStats_kickReturnsLong, 
		gp.gameStats_kickReturnsTouchdowns, gp.gameStats_kickReturnsYards, gp.gameStats_kickingFgAtt, gp.gameStats_kickingFgLong, gp.gameStats_kickingFgMade, 
		gp.gameStats_kickingXkAtt, gp.gameStats_kickingXkMade, gp.gameStats_kickoffReturnsTouchdowns, gp.gameStats_kickoffReturnsYards, 
		gp.gameStats_opponentFumbleRecovery, gp.gameStats_passingAttempts, gp.gameStats_passingCompletions, gp.gameStats_passingInterceptions, 
		gp.gameStats_passingTouchdowns, gp.gameStats_passingYards, gp.gameStats_puntReturns, gp.gameStats_puntReturnsAverageYards, 
		gp.gameStats_puntReturnsLong, gp.gameStats_puntReturnsTouchdowns, gp.gameStats_puntingAverageYards, gp.gameStats_puntingLong, 
		gp.gameStats_puntingPunts, gp.gameStats_puntingPuntsInside20, gp.gameStats_receivingReceptions, gp.gameStats_receivingTarget, 
		gp.gameStats_receivingTouchdowns, gp.gameStats_receivingYards, gp.gameStats_rushingAttempts, gp.gameStats_rushingAverageYards, 
		gp.gameStats_rushingTouchdowns, gp.gameStats_rushingYards, gp.gameStats_totalPointsScored, gp.gameStats_touchdownsDefense, gp.game_id, gp.id, 
		gp.lastModifiedDate, gp.player_currentTeam_abbreviation, gp.player_currentTeam_nickName, gp.player_jerseyNumber, 
		gp.player_person_displayName, gp.player_person_firstName, gp.player_person_headshot_asset_url, gp.player_person_lastName, gp.player_position, 
		gp.season_id, gp.week_id 	
	)	VALUES (  
		gpl.createdDate, gpl.gameStats_defensiveAssists, gpl.gameStats_defensiveForcedFumble, gpl.gameStats_defensiveInterceptions, 
		gpl.gameStats_defensiveInterceptionsYards, gpl.gameStats_defensivePassesDefensed, gpl.gameStats_defensiveSacks, gpl.gameStats_defensiveSafeties, 
		gpl.gameStats_defensiveSoloTackles, gpl.gameStats_defensiveTotalTackles, gpl.gameStats_fumblesLost, 
		gpl.gameStats_fumblesTotal, gpl.gameStats_kickReturns, gpl.gameStats_kickReturnsAverageYards, gpl.gameStats_kickReturnsLong, 
		gpl.gameStats_kickReturnsTouchdowns, gpl.gameStats_kickReturnsYards, gpl.gameStats_kickingFgAtt, gpl.gameStats_kickingFgLong, 
		gpl.gameStats_kickingFgMade, gpl.gameStats_kickingXkAtt, gpl.gameStats_kickingXkMade, gpl.gameStats_kickoffReturnsTouchdowns, 
		gpl.gameStats_kickoffReturnsYards, gpl.gameStats_opponentFumbleRecovery, gpl.gameStats_passingAttempts, gpl.gameStats_passingCompletions, 
		gpl.gameStats_passingInterceptions, gpl.gameStats_passingTouchdowns, gpl.gameStats_passingYards, gpl.gameStats_puntReturns, 
		gpl.gameStats_puntReturnsAverageYards, gpl.gameStats_puntReturnsLong, gpl.gameStats_puntReturnsTouchdowns, gpl.gameStats_puntingAverageYards, 
		gpl.gameStats_puntingLong, gpl.gameStats_puntingPunts, gpl.gameStats_puntingPuntsInside20, gpl.gameStats_receivingReceptions, 
		gpl.gameStats_receivingTarget, gpl.gameStats_receivingTouchdowns, gpl.gameStats_receivingYards, gpl.gameStats_rushingAttempts, 
		gpl.gameStats_rushingAverageYards, gpl.gameStats_rushingTouchdowns, gpl.gameStats_rushingYards, gpl.gameStats_totalPointsScored, 
		gpl.gameStats_touchdownsDefense, gpl.game_id, gpl.id, gpl.lastModifiedDate, gpl.player_currentTeam_abbreviation, 
		gpl.player_currentTeam_nickName, gpl.player_jerseyNumber, gpl.player_person_displayName, gpl.player_person_firstName, 
		gpl.player_person_headshot_asset_url, gpl.player_person_lastName, gpl.player_position, gpl.season_id, gpl.week_id 	
	);

MERGE INTO nfl_season_standings ss
USING ( SELECT 	conferenceRank, division, fullName, id, nickName, overallLoss, overallTie, overallWin, teamId, week_id, week_seasonType, week_season_season, 
	week_weekOrder, week_weekType, week_weekValue
FROM nfl_season_standings_load ) ssl ON (
	ss.teamId = ssl.teamId AND ss.week_id = ssl.week_id
)
WHEN NOT MATCHED THEN
	INSERT (  
		ss.conferenceRank, ss.division, ss.fullName, ss.id, ss.nickName, ss.overallLoss, ss.overallTie, ss.overallWin, ss.teamId, ss.week_id, 
		ss.week_seasonType, ss.week_season_season, ss.week_weekOrder, ss.week_weekType, ss.week_weekValue
	)	VALUES ( 
		ssl.conferenceRank, ssl.division, ssl.fullName, ssl.id, ssl.nickName, ssl.overallLoss, ssl.overallTie, ssl.overallWin, ssl.teamId, ssl.week_id, 
		ssl.week_seasonType, ssl.week_season_season, ssl.week_weekOrder, ssl.week_weekType, ssl.week_weekValue
	);

MERGE INTO nfl_game_teamstats gt
USING ( SELECT gameId, opponentGameStats_down3rdAttempted, opponentGameStats_down3rdFdMade, opponentGameStats_fumblesLost, opponentGameStats_gamesPlayed, 
	opponentGameStats_passingAverageYards, opponentGameStats_passingInterceptions, opponentGameStats_passingNetYards, opponentGameStats_passingSacked, 
	opponentGameStats_rushingAverageYards, opponentGameStats_rushingYards, opponentGameStats_scrimmageYds, opponentGameStats_totalPointsScored, 
	teamGameStats_down3rdAttempted, teamGameStats_down3rdFdMade, teamGameStats_fumblesLost, teamGameStats_gamesPlayed, teamGameStats_passingAverageYards, 
	teamGameStats_passingInterceptions, teamGameStats_passingNetYards, teamGameStats_passingSacked, teamGameStats_penaltiesTotal, 
	teamGameStats_rushingAverageYards, teamGameStats_rushingYards, teamGameStats_scrimmagePlays, teamGameStats_scrimmageYds, teamGameStats_timeOfPossSeconds, 
	teamGameStats_totalPointsScored, team_abbreviation
FROM nfl_game_teamstats_load) gtl ON (
	gt.gameId = gtl.gameId AND gt.team_abbreviation = gtl.team_abbreviation
)
WHEN NOT MATCHED THEN
	INSERT ( 
		gt.gameId, gt.opponentGameStats_down3rdAttempted, gt.opponentGameStats_down3rdFdMade, gt.opponentGameStats_fumblesLost, 
		gt.opponentGameStats_gamesPlayed, gt.opponentGameStats_passingAverageYards, gt.opponentGameStats_passingInterceptions, 
		gt.opponentGameStats_passingNetYards, gt.opponentGameStats_passingSacked, gt.opponentGameStats_rushingAverageYards, 
		gt.opponentGameStats_rushingYards, gt.opponentGameStats_scrimmageYds, gt.opponentGameStats_totalPointsScored, gt.teamGameStats_down3rdAttempted, 
		gt.teamGameStats_down3rdFdMade, gt.teamGameStats_fumblesLost, gt.teamGameStats_gamesPlayed, gt.teamGameStats_passingAverageYards, 
		gt.teamGameStats_passingInterceptions, gt.teamGameStats_passingNetYards, gt.teamGameStats_passingSacked, gt.teamGameStats_penaltiesTotal, 
		gt.teamGameStats_rushingAverageYards, gt.teamGameStats_rushingYards, gt.teamGameStats_scrimmagePlays, gt.teamGameStats_scrimmageYds, 
		gt.teamGameStats_timeOfPossSeconds, gt.teamGameStats_totalPointsScored, gt.team_abbreviation	
	)	VALUES (   
		gtl.gameId, gtl.opponentGameStats_down3rdAttempted, gtl.opponentGameStats_down3rdFdMade, gtl.opponentGameStats_fumblesLost, 
		gtl.opponentGameStats_gamesPlayed, gtl.opponentGameStats_passingAverageYards, gtl.opponentGameStats_passingInterceptions, 
		gtl.opponentGameStats_passingNetYards, gtl.opponentGameStats_passingSacked, gtl.opponentGameStats_rushingAverageYards, 
		gtl.opponentGameStats_rushingYards, gtl.opponentGameStats_scrimmageYds, gtl.opponentGameStats_totalPointsScored, gtl.teamGameStats_down3rdAttempted, 
		gtl.teamGameStats_down3rdFdMade, gtl.teamGameStats_fumblesLost, gtl.teamGameStats_gamesPlayed, gtl.teamGameStats_passingAverageYards, 
		gtl.teamGameStats_passingInterceptions, gtl.teamGameStats_passingNetYards, gtl.teamGameStats_passingSacked, gtl.teamGameStats_penaltiesTotal, 
		gtl.teamGameStats_rushingAverageYards, gtl.teamGameStats_rushingYards, gtl.teamGameStats_scrimmagePlays, gtl.teamGameStats_scrimmageYds, 
		gtl.teamGameStats_timeOfPossSeconds, gtl.teamGameStats_totalPointsScored, gtl.team_abbreviation	
	);
