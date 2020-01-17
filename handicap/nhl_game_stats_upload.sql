MERGE INTO nhl_game_teamsummary ts
USING ( SELECT faceoffWinPctg, faceoffsLost, faceoffsWon, TO_TIMESTAMP(gameDate, 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"')gameDate, gameId, gameLocationCode, gamesPlayed, goalsAgainst, 
	goalsFor, losses, opponentTeamAbbrev, otLosses, penaltyKillPctg, points, ppGoalsAgainst, ppGoalsFor, ppOpportunities, ppPctg, shNumTimes, shootoutGamesLost, shootoutGamesWon, 
	shotsAgainst, shotsFor, teamAbbrev, teamFullName, teamId, ties, REGEXP_SUBSTR(wins, '\d+') wins FROM nhl_game_teamsummary_load) tsl ON (
    ts.gameId = tsl.gameId
    and 
    ts.teamId = tsl.teamId
)
WHEN NOT MATCHED THEN
    INSERT (
		ts.faceoffWinPctg, ts.faceoffsLost, ts.faceoffsWon, ts.gameDate, ts.gameId, ts.gameLocationCode, ts.gamesPlayed, ts.goalsAgainst, ts.goalsFor, ts.losses,
		ts.opponentTeamAbbrev, ts.otLosses, ts.penaltyKillPctg, ts.points, ts.ppGoalsAgainst, ts.ppGoalsFor, ts.ppOpportunities, ts.ppPctg, ts.shNumTimes, ts.shootoutGamesLost, 
		ts.shootoutGamesWon, ts.shotsAgainst, ts.shotsFor, ts.teamAbbrev, ts.teamFullName, ts.teamId, ts.ties, ts.wins
    )   VALUES (
		tsl.faceoffWinPctg, tsl.faceoffsLost, tsl.faceoffsWon, tsl.gameDate, tsl.gameId, tsl.gameLocationCode, tsl.gamesPlayed, tsl.goalsAgainst, tsl.goalsFor, tsl.losses,
		tsl.opponentTeamAbbrev, tsl.otLosses, tsl.penaltyKillPctg, tsl.points, tsl.ppGoalsAgainst, tsl.ppGoalsFor, tsl.ppOpportunities, tsl.ppPctg, tsl.shNumTimes, 
		tsl.shootoutGamesLost, tsl.shootoutGamesWon, tsl.shotsAgainst, tsl.shotsFor, tsl.teamAbbrev, tsl.teamFullName, tsl.teamId, tsl.ties, tsl.wins
    );

