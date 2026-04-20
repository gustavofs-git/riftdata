# Riot API — Schema Documentation (mylolstats)

**Data de geração:** 2026-03-19 19:16:32

---

## 1. Account-V1

**URL:** `GET https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}`

| Campo | Tipo | Exemplo |
|-------|------|---------|
| `puuid` | string | `sBrBkY9mUxKZrhq4paPJ-mvctr-gUjgP4hpa1wgYivv9nDm...` |
| `gameName` | string | `art1st` |
| `tagLine` | string | `sal1m` |

## 2. Summoner-V4

**URL:** `GET https://br1.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}`

| Campo | Tipo | Exemplo |
|-------|------|---------|
| `puuid` | string | `sBrBkY9mUxKZrhq4paPJ-mvctr-gUjgP4hpa1wgYivv9nDm...` |
| `profileIconId` | integer | `936` |
| `revisionDate` | integer | `1773943191612` |
| `summonerLevel` | integer | `414` |

## 3. League-V4 Entries by PUUID

**URL:** `GET https://br1.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}`

| Campo | Tipo | Exemplo |
|-------|------|---------|
| `leagueId` | string | `bf1c3331-2e0d-4bd5-9cd8-947f199aef94` |
| `queueType` | string | `RANKED_SOLO_5x5` |
| `tier` | string | `PLATINUM` |
| `rank` | string | `III` |
| `puuid` | string | `sBrBkY9mUxKZrhq4paPJ-mvctr-gUjgP4hpa1wgYivv9nDm...` |
| `leaguePoints` | integer | `45` |
| `wins` | integer | `68` |
| `losses` | integer | `70` |
| `veteran` | boolean | `False` |
| `inactive` | boolean | `False` |
| `freshBlood` | boolean | `False` |
| `hotStreak` | boolean | `True` |

## 4. League-Exp-V4 (Sample por Tier)

**URL:** `GET https://br1.api.riotgames.com/lol/league-exp/v4/entries/RANKED_SOLO_5x5/{tier}/I?page=1`

### Schema (baseado em IRON)

| Campo | Tipo | Exemplo |
|-------|------|---------|
| `leagueId` | string | `43bd8019-c81d-43c8-a781-37135f80d77d` |
| `queueType` | string | `RANKED_SOLO_5x5` |
| `tier` | string | `IRON` |
| `rank` | string | `I` |
| `puuid` | string | `EtxPWjU6DHl-tPKQ6u4Wz1FSzIqr8i16F5G_Pe8UrnnHmS9...` |
| `leaguePoints` | integer | `32` |
| `wins` | integer | `28` |
| `losses` | integer | `38` |
| `veteran` | boolean | `False` |
| `inactive` | boolean | `False` |
| `freshBlood` | boolean | `False` |
| `hotStreak` | boolean | `False` |

## 5. Match-V5 IDs

**URL:** `GET https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids`

**Tipo:** array[string] (5 items)

**Exemplo:** `['BR1_3219665682', 'BR1_3219655371', 'BR1_3219645748']`

## 6. Match-V5 Detail

**URL:** `GET https://americas.api.riotgames.com/lol/match/v5/matches/{matchId}`

### 6.1 Metadata

| Campo | Tipo | Exemplo |
|-------|------|---------|
| `dataVersion` | string | `2` |
| `matchId` | string | `BR1_3219665682` |
| `participants` | array[string] (10 items) | `["73m9FCxDe4ZMpv_Xl9oo2nwca3iLFVJrcQvBjfek-gFxs...` |

### 6.2 Info (campos principais)

| Campo | Tipo | Exemplo |
|-------|------|---------|
| `endOfGameResult` | string | `GameComplete` |
| `gameCreation` | integer | `1773941590152` |
| `gameDuration` | integer | `1568` |
| `gameEndTimestamp` | integer | `1773943191612` |
| `gameId` | integer | `3219665682` |
| `gameMode` | string | `CLASSIC` |
| `gameName` | string | `teambuilder-match-3219665682` |
| `gameStartTimestamp` | integer | `1773941623911` |
| `gameType` | string | `MATCHED_GAME` |
| `gameVersion` | string | `16.6.753.8272` |
| `mapId` | integer | `11` |
| `platformId` | string | `BR1` |
| `queueId` | integer | `420` |
| `tournamentCode` | string | `` |

### 6.3 Teams

| Campo | Tipo | Exemplo |
|-------|------|---------|
| `bans` | array[object (2 campos)] (5 items) | `[{"championId": 51, "pickTurn": 1}, {"championI...` |
| `bans[].championId` | integer | `51` |
| `bans[].pickTurn` | integer | `1` |
| `feats` | object (3 campos) | `{"EPIC_MONSTER_KILL": {"featState": 0}, "FIRST_...` |
| `feats.EPIC_MONSTER_KILL` | object (1 campos) | `{"featState": 0}` |
| `feats.EPIC_MONSTER_KILL.featState` | integer | `0` |
| `feats.FIRST_BLOOD` | object (1 campos) | `{"featState": 0}` |
| `feats.FIRST_BLOOD.featState` | integer | `0` |
| `feats.FIRST_TURRET` | object (1 campos) | `{"featState": 0}` |
| `feats.FIRST_TURRET.featState` | integer | `0` |
| `objectives` | object (8 campos) | `{"atakhan": {"first": false, "kills": 0}, "baro...` |
| `objectives.atakhan` | object (2 campos) | `{"first": false, "kills": 0}` |
| `objectives.atakhan.first` | boolean | `False` |
| `objectives.atakhan.kills` | integer | `0` |
| `objectives.baron` | object (2 campos) | `{"first": false, "kills": 0}` |
| `objectives.baron.first` | boolean | `False` |
| `objectives.baron.kills` | integer | `0` |
| `objectives.champion` | object (2 campos) | `{"first": true, "kills": 19}` |
| `objectives.champion.first` | boolean | `True` |
| `objectives.champion.kills` | integer | `19` |
| `objectives.dragon` | object (2 campos) | `{"first": false, "kills": 0}` |
| `objectives.dragon.first` | boolean | `False` |
| `objectives.dragon.kills` | integer | `0` |
| `objectives.horde` | object (2 campos) | `{"first": false, "kills": 0}` |
| `objectives.horde.first` | boolean | `False` |
| `objectives.horde.kills` | integer | `0` |
| `objectives.inhibitor` | object (2 campos) | `{"first": false, "kills": 0}` |
| `objectives.inhibitor.first` | boolean | `False` |
| `objectives.inhibitor.kills` | integer | `0` |
| `objectives.riftHerald` | object (2 campos) | `{"first": false, "kills": 0}` |
| `objectives.riftHerald.first` | boolean | `False` |
| `objectives.riftHerald.kills` | integer | `0` |
| `objectives.tower` | object (2 campos) | `{"first": false, "kills": 2}` |
| `objectives.tower.first` | boolean | `False` |
| `objectives.tower.kills` | integer | `2` |
| `teamId` | integer | `100` |
| `win` | boolean | `False` |

## 📋 Campos do Participante (COMPLETO - ordem alfabética)

**Localização:** `match.info.participants[0]`

> Esta é a seção mais importante para a Silver layer.

**Total de campos:** 147

| Campo | Tipo | Exemplo |
|-------|------|---------|
| `PlayerBehavior` | object (1 campos) | `{"PlayerBehavior_IsHeroInCombat": 0}` |
| `PlayerScore0` | integer | `0` |
| `PlayerScore1` | integer | `0` |
| `PlayerScore10` | integer | `0` |
| `PlayerScore11` | integer | `0` |
| `PlayerScore2` | integer | `0` |
| `PlayerScore3` | integer | `0` |
| `PlayerScore4` | integer | `0` |
| `PlayerScore5` | integer | `0` |
| `PlayerScore6` | integer | `0` |
| `PlayerScore7` | integer | `0` |
| `PlayerScore8` | integer | `0` |
| `PlayerScore9` | integer | `0` |
| `allInPings` | integer | `0` |
| `assistMePings` | integer | `1` |
| `assists` | integer | `1` |
| `baronKills` | integer | `0` |
| `basicPings` | integer | `0` |
| `challenges` | object (125 campos) | `{"12AssistStreakCount": 0, "HealFromMapSources": 0, "Infe...` |
| `champExperience` | integer | `11971` |
| `champLevel` | integer | `14` |
| `championId` | integer | `79` |
| `championName` | string | `Gragas` |
| `championTransform` | integer | `0` |
| `commandPings` | integer | `5` |
| `consumablesPurchased` | integer | `2` |
| `damageDealtToBuildings` | integer | `3767` |
| `damageDealtToEpicMonsters` | integer | `603` |
| `damageDealtToObjectives` | integer | `4371` |
| `damageDealtToTurrets` | integer | `3767` |
| `damageSelfMitigated` | integer | `9639` |
| `dangerPings` | integer | `0` |
| `deaths` | integer | `5` |
| `detectorWardsPlaced` | integer | `0` |
| `doubleKills` | integer | `0` |
| `dragonKills` | integer | `0` |
| `eligibleForProgression` | boolean | `True` |
| `enemyMissingPings` | integer | `9` |
| `enemyVisionPings` | integer | `1` |
| `firstBloodAssist` | boolean | `False` |
| `firstBloodKill` | boolean | `False` |
| `firstTowerAssist` | boolean | `False` |
| `firstTowerKill` | boolean | `False` |
| `gameEndedInEarlySurrender` | boolean | `False` |
| `gameEndedInSurrender` | boolean | `False` |
| `getBackPings` | integer | `0` |
| `goldEarned` | integer | `8349` |
| `goldSpent` | integer | `8100` |
| `holdPings` | integer | `0` |
| `individualPosition` | string | `TOP` |
| `inhibitorKills` | integer | `0` |
| `inhibitorTakedowns` | integer | `0` |
| `inhibitorsLost` | integer | `1` |
| `item0` | integer | `2031` |
| `item1` | integer | `6655` |
| `item2` | integer | `4646` |
| `item3` | integer | `3020` |
| `item4` | integer | `1052` |
| `item5` | integer | `1052` |
| `item6` | integer | `3340` |
| `itemsPurchased` | integer | `20` |
| `killingSprees` | integer | `0` |
| `kills` | integer | `1` |
| `lane` | string | `JUNGLE` |
| `largestCriticalStrike` | integer | `0` |
| `largestKillingSpree` | integer | `0` |
| `largestMultiKill` | integer | `1` |
| `longestTimeSpentLiving` | integer | `507` |
| `magicDamageDealt` | integer | `91560` |
| `magicDamageDealtToChampions` | integer | `11519` |
| `magicDamageTaken` | integer | `9190` |
| `missions` | object (12 campos) | `{"playerScore0": 0, "playerScore1": 0, "playerScore2": 0,...` |
| `needVisionPings` | integer | `1` |
| `neutralMinionsKilled` | integer | `0` |
| `nexusKills` | integer | `0` |
| `nexusLost` | integer | `1` |
| `nexusTakedowns` | integer | `0` |
| `objectivesStolen` | integer | `0` |
| `objectivesStolenAssists` | integer | `0` |
| `onMyWayPings` | integer | `3` |
| `participantId` | integer | `1` |
| `pentaKills` | integer | `0` |
| `perks` | object (2 campos) | `{"statPerks": {"defense": 5001, "flex": 5008, "offense": ...` |
| `physicalDamageDealt` | integer | `10931` |
| `physicalDamageDealtToChampions` | integer | `999` |
| `physicalDamageTaken` | integer | `6953` |
| `placement` | integer | `0` |
| `playerAugment1` | integer | `0` |
| `playerAugment2` | integer | `0` |
| `playerAugment3` | integer | `0` |
| `playerAugment4` | integer | `0` |
| `playerAugment5` | integer | `0` |
| `playerAugment6` | integer | `0` |
| `playerSubteamId` | integer | `0` |
| `profileIcon` | integer | `7035` |
| `pushPings` | integer | `0` |
| `puuid` | string | `73m9FCxDe4ZMpv_Xl9oo2nwca3iLFVJrcQvBjfek-gFxs-YjyWeeKTI9x...` |
| `quadraKills` | integer | `0` |
| `retreatPings` | integer | `5` |
| `riotIdGameName` | string | `DustyElementalis` |
| `riotIdTagline` | string | `8269` |
| `role` | string | `NONE` |
| `roleBoundItem` | integer | `1221` |
| `sightWardsBoughtInGame` | integer | `0` |
| `spell1Casts` | integer | `141` |
| `spell2Casts` | integer | `37` |
| `spell3Casts` | integer | `59` |
| `spell4Casts` | integer | `6` |
| `subteamPlacement` | integer | `0` |
| `summoner1Casts` | integer | `2` |
| `summoner1Id` | integer | `12` |
| `summoner2Casts` | integer | `3` |
| `summoner2Id` | integer | `4` |
| `summonerId` | string | `cKF0mGC2VSkLPQsSwYbv9llwIvL_GscxcjACoQF_ONL0dc2Oty573Eqo0w` |
| `summonerLevel` | integer | `44` |
| `summonerName` | string | `` |
| `teamEarlySurrendered` | boolean | `False` |
| `teamId` | integer | `100` |
| `teamPosition` | string | `TOP` |
| `timeCCingOthers` | integer | `29` |
| `timePlayed` | integer | `1568` |
| `totalAllyJungleMinionsKilled` | integer | `0` |
| `totalDamageDealt` | integer | `108106` |
| `totalDamageDealtToChampions` | integer | `12518` |
| `totalDamageShieldedOnTeammates` | integer | `0` |
| `totalDamageTaken` | integer | `17593` |
| `totalEnemyJungleMinionsKilled` | integer | `0` |
| `totalHeal` | integer | `4850` |
| `totalHealsOnTeammates` | integer | `0` |
| `totalMinionsKilled` | integer | `175` |
| `totalTimeCCDealt` | integer | `284` |
| `totalTimeSpentDead` | integer | `161` |
| `totalUnitsHealed` | integer | `1` |
| `tripleKills` | integer | `0` |
| `trueDamageDealt` | integer | `5614` |
| `trueDamageDealtToChampions` | integer | `0` |
| `trueDamageTaken` | integer | `1449` |
| `turretKills` | integer | `1` |
| `turretTakedowns` | integer | `1` |
| `turretsLost` | integer | `9` |
| `unrealKills` | integer | `0` |
| `visionClearedPings` | integer | `0` |
| `visionScore` | integer | `23` |
| `visionWardsBoughtInGame` | integer | `0` |
| `wardsKilled` | integer | `2` |
| `wardsPlaced` | integer | `10` |
| `win` | boolean | `False` |

## 📋 Campos do Timeline Frame

### Frame (nível raiz)

**Localização:** `match_timeline.info.frames[0]`

| Campo | Tipo | Exemplo |
|-------|------|---------|
| `events` | array[object (3 campos)] (3 items) | `[{"realTimestamp": 1773941623721, "timestamp": 0, "type":...` |
| `participantFrames` | object (10 campos) | `{"1": {"championStats": {"abilityHaste": 0, "abilityPower...` |
| `timestamp` | integer | `0` |

### Participant Frame

**Localização:** `match_timeline.info.frames[0].participantFrames["1"]`

| Campo | Tipo | Exemplo |
|-------|------|---------|
| `championStats` | object (25 campos) | `{"abilityHaste": 0, "abilityPower": 0, "armor":...` |
| `championStats.abilityHaste` | integer | `0` |
| `championStats.abilityPower` | integer | `0` |
| `championStats.armor` | integer | `38` |
| `championStats.armorPen` | integer | `0` |
| `championStats.armorPenPercent` | integer | `0` |
| `championStats.attackDamage` | integer | `64` |
| `championStats.attackSpeed` | integer | `100` |
| `championStats.bonusArmorPenPercent` | integer | `0` |
| `championStats.bonusMagicPenPercent` | integer | `0` |
| `championStats.ccReduction` | integer | `0` |
| `championStats.cooldownReduction` | integer | `0` |
| `championStats.health` | integer | `640` |
| `championStats.healthMax` | integer | `640` |
| `championStats.healthRegen` | integer | `11` |
| `championStats.lifesteal` | integer | `0` |
| `championStats.magicPen` | integer | `0` |
| `championStats.magicPenPercent` | integer | `0` |
| `championStats.magicResist` | integer | `32` |
| `championStats.movementSpeed` | integer | `330` |
| `championStats.omnivamp` | integer | `0` |
| `championStats.physicalVamp` | integer | `0` |
| `championStats.power` | integer | `400` |
| `championStats.powerMax` | integer | `400` |
| `championStats.powerRegen` | integer | `12` |
| `championStats.spellVamp` | integer | `0` |
| `currentGold` | integer | `500` |
| `damageStats` | object (12 campos) | `{"magicDamageDone": 0, "magicDamageDoneToChampi...` |
| `damageStats.magicDamageDone` | integer | `0` |
| `damageStats.magicDamageDoneToChampions` | integer | `0` |
| `damageStats.magicDamageTaken` | integer | `0` |
| `damageStats.physicalDamageDone` | integer | `0` |
| `damageStats.physicalDamageDoneToChampions` | integer | `0` |
| `damageStats.physicalDamageTaken` | integer | `0` |
| `damageStats.totalDamageDone` | integer | `0` |
| `damageStats.totalDamageDoneToChampions` | integer | `0` |
| `damageStats.totalDamageTaken` | integer | `0` |
| `damageStats.trueDamageDone` | integer | `0` |
| `damageStats.trueDamageDoneToChampions` | integer | `0` |
| `damageStats.trueDamageTaken` | integer | `0` |
| `goldPerSecond` | integer | `0` |
| `jungleMinionsKilled` | integer | `0` |
| `level` | integer | `1` |
| `minionsKilled` | integer | `0` |
| `participantId` | integer | `1` |
| `position` | object (2 campos) | `{"x": 603, "y": 611}` |
| `position.x` | integer | `603` |
| `position.y` | integer | `611` |
| `timeEnemySpentControlled` | integer | `0` |
| `totalGold` | integer | `500` |
| `xp` | integer | `0` |

## 📋 Campos do League Entry (LeagueEntryDTO)

**Fonte:** League-Exp-V4 (IRON)

| Campo | Tipo | Exemplo |
|-------|------|---------|
| `freshBlood` | boolean | `False` |
| `hotStreak` | boolean | `False` |
| `inactive` | boolean | `False` |
| `leagueId` | string | `43bd8019-c81d-43c8-a781-37135f80d77d` |
| `leaguePoints` | integer | `32` |
| `losses` | integer | `38` |
| `puuid` | string | `EtxPWjU6DHl-tPKQ6u4Wz1FSzIqr8i16F5G_Pe8UrnnHmS9f4ms6wzeVk...` |
| `queueType` | string | `RANKED_SOLO_5x5` |
| `rank` | string | `I` |
| `tier` | string | `IRON` |
| `veteran` | boolean | `False` |
| `wins` | integer | `28` |

## 📊 Resumo

### Contagem de Campos por Endpoint

| Endpoint | Campos |
|----------|--------|
| Account-V1 | 3 |
| Summoner-V4 | 4 |
| League-V4 Entry | 12 |
| Match Participant | 147 |

**Participantes por partida:** 10

**Frames no timeline:** 28

### Entries por Tier (League-Exp-V4)

| Tier | Entries |
|------|---------|
| IRON | 205 |
| GOLD | 205 |
| EMERALD | 205 |
| DIAMOND | 205 |
| MASTER | 205 |
| CHALLENGER | 200 |

---

*Relatório gerado automaticamente por `riot_api_explorer.py`*