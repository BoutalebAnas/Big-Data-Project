# IMDb · Résultats du projet

Ce rapport rassemble les sorties finales des traitements Spark et, pour chaque partie, l'explication
méthodologique demandée. Les tableaux sont des sorties de requêtes. Les explications, choix et
conclusions sont rédigés par les membres du groupe.

## Contributions

| Partie | Membre (GitHub) | Pseudo Discord | Pull Requests |
| --- | --- | --- | --- |
| Data Preparation | thomassbgn | *à compléter* | #2 |
| Data Exploration | thomassbgn | *à compléter* | #2, #4, correctifs Q1, Q6 et Q8 |
| Data Enrichment | TaDLpZ | *à compléter* | #5, correctifs de nettoyage |
| Streaming | BoutalebAnas | *à compléter* | #3, correctif correspondance IMDb et données tardives |
| Open Question | BoutalebAnas | *à compléter* | #6 |
| Hygiène du dépôt | *à compléter* | *à compléter* | `.gitignore` des sorties d'enrichissement |

## Données utilisées

- Instantané IMDb : sept archives `*.tsv.gz` du point de distribution IMDb, préparation
  `452e5d1ccb1142b3823d9ba935e401a0` (6 novembre 2026, année de validation 2026).
- Enrichissement : Wikidata (P345 identifiant IMDb, P364 langue originale) et Wikipedia en anglais, via leurs API publiques.
- Streaming : flux Wikimedia EventStreams `recentchange`, transporté par Kafka.

## Data Preparation

### Valeurs manquantes par colonne

Valeurs nulles, chaînes vides ou blanches et marqueur IMDb `\N`, mesurées sur toutes les lignes brutes.

| table | column | rows | missing | missing_pct |
| --- | --- | --- | --- | --- |
| name.basics | birthYear | 15,793,855 | 15,108,483 | 95.661 |
| name.basics | deathYear | 15,793,855 | 15,528,603 | 98.321 |
| name.basics | knownForTitles | 15,793,855 | 1,914,559 | 12.122 |
| name.basics | nconst | 15,793,855 | 0 | 0 |
| name.basics | primaryName | 15,793,855 | 89 | 0.001 |
| name.basics | primaryProfession | 15,793,855 | 3,213,175 | 20.344 |
| title.akas | attributes | 60,231,176 | 59,914,206 | 99.474 |
| title.akas | isOriginalTitle | 60,231,176 | 0 | 0 |
| title.akas | language | 60,231,176 | 19,619,103 | 32.573 |
| title.akas | ordering | 60,231,176 | 0 | 0 |
| title.akas | region | 60,231,176 | 12,976,473 | 21.544 |
| title.akas | title | 60,231,176 | 0 | 0 |
| title.akas | titleId | 60,231,176 | 0 | 0 |
| title.akas | types | 60,231,176 | 40,273,932 | 66.866 |
| title.basics | endYear | 12,936,379 | 12,772,245 | 98.731 |
| title.basics | genres | 12,936,379 | 547,049 | 4.229 |
| title.basics | isAdult | 12,936,379 | 0 | 0 |
| title.basics | originalTitle | 12,936,379 | 0 | 0 |
| title.basics | primaryTitle | 12,936,379 | 0 | 0 |
| title.basics | runtimeMinutes | 12,936,379 | 8,053,347 | 62.253 |
| title.basics | startYear | 12,936,379 | 1,487,009 | 11.495 |
| title.basics | tconst | 12,936,379 | 0 | 0 |
| title.basics | titleType | 12,936,379 | 0 | 0 |
| title.crew | directors | 12,936,379 | 5,774,164 | 44.635 |
| title.crew | tconst | 12,936,379 | 0 | 0 |
| title.crew | writers | 12,936,379 | 6,415,222 | 49.591 |
| title.episode | episodeNumber | 10,012,173 | 2,112,415 | 21.098 |
| title.episode | parentTconst | 10,012,173 | 0 | 0 |
| title.episode | seasonNumber | 10,012,173 | 2,112,415 | 21.098 |
| title.episode | tconst | 10,012,173 | 0 | 0 |
| title.principals | category | 102,588,083 | 0 | 0 |
| title.principals | characters | 102,588,083 | 52,609,910 | 51.283 |
| title.principals | job | 102,588,083 | 82,747,283 | 80.66 |
| title.principals | nconst | 102,588,083 | 0 | 0 |
| title.principals | ordering | 102,588,083 | 0 | 0 |
| title.principals | tconst | 102,588,083 | 0 | 0 |
| title.ratings | averageRating | 1,729,431 | 0 | 0 |
| title.ratings | numVotes | 1,729,431 | 0 | 0 |
| title.ratings | tconst | 1,729,431 | 0 | 0 |

### Cinq colonnes les plus incomplètes

| table | column | missing_pct |
| --- | --- | --- |
| title.akas | attributes | 99.474 |
| title.basics | endYear | 98.731 |
| name.basics | deathYear | 98.321 |
| name.basics | birthYear | 95.661 |
| title.principals | job | 80.66 |

### Anomalies détectées

Contrôles avec au moins une occurrence. Les orphelins sont comptés après le nettoyage des clés.

| table | issue | count | unit | action | example |
| --- | --- | --- | --- | --- | --- |
| title.principals | orphan_nconst_to_name.basics | 7,191 | links | retain_source_require_parent_in_analysis | {"tconst":"tt0022647","ordering":10,"_reference":"nm17757978"} |
| title.crew | orphan_writers_to_name.basics | 1,148 | links | retain_source_require_parent_in_analysis | {"tconst":"tt0090442","_reference":"nm18617482"} |
| title.crew | orphan_directors_to_name.basics | 423 | links | retain_source_require_parent_in_analysis | {"tconst":"tt0850260","_reference":"nm17757903"} |
| name.basics | death_before_birth | 28 | rows | set_null | {"nconst":"nm0012634","deathYear":"456"} |
| name.basics | orphan_knownForTitles_to_title.basics | 8 | links | retain_source_require_parent_in_analysis | {"nconst":"nm18052615","_reference":"tt39377293"} |
| title.episode | orphan_parentTconst_to_title.basics | 5 | links | retain_source_require_parent_in_analysis | {"tconst":"tt28611150","_reference":"tt28606619"} |
| title.basics | invalid_runtimeMinutes | 4 | rows | set_null | {"tconst":"tt0348126","runtimeMinutes":"0"} |
| title.basics | invalid_startYear | 1 | rows | set_null | {"tconst":"tt5174640","startYear":"2115"} |

Contrôles sans aucune occurrence sur cet instantané :

| table | checks_with_zero_count |
| --- | --- |
| name.basics | conflicting_keys, conflicting_rows, duplicate_rows, invalid_birthYear, invalid_deathYear, lifespan_over_125, missing_or_invalid_key |
| title.akas | conflicting_keys, conflicting_rows, duplicate_rows, invalid_isOriginalTitle, invalid_ordering, missing_or_invalid_key, orphan_titleId_to_title.basics |
| title.basics | conflicting_keys, conflicting_rows, duplicate_rows, end_before_start, invalid_endYear, invalid_isAdult, missing_or_invalid_key |
| title.crew | conflicting_keys, conflicting_rows, duplicate_rows, missing_or_invalid_key, orphan_tconst_to_title.basics |
| title.episode | conflicting_keys, conflicting_rows, duplicate_rows, episode_is_own_parent, invalid_episodeNumber, invalid_seasonNumber, missing_or_invalid_key, orphan_tconst_to_title.basics |
| title.principals | conflicting_keys, conflicting_rows, duplicate_rows, invalid_ordering, missing_or_invalid_key, orphan_tconst_to_title.basics |
| title.ratings | conflicting_keys, conflicting_rows, duplicate_rows, invalid_averageRating, invalid_numVotes, missing_or_invalid_key, orphan_tconst_to_title.basics |

### Traitement appliqué

| Action | Effet |
| --- | --- |
| `set_null` | Valeur invalide ou incohérente remplacée par une valeur nulle typée |
| `exclude_row` | Ligne sans clé valide écartée |
| `collapse_identical_rows` | Doublons identiques fusionnés |
| `exclude_all_variants` | Toutes les variantes d'une clé en conflit écartées |
| `retain_source_require_parent_in_analysis` | Lien orphelin conservé, parent exigé dans les jointures analytiques |
| `retain_for_review` | Valeur conservée et signalée |

Sur cet instantané, aucune ligne n'a été retirée : le nombre de lignes brutes et nettoyées est identique pour les sept tables.

### Methodological Explanation

> **À RÉDIGER PAR LE GROUPE (sans IA)** : méthode utilisée et raisons de ce choix ; alternatives envisagées et compromis.

## Data Exploration

Seuils de l'exécution : `PARAMETERS` dans `notebooks/02_data_exploration.ipynb`. Les exports complets se trouvent dans
`data/results/<préparation>/<exécution>/`.

### Q1 — Durées atypiques par genre

Films de type `movie` d'au moins 40 minutes, genres d'au moins 30 films, score z modifié
`0.6745 × (durée − médiane) / MAD` d'au moins 3,5 en valeur absolue. Résultat : 16 401 couples film–genre
pour 10 745 films distincts. Exécution `192d74d7dd8d48b085a7ae4924eb3708`, code des PR de correction.

| genre | outliers | longer | shorter | genre_films | median_min | mad_min | mean_min | sd_min |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Drama | 3,354 | 3,354 | 0 | 196,627 | 93 | 13 | 96 | 37.1 |
| Comedy | 2,706 | 2,706 | 0 | 96,584 | 91 | 10 | 93.1 | 28.7 |
| Action | 1,779 | 1,779 | 0 | 40,345 | 94 | 12 | 101.2 | 33.7 |
| Documentary | 1,314 | 1,314 | 0 | 116,047 | 74 | 16 | 78 | 223.6 |
| Thriller | 1,046 | 1,040 | 6 | 34,075 | 93 | 10 | 96.1 | 21.8 |
| Crime | 1,041 | 1,041 | 0 | 34,618 | 92 | 11 | 95.5 | 25.6 |
| Romance | 987 | 987 | 0 | 42,421 | 95 | 12 | 99.1 | 25.1 |
| Adventure | 802 | 802 | 0 | 24,054 | 90 | 11 | 93 | 30.6 |
| Horror | 660 | 650 | 10 | 30,722 | 88 | 9 | 88.6 | 22.3 |
| Fantasy | 298 | 298 | 0 | 12,714 | 91 | 12 | 93.9 | 31.9 |
| Family | 282 | 282 | 0 | 15,939 | 90 | 13 | 92.3 | 32.7 |
| Sci-Fi | 245 | 245 | 0 | 9,810 | 90 | 10 | 91.2 | 30.5 |
| Musical | 238 | 238 | 0 | 8,497 | 94 | 14 | 100 | 33.3 |
| Mystery | 232 | 232 | 0 | 16,473 | 92 | 12 | 94.3 | 50.4 |
| Adult | 230 | 230 | 0 | 6,118 | 77 | 9 | 79.5 | 30.1 |
| History | 209 | 209 | 0 | 13,631 | 90 | 18 | 94.3 | 52.9 |
| War | 206 | 206 | 0 | 8,047 | 92 | 13 | 94.8 | 31.3 |
| Music | 200 | 200 | 0 | 13,963 | 87 | 16 | 88.4 | 57.7 |
| Biography | 174 | 174 | 0 | 16,100 | 88 | 17 | 89.2 | 29.4 |
| Animation | 145 | 145 | 0 | 8,335 | 81 | 12 | 82.7 | 26.8 |
| Western | 117 | 117 | 0 | 6,376 | 70 | 15 | 76.5 | 28.7 |
| Sport | 100 | 100 | 0 | 6,809 | 90 | 14 | 88.9 | 37.4 |
| News | 22 | 22 | 0 | 1,408 | 75 | 15 | 77.6 | 32.2 |
| Reality-TV | 9 | 9 | 0 | 298 | 90 | 15 | 92.2 | 85 |
| Talk-Show | 3 | 3 | 0 | 104 | 79 | 19 | 87.6 | 33.4 |
| Film-Noir | 2 | 2 | 0 | 877 | 82 | 10 | 81.9 | 13.8 |

Écarts les plus forts :

| tconst | primaryTitle | startYear | genre | runtimeMinutes | robust_z |
| --- | --- | --- | --- | --- | --- |
| tt8273150 | Logistics | 2012 | Documentary | 51,420 | 2,164.6 |
| tt3854496 | Ambiancé | 2020 | Documentary | 43,200 | 1,818 |
| tt12277054 | Carnets Filmés (Liste Complète) | 2019 | Documentary | 28,643 | 1,204.4 |
| tt2659636 | Modern Times Forever | 2011 | Documentary | 14,400 | 603.9 |
| tt2355497 | Beijing 2003 | 2004 | Documentary | 9,000 | 376.3 |
| tt5136218 | London EC1 | 2015 | Comedy | 5,460 | 362.1 |
| tt5068890 | Hunger! | 2015 | Drama | 6,000 | 306.5 |
| tt5136218 | London EC1 | 2015 | Mystery | 5,460 | 301.7 |
| tt1806963 | Matrjoschka | 2006 | Drama | 5,700 | 290.9 |
| tt5136218 | London EC1 | 2015 | Drama | 5,460 | 278.5 |
| tt5068890 | Hunger! | 2015 | Documentary | 6,000 | 249.8 |
| tt0284020 | The Cure for Insomnia | 1987 | Documentary | 5,220 | 216.9 |
| tt0284020 | The Cure for Insomnia | 1987 | Music | 5,220 | 216.4 |
| tt3837350 | A 2nd generation film | 2013 | Drama | 3,077 | 154.8 |
| tt0342707 | The Longest Most Meaningless Movie in the World | 1970 | Drama | 2,880 | 144.6 |

Films plus courts que leur genre :

| tconst | primaryTitle | startYear | genre | runtimeMinutes | robust_z |
| --- | --- | --- | --- | --- | --- |
| tt1294998 | Female Prisoner Ayaka: Bitch-Training Torment | 2008 | Horror | 40 | -3.6 |
| tt1337118 | Not Haunted | 2009 | Horror | 40 | -3.6 |
| tt37840127 | Give Me Something Good | 2026 | Horror | 40 | -3.6 |
| tt42299695 | The Wicked Hour | 2026 | Horror | 40 | -3.6 |
| tt43671385 | Hush | 2026 | Horror | 40 | -3.6 |
| tt50320754 | Il Tempo dell'Esilio | 2026 | Horror | 40 | -3.6 |
| tt1337118 | Not Haunted | 2009 | Thriller | 40 | -3.6 |
| tt43671385 | Hush | 2026 | Thriller | 40 | -3.6 |
| tt5476100 | Vistas | 2020 | Thriller | 40 | -3.6 |
| tt8231444 | The Last Cry | 2019 | Thriller | 40 | -3.6 |

### Q2 — Trois genres les plus populaires par année (1980–2025)

Popularité : somme des votes IMDb des films de l'année dans le genre (votes entre parenthèses).

| year | rank_1 | rank_2 | rank_3 |
| --- | --- | --- | --- |
| 1980 | Drama (3,166,513) | Adventure (2,482,053) | Horror (2,072,775) |
| 1981 | Adventure (2,250,738) | Drama (1,713,353) | Horror (1,275,987) |
| 1982 | Drama (3,075,644) | Sci-Fi (2,457,489) | Action (1,874,102) |
| 1983 | Drama (2,925,343) | Action (2,025,321) | Adventure (1,984,401) |
| 1984 | Action (3,668,506) | Drama (3,028,185) | Adventure (2,931,851) |
| 1985 | Comedy (4,567,339) | Adventure (3,410,867) | Drama (3,080,425) |
| 1986 | Drama (4,003,756) | Adventure (2,965,620) | Comedy (2,787,222) |
| 1987 | Drama (4,139,617) | Comedy (3,688,010) | Action (2,361,972) |
| 1988 | Drama (4,468,260) | Comedy (4,431,825) | Action (2,716,774) |
| 1989 | Comedy (5,014,127) | Drama (3,965,661) | Adventure (2,943,486) |
| 1990 | Drama (6,326,440) | Comedy (4,019,187) | Crime (3,589,228) |
| 1991 | Drama (5,590,462) | Crime (3,675,141) | Comedy (3,196,897) |
| 1992 | Drama (6,100,874) | Comedy (4,075,590) | Crime (3,610,507) |
| 1993 | Drama (9,003,121) | Comedy (4,503,259) | Crime (3,046,095) |
| 1994 | Drama (15,745,604) | Crime (6,505,804) | Comedy (5,679,549) |
| 1995 | Drama (12,526,677) | Crime (7,764,595) | Comedy (6,082,747) |
| 1996 | Drama (7,866,094) | Comedy (4,791,294) | Crime (4,444,973) |
| 1997 | Drama (12,633,814) | Action (6,769,203) | Comedy (5,608,359) |
| 1998 | Drama (11,790,279) | Comedy (9,865,756) | Crime (6,219,068) |
| 1999 | Drama (15,526,651) | Comedy (8,226,358) | Crime (7,183,418) |
| 2000 | Drama (13,028,344) | Comedy (8,340,187) | Adventure (6,991,391) |
| 2001 | Drama (14,744,051) | Comedy (9,519,314) | Adventure (9,252,963) |
| 2002 | Drama (17,066,500) | Adventure (8,692,522) | Action (8,398,069) |
| 2003 | Drama (13,263,178) | Action (11,712,739) | Comedy (10,765,063) |
| 2004 | Drama (16,483,105) | Action (11,521,845) | Comedy (10,952,835) |
| 2005 | Drama (13,698,691) | Comedy (10,203,287) | Action (9,063,434) |
| 2006 | Drama (20,895,381) | Comedy (11,783,966) | Thriller (8,666,092) |
| 2007 | Drama (18,543,802) | Comedy (11,724,907) | Action (8,998,225) |
| 2008 | Drama (17,584,313) | Action (13,410,426) | Comedy (11,333,470) |
| 2009 | Drama (17,599,382) | Comedy (13,470,204) | Action (12,118,681) |
| 2010 | Drama (17,565,645) | Action (13,337,157) | Adventure (13,178,527) |
| 2011 | Drama (20,755,824) | Action (17,150,864) | Comedy (13,987,885) |
| 2012 | Drama (20,122,054) | Action (14,895,517) | Comedy (12,514,520) |
| 2013 | Drama (21,990,609) | Action (17,163,498) | Comedy (14,961,207) |
| 2014 | Drama (24,520,931) | Action (20,937,540) | Adventure (17,737,443) |
| 2015 | Drama (20,496,264) | Action (15,560,093) | Adventure (14,160,956) |
| 2016 | Drama (20,890,495) | Action (17,205,809) | Adventure (12,363,069) |
| 2017 | Drama (20,809,449) | Action (16,709,672) | Comedy (11,305,746) |
| 2018 | Drama (17,415,918) | Action (15,076,998) | Adventure (13,345,118) |
| 2019 | Drama (22,025,570) | Action (12,360,138) | Comedy (12,267,153) |
| 2020 | Drama (10,480,614) | Comedy (6,515,766) | Action (6,129,695) |
| 2021 | Drama (14,399,857) | Action (12,873,357) | Adventure (10,187,818) |
| 2022 | Drama (15,980,070) | Action (13,352,496) | Comedy (10,208,617) |
| 2023 | Drama (14,770,722) | Action (10,578,326) | Comedy (9,506,913) |
| 2024 | Drama (11,671,394) | Action (8,654,095) | Comedy (7,099,145) |
| 2025 | Drama (8,632,927) | Action (7,683,959) | Comedy (5,900,602) |

### Q3 — Trois premiers genres comparés aux autres

| year | top3_vote_share | other_genres | top3_votes_per_film | rest_votes_per_film | top3_weighted_rating | rest_weighted_rating |
| --- | --- | --- | --- | --- | --- | --- |
| 1980 | 0.443 | 21 | 3,948 | 2,834 | 7.73 | 7.5 |
| 1981 | 0.465 | 20 | 2,693 | 1,750 | 7.24 | 6.85 |
| 1982 | 0.437 | 19 | 3,434 | 2,886 | 7.56 | 7.15 |
| 1983 | 0.459 | 19 | 2,950 | 2,508 | 7.43 | 7.11 |
| 1984 | 0.441 | 20 | 4,188 | 3,538 | 7.35 | 7.19 |
| 1985 | 0.515 | 19 | 4,142 | 3,173 | 7.39 | 7.02 |
| 1986 | 0.462 | 19 | 3,516 | 3,476 | 7.2 | 7.05 |
| 1987 | 0.426 | 19 | 3,105 | 4,567 | 7.09 | 7.06 |
| 1988 | 0.468 | 19 | 3,608 | 4,106 | 7.16 | 7.07 |
| 1989 | 0.494 | 20 | 4,149 | 3,260 | 7.22 | 6.78 |
| 1990 | 0.456 | 19 | 4,626 | 4,607 | 7.3 | 7.07 |
| 1991 | 0.442 | 20 | 4,296 | 4,839 | 7.36 | 7.49 |
| 1992 | 0.463 | 20 | 4,906 | 5,009 | 7.23 | 7 |
| 1993 | 0.448 | 19 | 6,121 | 6,674 | 7.31 | 7.39 |
| 1994 | 0.583 | 20 | 10,333 | 6,497 | 7.87 | 7.32 |
| 1995 | 0.49 | 20 | 9,801 | 8,199 | 7.59 | 7.28 |
| 1996 | 0.449 | 21 | 6,318 | 6,574 | 6.98 | 6.78 |
| 1997 | 0.441 | 19 | 7,865 | 9,727 | 7.09 | 7.13 |
| 1998 | 0.515 | 21 | 9,136 | 7,404 | 7.4 | 6.8 |
| 1999 | 0.436 | 20 | 9,626 | 10,740 | 7.53 | 7.32 |
| 2000 | 0.45 | 21 | 9,026 | 7,940 | 7.21 | 6.98 |
| 2001 | 0.424 | 20 | 9,812 | 9,707 | 7.36 | 7.16 |
| 2002 | 0.424 | 22 | 11,310 | 8,603 | 7.22 | 7.04 |
| 2003 | 0.455 | 21 | 8,606 | 8,949 | 7.11 | 7.22 |
| 2004 | 0.413 | 21 | 9,119 | 10,084 | 7.02 | 7.11 |
| 2005 | 0.404 | 22 | 6,901 | 7,649 | 6.79 | 6.87 |
| 2006 | 0.427 | 22 | 8,109 | 7,950 | 7.12 | 6.95 |
| 2007 | 0.383 | 22 | 7,181 | 8,195 | 6.96 | 6.91 |
| 2008 | 0.404 | 21 | 6,807 | 7,002 | 6.75 | 7.1 |
| 2009 | 0.413 | 21 | 5,855 | 5,720 | 6.98 | 6.88 |
| 2010 | 0.4 | 22 | 6,926 | 4,637 | 6.97 | 7.02 |
| 2011 | 0.461 | 22 | 6,090 | 4,458 | 6.84 | 6.75 |
| 2012 | 0.427 | 22 | 5,255 | 4,265 | 6.9 | 6.96 |
| 2013 | 0.415 | 22 | 5,469 | 4,628 | 6.85 | 6.95 |
| 2014 | 0.47 | 23 | 7,711 | 3,550 | 7.09 | 6.96 |
| 2015 | 0.466 | 22 | 6,066 | 2,988 | 6.98 | 6.77 |
| 2016 | 0.441 | 22 | 5,880 | 3,272 | 6.89 | 6.9 |
| 2017 | 0.451 | 23 | 4,383 | 3,466 | 6.93 | 6.8 |
| 2018 | 0.443 | 22 | 5,192 | 2,950 | 6.94 | 6.74 |
| 2019 | 0.436 | 23 | 4,228 | 3,581 | 7 | 6.85 |
| 2020 | 0.448 | 22 | 2,669 | 1,951 | 6.42 | 6.42 |
| 2021 | 0.464 | 22 | 4,429 | 2,393 | 6.78 | 6.54 |
| 2022 | 0.464 | 23 | 3,300 | 2,596 | 6.75 | 6.56 |
| 2023 | 0.447 | 22 | 2,881 | 2,458 | 6.67 | 6.66 |
| 2024 | 0.429 | 23 | 2,328 | 2,048 | 6.57 | 6.52 |
| 2025 | 0.393 | 22 | 1,980 | 1,917 | 6.51 | 6.46 |

### Q4 — Réalisateurs

Exactement un film noté au moins 8,0 et au moins deux autres films notés, de moyenne au plus 6,0.
1 563 réalisateurs. Extrait : les 20 réalisateurs ayant le plus d'autres films notés.

| nconst | primaryName | high_film | high_rating | other_rated_films | other_mean | unrated_films |
| --- | --- | --- | --- | --- | --- | --- |
| nm0002031 | Michael Curtiz | tt0034583 | 8.5 | 170 | 5.68 | 1 |
| nm0064415 | William Beaudine | tt0040488 | 8.2 | 146 | 5.85 | 42 |
| nm0861703 | Richard Thorpe | tt0032138 | 8.1 | 131 | 5.71 | 53 |
| nm0187671 | Alfredo B. Crevenna | tt0284471 | 8.3 | 117 | 5.66 | 32 |
| nm0909825 | Raoul Walsh | tt0042041 | 8.1 | 116 | 5.86 | 6 |
| nm4341114 | Dustin Ferguson | tt46446808 | 9 | 110 | 3.22 | 31 |
| nm0567757 | Frank McDonald | tt0129427 | 8 | 97 | 5.97 | 17 |
| nm0052897 | Henry Barakat | tt0231477 | 8.1 | 87 | 5.66 | 0 |
| nm0136552 | René Cardona Jr. | tt0274840 | 8 | 76 | 5.68 | 14 |
| nm0030762 | Franz Antel | tt0082087 | 8.1 | 74 | 5.21 | 1 |
| nm0015648 | Hasan El-Emam | tt0343169 | 8.1 | 73 | 5.7 | 11 |
| nm0759232 | Jaime Salvador | tt0234278 | 8.5 | 72 | 5.89 | 32 |
| nm0000465 | Ron Jeremy | tt0135376 | 8 | 63 | 5.99 | 23 |
| nm0246473 | José Díaz Morales | tt0280217 | 8.1 | 59 | 5.98 | 31 |
| nm0704841 | Arthur Maria Rabenalt | tt0205095 | 8 | 59 | 5.65 | 12 |
| nm0303120 | Carmine Gallone | tt0049864 | 8.2 | 57 | 5.89 | 54 |
| nm0766211 | Hisayasu Satō | tt0291347 | 8 | 56 | 5.72 | 6 |
| nm0166730 | Elmer Clifton | tt0018597 | 8.3 | 55 | 5.41 | 35 |
| nm0559285 | Rizal Mantovani | tt22743310 | 9 | 52 | 5.47 | 3 |
| nm0210322 | Tulio Demicheli | tt0186935 | 8 | 51 | 5.93 | 3 |

### Q5 — Séries

Au moins trois saisons de dix épisodes ou plus, pire épisode dans la dernière saison, meilleur épisode
dans les saisons 1 ou 2. 479 séries. Extrait : les 20 séries ayant le plus de saisons qualifiantes.

| parentTconst | series_title | qualifying_seasons | last_season | episodes | rated_episodes | worst | best |
| --- | --- | --- | --- | --- | --- | --- | --- |
| tt1796154 | Papiny dochki | 20 | 20 | 410 | 259 | 4.6 (S20) | 8.5 (S2) |
| tt6953640 | Roadkill | 13 | 13 | 169 | 9 | 6.8 (S13) | 9.5 (S2) |
| tt1068912 | Ice Road Truckers | 11 | 12 | 154 | 153 | 5.8 (S12) | 9.0 (S1) |
| tt4264964 | Evening Urgant | 11 | 11 | 1,555 | 5 | 7.2 (S11) | 9.4 (S1) |
| tt1563280 | Disappeared | 10 | 11 | 150 | 143 | 5.2 (S11) | 8.6 (S2) |
| tt0055662 | The Beverly Hillbillies | 9 | 9 | 274 | 274 | 5.8 (S9) | 8.8 (S2) |
| tt0096560 | Coach | 9 | 9 | 199 | 199 | 6.7 (S9) | 9.1 (S2) |
| tt3107288 | The Flash | 9 | 9 | 184 | 184 | 3.8 (S9) | 9.6 (S1) |
| tt0043194 | Dragnet | 8 | 8 | 276 | 109 | 6.5 (S8) | 9.0 (S2) |
| tt0086687 | The Cosby Show | 8 | 8 | 197 | 197 | 5.4 (S8) | 8.5 (S2) |
| tt0285331 | 24 | 8 | 8 | 192 | 192 | 7.6 (S8) | 9.3 (S1) |
| tt0439100 | Weeds | 8 | 8 | 102 | 102 | 6.9 (S8) | 8.9 (S2) |
| tt0475900 | Anthony Bourdain: No Reservations | 8 | 9 | 143 | 143 | 6.2 (S9) | 8.9 (S2) |
| tt7414406 | All American | 8 | 8 | 132 | 132 | 6.1 (S8) | 9.0 (S2) |
| tt7828138 | Susedia | 8 | 10 | 161 | 55 | 6.2 (S10) | 9.6 (S2) |
| tt0078569 | Benson | 7 | 7 | 158 | 158 | 6.0 (S7) | 9.0 (S2) |
| tt0096641 | The All New Mickey Mouse Club | 7 | 7 | 315 | 5 | 7.4 (S7) | 9.7 (S2) |
| tt0098948 | Wings | 7 | 8 | 172 | 172 | 6.4 (S8) | 8.6 (S2) |
| tt0112111 | The Outer Limits | 7 | 7 | 152 | 152 | 5.2 (S7) | 8.4 (S2) |
| tt0200339 | Gamesmaster | 7 | 7 | 126 | 4 | 7.2 (S7) | 8.6 (S1) |

### Q6 — Succès dans plusieurs langues

Succès : note d'au moins 7,0 et au moins 1 000 votes. Langues : langue originale du film d'après Wikidata (P364),
exportée par le notebook 03 (mode `verified`). Seuls les films couverts par l'enrichissement ont une langue.

| successful_films | films_with_language | mode | coverage |
| --- | --- | --- | --- |
| 14,178 | 2,644 | verified | 0.186 |

Au moins deux langues : 1 144 acteurs et actrices. Au moins trois langues : 496.
Extrait : les 20 premiers par nombre de langues (identiques dans les deux listes). Entre parenthèses, le nombre de
films à succès dans chaque langue.

| nconst | primaryName | language_count | languages_films |
| --- | --- | --- | --- |
| nm0303826 | Jacques Gamblin | 10 | ar (1), de (2), en (1), es (1), fr (1), ja (1), ku (1), nl (1), pt (1), ru (1) |
| nm0000164 | Anthony Hopkins | 9 | bg (1), de (2), en (20), es (1), fr (1), it (1), la (3), pt (1), ro (1) |
| nm0001838 | Rachel Weisz | 9 | ar (1), de (1), egy (1), el (1), en (9), fr (1), he (1), it (1), la (1) |
| nm0229301 | Branko Djuric | 8 | bs (2), de (1), en (2), fr (1), hr (1), it (1), sh (1), sr (2) |
| nm0004486 | Bruno Ganz | 8 | de (4), en (2), es (1), fr (1), he (1), ja (1), sv (1), tr (1) |
| nm0001354 | Ciarán Hinds | 8 | ar (1), de (1), en (10), fr (1), he (1), it (1), ja (1), la (1) |
| nm0957193 | Hanns Zischler | 8 | ar (1), da (1), de (4), en (4), fr (2), he (1), it (1), ru (1) |
| nm0000353 | Willem Dafoe | 8 | ar (1), de (2), en (21), es (1), fr (1), it (2), ro (1), ru (1) |
| nm0004778 | Adrien Brody | 7 | de (1), en (11), fr (2), hr (1), ja (1), ru (1), tpi (1) |
| nm0226699 | Bogdan Diklic | 7 | bs (1), de (1), en (1), fr (1), hr (1), sh (1), sr (2) |
| nm0001691 | Geoffrey Rush | 7 | ar (1), de (2), en (11), fr (1), he (1), it (1), la (1) |
| nm0000172 | Harvey Keitel | 7 | bg (1), de (1), el (1), en (12), es (1), fr (2), ru (1) |
| nm0374949 | Johan Heldenbergh | 7 | bs (1), de (1), en (3), he (1), hr (1), nl (2), sr (1) |
| nm0627453 | John Neville | 7 | ar (1), de (1), en (4), fr (1), he (1), it (1), la (1) |
| nm0000596 | Jonathan Pryce | 7 | de (1), en (9), es (1), fr (1), it (1), la (1), pt (1) |
| nm0000553 | Liam Neeson | 7 | ar (1), en (11), fr (1), ja (1), la (1), sq (1), sr (1) |
| nm0323483 | Luis Gnecco | 7 | de (1), en (1), es (2), fr (1), it (1), la (1), pt (1) |
| nm0003909 | Michael Lonsdale | 7 | ar (2), de (1), en (4), fr (2), he (1), it (2), la (1) |
| nm0000899 | Monica Bellucci | 7 | bg (1), en (2), es (1), fr (1), it (2), la (1), ro (1) |
| nm0618768 | Mustafa Nadarević | 7 | bs (2), de (1), en (1), fr (1), hr (1), sh (1), sr (1) |

### Q7 — Pic de popularité en début de carrière

Au moins 20 ans et 15 films ; début de carrière : cinq premières années ; notes avec au moins 100 votes.
4 573 acteurs et actrices. Extrait : les 20 dont le film du pic a le plus de votes.

| nconst | primaryName | first_year | last_year | dated_films | early_peak_film | peak_year | early_peak_rating | later_peak_rating |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nm0006669 | William Sadler | 1990 | 2025 | 55 | tt0111161 | 1994 | 9.3 | 7.4 |
| nm0001570 | Edward Norton | 1996 | 2026 | 32 | tt0137523 | 1999 | 8.8 | 8.1 |
| nm0000641 | Gary Sinise | 1992 | 2026 | 30 | tt0109830 | 1994 | 8.8 | 8.2 |
| nm0482851 | Phil LaMarr | 1994 | 2026 | 23 | tt0110912 | 1994 | 8.8 | 7.1 |
| nm0000199 | Al Pacino | 1971 | 2026 | 60 | tt0068646 | 1972 | 9.2 | 8.3 |
| nm0000473 | Diane Keaton | 1970 | 2024 | 57 | tt0068646 | 1972 | 9.2 | 7.9 |
| nm0101710 | Billy Boyd | 1998 | 2025 | 24 | tt0120737 | 2001 | 8.9 | 7.5 |
| nm0089217 | Orlando Bloom | 2001 | 2026 | 31 | tt0167260 | 2003 | 9 | 7.4 |
| nm0000569 | Gwyneth Paltrow | 1995 | 2025 | 37 | tt0114369 | 1995 | 8.6 | 8.1 |
| nm0144307 | John Cassini | 1991 | 2023 | 30 | tt0114369 | 1995 | 8.6 | 6.7 |
| nm1055413 | Michael Fassbender | 2006 | 2026 | 36 | tt0361748 | 2009 | 8.4 | 8.1 |
| nm0505971 | Ted Levine | 1988 | 2026 | 38 | tt0102926 | 1991 | 8.6 | 8.2 |
| nm0000610 | Giovanni Ribisi | 1996 | 2024 | 45 | tt0120815 | 1998 | 8.6 | 7.9 |
| nm0001608 | Barry Pepper | 1998 | 2026 | 30 | tt0120815 | 1998 | 8.6 | 7.6 |
| nm0004874 | Vin Diesel | 1997 | 2023 | 28 | tt0120815 | 1998 | 8.6 | 8 |
| nm0122653 | Edward Burns | 1995 | 2026 | 28 | tt0120815 | 1998 | 8.6 | 7 |
| nm0356017 | Rebecca Hall | 2006 | 2026 | 34 | tt0482571 | 2006 | 8.5 | 7.1 |
| nm0000146 | Ralph Fiennes | 1992 | 2026 | 64 | tt0108052 | 1993 | 9 | 8.2 |
| nm0001110 | Embeth Davidtz | 1989 | 2024 | 27 | tt0108052 | 1993 | 9 | 7.3 |
| nm0328751 | Caroline Goodall | 1991 | 2026 | 28 | tt0108052 | 1993 | 9 | 7 |

### Q8 — Professionnels aux métiers multiples

381 967 professionnels avec au moins deux métiers sur l'ensemble de leur carrière. Synthèse : pour chaque
personne, moyenne des films à plusieurs rôles moins moyenne des films à un seul rôle (films d'au moins 100 votes).
Exécution `192d74d7dd8d48b085a7ae4924eb3708`, code des PR de correction.

| combination | people | multi_rated_films | single_rated_films | mean_multi_rating | mean_single_rating | mean_rating_delta | median_rating_delta | share_better_multi | share_worse_multi |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| any | 42,618 | 104,392 | 313,054 | 5.707 | 5.729 | -0.022 | 0 | 0.49 | 0.499 |
| director+writer | 20,886 | 54,476 | 113,126 | 5.825 | 5.836 | -0.011 | 0 | 0.492 | 0.497 |
| actor+writer | 5,866 | 8,528 | 62,072 | 5.516 | 5.559 | -0.042 | 0 | 0.493 | 0.497 |
| actor+director+writer | 3,892 | 6,408 | 41,655 | 5.479 | 5.56 | -0.081 | -0.05 | 0.475 | 0.517 |
| actor+director | 1,446 | 2,012 | 26,723 | 5.553 | 5.705 | -0.151 | -0.119 | 0.443 | 0.548 |

### Methodological Explanation

> **À RÉDIGER PAR LE GROUPE (sans IA)** : méthode utilisée et raisons de ce choix ; alternatives envisagées et compromis.

## Data Enrichment

Films de type `movie` de 1980 à 2025, d'au moins 40 minutes ; les 10 films les plus votés par genre et par année ;
les 3 premiers principaux de chaque film.

| Mesure | Valeur |
| --- | --- |
| Lignes film–genre–année | 10 198 |
| Films distincts | 7 186 |
| Principaux film–personne | 21 493 |
| Personnes distinctes | 11 282 |

### Couverture des films (synopsis)

| enrichment_status | enrichment_note | count | pct |
| --- | --- | --- | --- |
| enriched | | 5 986 | 83,30 |
| not_available | No Wikidata item carries this IMDb identifier (P345) | 490 | 6,82 |
| not_available | Wikipedia article has no plot or synopsis section | 382 | 5,32 |
| not_available | Wikidata item has no English Wikipedia article | 328 | 4,56 |

### Couverture des principaux (biographie)

| enrichment_status | enrichment_note | count | pct |
| --- | --- | --- | --- |
| enriched | | 18 128 | 84,34 |
| not_available | No Wikidata item carries this IMDb identifier (P345) | 2 115 | 9,84 |
| not_available | Wikidata item has no English Wikipedia article | 1 249 | 5,81 |
| not_available | Wikipedia article has no introduction text | 1 | 0,00 |

Chaque enregistrement porte `source_url` et, si un article a été lu, `source_revision_url` (permalien de la révision).

### Films selon le nombre de principaux listés par IMDb

| principal_count | principals_note | count |
| --- | --- | --- |
| 0 | IMDb lists only 0 principal(s) for this film (fewer than 3) | 7 |
| 1 | IMDb lists only 1 principal(s) for this film (fewer than 3) | 12 |
| 2 | IMDb lists only 2 principal(s) for this film (fewer than 3) | 20 |
| 3 | | 7 147 |

Langues originales exportées pour Q6 : 7 403 lignes film–langue (`data/reference/original_languages.csv`).

### Methodological Explanation

> **À RÉDIGER PAR LE GROUPE (sans IA)** : méthode utilisée et raisons de ce choix ; alternatives envisagées et compromis.

## Streaming

Test en direct du 15 novembre 2026, 16:37–17:08 UTC : flux Wikimedia `recentchange` réel (`enwiki` et
`wikidatawiki`) → Kafka → Spark Structured Streaming. Paramètres : watermark 10 minutes, fenêtres de 5 minutes,
déclenchement toutes les 30 secondes, lecture du topic depuis `earliest`. Une sonde datée de 30 minutes dans le passé
(`--late-probe "Inside Out (2015 film)" --minutes 30`) a été envoyée à 16:53.

### Métriques par requête

| query | minutes | batches | kafka_rows_read | rows_per_second | dropped_by_watermark | last_watermark |
| --- | --- | --- | --- | --- | --- | --- |
| matched_events | 30,8 | 63 | 16 285 | 8,82 | 1 | 2026-11-15 16:58:00 |
| window_counts | 30,7 | 63 | 16 285 | 8,83 | 1 | 2026-11-15 16:58:00 |

Producteur : flux interrompu à 16:52:34 (réponse terminée prématurément, puis échec de résolution DNS), reconnexion
automatique à 16:53:09.

### Événements reliés à IMDb

| match_type | events | distinct_items |
| --- | --- | --- |
| wikipedia_page | 36 | 22 |
| imdb_title | 26 | 13 |
| wikidata_item | 3 | 3 |

Éléments IMDb les plus modifiés :

| match_type | label | imdb_ids | events |
| --- | --- | --- | --- |
| wikipedia_page | Emily Osment | nm0652089 | 10 |
| imdb_title | The Three Musketeers | tt0108333, tt0486728, tt15049210, tt1509767, tt3274666, … | 5 |
| imdb_title | Street Fighter | tt0111301, tt1579705 | 4 |
| imdb_title | Jailer | tt11663228, tt21158466 | 3 |
| wikipedia_page | Danai Gurira | nm1775091 | 2 |
| imdb_title | The Cat in the Hat | tt0312528 | 2 |
| wikipedia_page | Forgetting Sarah Marshall | tt0800039 | 2 |
| imdb_title | Ebenezer | tt36667189, tt43658564 | 2 |
| imdb_title | Coraline | tt0327597 | 2 |
| wikipedia_page | Kelly Marie Tran | nm4511652 | 2 |

### Délai d'ingestion (événements réels)

| events | median_s | p95_s | max_s |
| --- | --- | --- | --- |
| 61 | 2,55 | 9,38 | 10,34 |

### Comptes par fenêtre de 5 minutes (temps de l'événement)

Seules les fenêtres closes par le watermark (16:58) au moment de l'arrêt sont écrites.

| window_start | window_end | match_type | events | distinct_items | mean_ingest_delay_s |
| --- | --- | --- | --- | --- | --- |
| 16:35 | 16:40 | imdb_title | 3 | 3 | 2,75 |
| 16:35 | 16:40 | wikipedia_page | 6 | 4 | 2,94 |
| 16:40 | 16:45 | imdb_title | 2 | 2 | 4,82 |
| 16:40 | 16:45 | wikipedia_page | 3 | 3 | 5,26 |
| 16:45 | 16:50 | imdb_title | 6 | 2 | 2,45 |
| 16:45 | 16:50 | wikipedia_page | 12 | 5 | 2,50 |
| 16:50 | 16:55 | imdb_title | 2 | 1 | 2,08 |
| 16:50 | 16:55 | wikidata_item | 1 | 1 | 1,54 |
| 16:50 | 16:55 | wikipedia_page | 5 | 4 | 3,41 |

### Sonde tardive

Lignes écartées par le watermark : 1 (`numRowsDroppedByWatermark`). Sondes présentes dans la sortie : 0.

### Methodological Explanation

> **À RÉDIGER PAR LE GROUPE (sans IA)** : méthode utilisée et raisons de ce choix ; alternatives envisagées et compromis.

> **À RÉDIGER PAR LE GROUPE (sans IA)** : explication de la méthode choisie pour les données arrivant en retard (exigée par la consigne).

## Open Question

### Question, intérêt et données utilisées

> **À RÉDIGER PAR LE GROUPE (sans IA)** : question étudiée, intérêt et données supplémentaires nécessaires.

### Résultats

Acteurs et actrices d'au moins 5 films notés, de nationalité identifiée. Sortie de `notebooks/05_open_question.ipynb`
sur l'enrichissement de la préparation `452e5d1ccb1142b3823d9ba935e401a0`.

| is_multilingual | num_actors | avg_imdb_rating | avg_career_years | avg_films |
| --- | --- | --- | --- | --- |
| Monolingual | 4 017 | 5,88 | 28,9 | 27,3 |
| Multilingual | 1 741 | 6,14 | 33,3 | 44,7 |

| is_anglophone | is_multilingual | num_actors | avg_imdb_rating | avg_career_years |
| --- | --- | --- | --- | --- |
| Anglophone | Monolingual | 2 843 | 5,76 | 27,9 |
| Anglophone | Multilingual | 1 066 | 6,11 | 32,5 |
| Non-Anglophone | Monolingual | 1 174 | 6,16 | 31,1 |
| Non-Anglophone | Multilingual | 675 | 6,19 | 34,4 |

| country | num_actors | avg_imdb_rating | avg_career_years |
| --- | --- | --- | --- |
| Japan | 272 | 6,66 | 29,9 |
| Germany | 114 | 6,31 | 31,7 |
| Brazil | 42 | 6,30 | 34,3 |
| Argentina | 26 | 6,26 | 35,3 |
| Ireland | 71 | 6,19 | 26,9 |
| Russia | 93 | 6,17 | 33,9 |
| China | 56 | 6,17 | 29,1 |
| Norway | 29 | 6,16 | 31,7 |
| India | 491 | 6,13 | 28,9 |
| United Kingdom | 699 | 6,12 | 28,5 |

> **À RÉDIGER PAR LE GROUPE (sans IA)** : principaux enseignements à tirer de ces résultats.

### Methodological Explanation

> **À RÉDIGER PAR LE GROUPE (sans IA)** : méthode utilisée et raisons de ce choix ; alternatives envisagées et compromis.
