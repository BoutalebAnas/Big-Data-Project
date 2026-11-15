# Projet ECE Big Data Processing · IMDb

## Membres

| Membre | E-mail | GitHub | Parties |
| --- | --- | --- | --- |
| Thomas Soubagne | thomas.soubagne@edu.ece.fr | thomassbgn | Data Preparation, Data Exploration |
| *à compléter* | *à compléter* | TaDLpZ | Data Enrichment |
| Anas Boutaleb | anas.boutaleb@edu.ece.fr | BoutalebAnas | Streaming, Open Question |

## Ordre d'exécution

| Étape | Commande ou notebook | Produit | Durée indicative |
| --- | --- | --- | --- |
| 1 | `python -m pytest -q` | Vérification de l'environnement | 2 min |
| 2 | `notebooks/01_data_preparation.ipynb` | `data/processed/<préparation>/` | 35 min |
| 3 | `notebooks/03_data_enrichment.ipynb` | `data/enrichment/output/<préparation>/`, `data/reference/original_languages.csv` | 3 min avec les caches, 1 à 2 h sans |
| 4 | `notebooks/02_data_exploration.ipynb` | `data/results/<préparation>/<exécution>/` | 10 min |
| 5 | Kafka, producteur et pipeline, puis `notebooks/04_streaming.ipynb` | `data/streaming/output/` | 30 min |
| 6 | `notebooks/05_open_question.ipynb` | Tableaux de la question ouverte | 5 min |

Le notebook 03 doit précéder le 02 : il crée le référentiel des langues originales qui fait passer Q6
en mode `verified`. Les notebooks 04 et 05 lisent la sortie d'enrichissement du notebook 03.

## Installation

Le traitement local complet requiert Python 3.11 ou 3.12, Java 17, Docker (pour Kafka), un accès HTTPS
à PyPI, à IMDb et aux API Wikimedia, 8 Gio de RAM disponibles et environ 20 Gio de disque libre pour les
archives, le travail Spark et une préparation. Sur Windows, exécuter les commandes dans Ubuntu WSL 2
(recommandé), ou installer les binaires Hadoop décrits dans [setup.md](setup.md) pour travailler sous
Windows natif. Pour de meilleures performances sous WSL, cloner le projet sur le système de fichiers Linux.
Java 17 doit être accessible via `JAVA_HOME` ou `PATH`.

Sur Ubuntu ou Ubuntu WSL, installer les prérequis depuis le terminal Linux.

```bash
sudo apt-get update
sudo apt-get install openjdk-17-jdk python3 python3-venv
```

Depuis la racine du dépôt, créer l'environnement Python et lancer Jupyter.

```bash
java -version
python3 --version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m ipykernel install --prefix "$VIRTUAL_ENV" --name imdb --display-name "Python (IMDb)"
python -m pytest -q
jupyter lab
```

Ouvrir `notebooks/01_data_preparation.ipynb` avec le noyau **Python (IMDb)** puis exécuter
toutes les cellules. Continuer dans l'ordre du tableau ci-dessus : 03 (section *Enrichissement*),
puis 02, 04 (section *Streaming*) et 05. L'exécution peut aussi se faire sans interface depuis la
racine. Les copies exécutées seront dans `/tmp`.

```bash
jupyter nbconvert --to notebook --execute --ExecutePreprocessor.kernel_name=imdb \
  --ExecutePreprocessor.timeout=-1 --output-dir=/tmp notebooks/01_data_preparation.ipynb
# notebook 03 : voir la section Enrichissement (WIKI_CONTACT requis)
jupyter nbconvert --to notebook --execute --ExecutePreprocessor.kernel_name=imdb \
  --ExecutePreprocessor.timeout=-1 --output-dir=/tmp notebooks/02_data_exploration.ipynb
```

Les archives existantes dans `data/raw/` sont réutilisées. Les autres sont téléchargées
depuis le [point de distribution IMDb](https://datasets.imdbws.com/). IMDb actualise
les données quotidiennement. Conserver les mêmes sept archives pour reproduire un calcul
et remplacer les sept ensemble pour changer de millésime. Les fichiers sont soumis
aux [conditions d'utilisation IMDb](https://data.imdb.com/non-commercial-datasets/).

## Données et interfaces

| Table | Clé | Liens |
| --- | --- | --- |
| `title.basics` | `tconst` | Dimensions titre et genre |
| `name.basics` | `nconst` | `knownForTitles[] → title.basics.tconst` |
| `title.akas` | `titleId, ordering` | `titleId → title.basics.tconst` |
| `title.crew` | `tconst` | `tconst → title.basics`, `directors[] / writers[] → name.basics.nconst` |
| `title.episode` | `tconst` | Épisode et série via `tconst / parentTconst → title.basics.tconst` |
| `title.principals` | `tconst, ordering` | `tconst → title.basics`, `nconst → name.basics` |
| `title.ratings` | `tconst` | `tconst → title.basics` |

`data/processed/LATEST` désigne la dernière préparation terminée. Son sous-dossier contient
`tables/<table>`, `quality/missing`, `quality/anomalies` (Parquet), et `manifest.json`.
Les autres parties peuvent appeler `prepared_snapshot` et `read_prepared` dans
`src/imdb_loader.py` depuis leurs propres notebooks. Une exécution interrompue laisse le
pointeur en place. Les anciens répertoires de préparation peuvent être retirés localement
une fois inutilisés.

`IMDB_DATA_DIR` redirige l'arborescence de données. Un chemin relatif est résolu depuis le dépôt.
`IMDB_SPARK_THREADS`, `IMDB_SPARK_PARTITIONS` et `IMDB_SPARK_MEMORY` configurent le calcul
(4 threads, 32 partitions et 4g par défaut). `IMDB_SCRATCH_DIR` désigne un répertoire
temporaire existant pour décompresser un TSV à la fois, sans chargement global en Python.
Définir `IMDB_EXPAND_GZIP=0` pour lire directement les archives avec Spark si le disque
temporaire est limité. Chaque archive gzip est alors lue par une seule tâche Spark.

Les références orphelines sont comptées après le nettoyage des clés. Les valeurs sont
conservées dans les tables, et les jointures analytiques sur une entité exigent un parent.
Les champs à listes restent séparés par des virgules, conformément au format d'entrée.

## Exploitation des résultats

Les seuils des huit questions sont regroupés dans `PARAMETERS` au début du deuxième
notebook. Chaque exécution exporte ses DataFrames dans
`data/results/<préparation>/<exécution>/`. `summary.json` y consigne les seuils, les
volumes et la couverture des langues. La présence de ce fichier indique une exécution
terminée. Les tables peuvent être relues avec `spark.read.parquet(chemin)`.

Les crédits publics sont limités aux principaux pour les acteurs et plusieurs autres
métiers. Les notes et votes sont ceux de l'instantané utilisé, y compris pour les films
anciens. Le notebook affiche les hypothèses de chaque requête ainsi que les tableaux
destinés à la vérification. Les exports de préparation contiennent les profils,
les anomalies et des exemples. Les exports d'exploration comprennent le détail par
genre, le classement annuel, les comparaisons et les résultats individuels.

### Langue originale des films

IMDb fournit la langue d'un titre alternatif dans `title.akas`. En l'absence d'un
référentiel externe, Q6 exporte seulement un indicateur nommé `title_proxy` et sa
couverture. Pour calculer Q6 sur les langues originales des films, créer le fichier UTF-8
`data/reference/original_languages.csv` avec l'en-tête `tconst,language,source_uri`.
Fournir un titre IMDb, un code ISO 639-1 ou ISO 639-3 normalisé et une URL justifiant
la langue, à raison d'une ligne par couple titre–langue. Utiliser le même code pour une
même langue. Le mode `verified` analyse la couverture du fichier fourni et enregistre
son empreinte SHA-256. Les résultats restent bornés aux films documentés et aux
crédits publics. Wikidata offre une piste pour construire ce référentiel en joignant
les identifiants IMDb aux déclarations de langue originale, après vérification des
sources. [Identifiant IMDb P345](https://www.wikidata.org/wiki/Property:P345) ·
[Langue originale P364](https://www.wikidata.org/wiki/Property:P364)

Le notebook `03_data_enrichment.ipynb` construit ce fichier à partir de Wikidata (P364)
pour les films enrichis.

## Enrichissement

Les API Wikimedia exigent une adresse de contact dans l'en-tête `User-Agent`. La définir avant
d'ouvrir Jupyter, puis exécuter `notebooks/03_data_enrichment.ipynb`.

```bash
export WIKI_CONTACT=prenom.nom@edu.ece.fr
jupyter nbconvert --to notebook --execute --ExecutePreprocessor.kernel_name=imdb \
  --ExecutePreprocessor.timeout=-1 --output-dir=/tmp notebooks/03_data_enrichment.ipynb
```

Les réponses de Wikidata et de Wikipedia sont mises en cache dans `data/enrichment/raw/` (versionné).
Seuls les identifiants absents du cache sont demandés, à raison d'une requête au plus toutes les
0,2 seconde. `GATHER_LIMIT=10` limite la collecte pour un premier essai. Les tables Spark sont écrites
dans `data/enrichment/output/<préparation>/films` et `.../principals`.

## Streaming

Démarrer Kafka, puis lancer le pipeline et le producteur dans deux terminaux, depuis la racine du dépôt
et avec l'environnement Python activé.

```bash
docker compose up -d
```

```bash
# Terminal 1 : pipeline Spark, arrêt automatique après 30 minutes
STREAM_DURATION_MINUTES=30 python -m src.streaming_pipeline
```

```bash
# Terminal 2 : producteur Wikimedia vers Kafka (Ctrl+C pour l'arrêter à la fin)
export WIKI_CONTACT=prenom.nom@edu.ece.fr
python -m src.streaming_producer
```

Pour vérifier le traitement des données tardives, envoyer pendant l'exécution un événement daté de
30 minutes dans le passé. Il doit être écarté par le watermark.

```bash
python -m src.streaming_producer --late-probe "Inside Out (2015 film)" --minutes 30
```

Le pipeline crée le topic s'il n'existe pas et charge la table de référence depuis la préparation et
la sortie d'enrichissement. Il écrit `matched_events` et `window_counts` en Parquet dans
`data/streaming/output/`, avec leurs checkpoints dans `data/streaming/checkpoints/`, et la progression
de chaque batch dans `data/streaming/output/metrics/progress.jsonl`. Ouvrir ensuite
`notebooks/04_streaming.ipynb` pour les métriques. Supprimer `data/streaming/` avant un nouveau test
indépendant.

| Variable | Défaut | Rôle |
| --- | --- | --- |
| `KAFKA_BROKER` | `localhost:9092` | Adresse du broker |
| `KAFKA_TOPIC` | `wiki-changes` | Topic Kafka |
| `STREAM_WIKIS` | `enwiki,wikidatawiki` | Wikis transmis par le producteur |
| `STREAM_WATERMARK` | `10 minutes` | Retard toléré sur le temps de l'événement |
| `STREAM_WINDOW` | `5 minutes` | Taille des fenêtres de comptage |
| `STREAM_TRIGGER` | `30 seconds` | Intervalle entre deux micro-batchs |
| `STREAM_STARTING_OFFSETS` | `earliest` | Position de lecture initiale du topic |
| `STREAM_DURATION_MINUTES` | aucun | Durée avant arrêt automatique |

Arrêter Kafka avec `docker compose down`.

## Question ouverte

Exécuter `notebooks/05_open_question.ipynb` après le notebook 03. Il lit la sortie d'enrichissement
et `data/reference/original_languages.csv`, sans nouvel accès réseau.

## Utilisation d'IA

| Outil | Tâches |
| --- | --- |
| OpenAI Codex | Chargement, contrôles de qualité, nettoyage, requêtes d'exploration, notebooks 01 et 02, tests associés, première version de ce guide |
| Claude Code (Anthropic) | Audit du dépôt par rapport à la consigne ; correctifs de l'enrichissement (nettoyage du texte, signalement des films à moins de trois principaux) ; Q1 robuste, synthèse de Q8 et message de Q6 ; réécriture de la correspondance IMDb et du traitement des données tardives dans le streaming, avec ses tests ; `.gitignore` des sorties ; scripts d'extraction des tableaux de `REPORT.md` ; sections d'exécution de ce guide |
| *à compléter par chaque membre* | *autres outils et tâches* |

Aucune explication, aucun choix méthodologique et aucune conclusion de `REPORT.md` n'a été rédigé
avec une IA. Les tableaux de résultats qu'il contient sont des sorties de requêtes.
