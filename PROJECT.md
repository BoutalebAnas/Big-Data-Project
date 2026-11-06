# Projet ECE Big Data Processing · IMDb

## Membres

La liste des contributeurs et leurs adresses e-mail sont à compléter par le groupe avant la remise.

## Installation

Le traitement local complet requiert Python 3.11 ou 3.12, Java 17, un accès HTTPS à PyPI
et à IMDb, 8 Gio de RAM disponibles et environ 20 Gio de disque libre pour les archives,
le travail Spark et une préparation. Sur Windows, exécuter les commandes dans Ubuntu WSL 2.
Pour de meilleures performances, cloner le projet sur le système de fichiers Linux de WSL.
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
toutes les cellules. Répéter avec `notebooks/02_data_exploration.ipynb`. L'exécution peut
aussi se faire sans interface depuis la racine. Les copies exécutées seront dans `/tmp`.

```bash
jupyter nbconvert --to notebook --execute --ExecutePreprocessor.kernel_name=imdb \
  --ExecutePreprocessor.timeout=-1 --output-dir=/tmp notebooks/01_data_preparation.ipynb
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

## Utilisation d'IA

OpenAI Codex a été utilisé pour développer le chargement, les contrôles de qualité, le
nettoyage, les requêtes d'exploration, les deux notebooks, les tests et ce guide technique.
Chaque membre rédige lui-même ses explications et raisonnements dans `REPORT.md`.
