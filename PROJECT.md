# Projet ECE Big Data Processing · IMDb

## Membres

La liste des contributeurs et leurs adresses e-mail sont à compléter par le groupe avant la remise.

## Installation

Prérequis pour le traitement local complet : Python 3.11 ou 3.12, Java 17, connexion HTTPS
à PyPI et IMDb, 8 Gio de RAM disponibles et environ 20 Gio de disque libre pour les archives,
le travail Spark et une préparation. Sur Windows, exécuter les commandes dans Ubuntu WSL 2.
Pour de meilleures performances, cloner le projet sur le système de fichiers Linux de WSL.
Java 17 doit être accessible via `JAVA_HOME` ou `PATH`.

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
toutes les cellules. L'exécution peut aussi se faire sans interface depuis la racine :

```bash
jupyter nbconvert --to notebook --execute --ExecutePreprocessor.kernel_name=imdb \
  --ExecutePreprocessor.timeout=-1 --output-dir=/tmp notebooks/01_data_preparation.ipynb
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

## Utilisation d'IA

OpenAI Codex a été utilisé pour développer le chargement, les contrôles de qualité, le
nettoyage, le notebook de préparation, les tests et ce guide technique. Chaque membre
rédige lui-même ses explications et raisonnements dans `REPORT.md`.
