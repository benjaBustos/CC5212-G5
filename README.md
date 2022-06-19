# CC5212-G5
Repository for Group-5's project coursing CC5212-1 

## Data ingestion

To ingest taxonomic data run `data_ingestion.py`, which results in a test file with only 1000 sightings, and the full dataset.

## Closest relative with most sightings 

To generate closest relative with most sightings, package project as jar using maven, and run `ClosestRelativeWithMostSightings` class with path of dataset and path of result.

## Results

To generate results using the output of Closest Relative With Most Sightings Spark job, package project as jar using maven and run `Results` class with path of the closest relative with most sightings dataset, path where to write the first result, and path where to write second result.
