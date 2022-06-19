import io
import boto3
import botocore
from botocore import UNSIGNED
from botocore.config import Config
import pyarrow.parquet as pq

BUCKET_NAME = 'gbif-open-data-sa-east-1'
# Just read a single (~100MB) chunk of the table
PATH = 'occurrence/2022-04-01/occurrence.parquet/000000'

buffer = io.BytesIO()
s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
s3_object = s3.Object(BUCKET_NAME, PATH)
s3_object.download_fileobj(buffer)

table = pq.read_table(buffer)
df = table.to_pandas()

# Only taxonomic information of each sighting.
df1 = df[['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']]

# Dataset with all instances.
df2 = df1.dropna()

# Dataset with only 1000 instances for testing.
df3 = df2.head(1000)

df2.to_csv(
    './sightings_data.tsv',
    header=False,
    sep='\t',
    index=False,
    mode='w',
    line_terminator='\n',
    encoding='utf-8'
)

df3.to_csv(
    './sightings_data_test.tsv',
    header=False,
    sep='\t',
    index=False,
    mode='w',
    line_terminator='\n',
    encoding='utf-8'
)
