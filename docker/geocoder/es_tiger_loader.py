from zipfile import ZipFile
import boto3
import sys
import json
import csv
import os
import string
from io import BytesIO
import shapefile
import pyproj
from random import SystemRandom
from elasticsearch import Elasticsearch, helpers

CURRENT_DIR = os.path.dirname(__file__)

wgs84 = pyproj.Proj('+init=EPSG:4326')
nad83 = pyproj.Proj('+init=EPSG:4269')

s3 = boto3.resource('s3')
bucket = s3.Bucket('nvf-tiger-2016')


with open(os.path.join(CURRENT_DIR, 'es', 'censusSchema.json'), 'r') as f:
    censusSchema = json.load(f)

with open(os.path.join(CURRENT_DIR, 'es', 'synonyms.json'), 'r') as f:
    synonyms = json.load(f)

with open(os.path.join(CURRENT_DIR, 'es', 'fips_state_map.csv'), 'r') as f:
    fips_state_map = {}
    csv_reader = csv.reader(f, delimiter=',')
    for row in csv_reader:
        fips_state_map[row[1]] = row[0]

tiger_settings = {
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "state_synonyms": {
            "tokenizer": "standard",
            "filter": [
              "lowercase",
              "state_synonyms"
            ]
          },
          "address_synonyms": {
            "tokenizer": "standard",
            "filter": [
              "lowercase",
              "address_synonyms"
            ]
          }
        },
        "filter": {
          "state_synonyms": {
            "type": "synonym",
            "ignore_case": True,
            "synonyms": synonyms['state_synonyms']
          },
          "address_synonyms": {
            "type": "synonym",
            "ignore_case": True,
            "synonyms": synonyms['address_synonyms']
          }
        }
      }
    }
  },
  "mappings": {
    'addrfeat': censusSchema
  }
}


def process_zip(obj):
    tiger_key = obj.key[3:-4]
    tiger_bytes = BytesIO(obj.get()['Body'].read())
    zip_tiger = ZipFile(tiger_bytes)
    return shapefile.Reader(
        shp=BytesIO(zip_tiger.read('{}.shp'.format(tiger_key))),
        dbf=BytesIO(zip_tiger.read('{}.dbf'.format(tiger_key)))
    )


def process_records(reader, state_str):
    field_names = [f[0] for f in reader.fields[1:]]
    feature_list = list()

    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        # Getting type error on bytes, converting
        for k in atr:
            if isinstance(atr[k], bytes):
                atr[k] = atr[k].decode('utf-8').strip()

        atr['STATE'] = state_str
        geom = sr.shape.__geo_interface__
        geom['coordinates'] = [
            pyproj.transform(nad83, wgs84, p[0], p[1]) for p in geom['coordinates']
        ]

        feature_list.append(dict(type='Feature', geometry=geom, properties=atr))

    return feature_list


if __name__ == '__main__':
    rand_str = ''.join(SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(6))
    index_name = 'tiger-{}'.format(rand_str)

    es = Elasticsearch(host='elasticsearch')
    es.indices.create(index=index_name, body=tiger_settings)
    es.indices.put_alias(index=index_name, name='census')

    state_str = sys.argv[1].upper()
    prefix_str = fips_state_map[state_str]
    feature_list = []

    for o in bucket.objects.filter(Prefix='{}/'.format(prefix_str)):
        reader = process_zip(o)
        feature_list.extend(process_records(reader, state_str))

    es_gen = ({'_index': index_name,
               '_type': 'addrfeat',
               '_source': f} for f in feature_list)

    # TODO: Look into parallel and streaming
    helpers.bulk(es, es_gen)
    print(es.count(index=index_name))
