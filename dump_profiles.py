#!/usr/bin/env python3

import sys
import pymongo
import bson

from pymongo import MongoClient
mongo_url = 'mongodb://mongodb-1,mongodb-2,mongodb-3:27017'
mongo_client = MongoClient(mongo_url)

genomes_collection = mongo_client['wgsa-edge']['genomes']
cursor = genomes_collection.find(
  { "analysis.cgmlst": { '$exists': 1} },
  { '_id': 1, 'public': 1, 'fileId': 1, "analysis.cgmlst": 1, "organismId": 1}
)

errors = []

for i, doc in enumerate(cursor):
  try:
    cgmlst = doc['analysis']['cgmlst']
    _id = str(doc['_id'])
    organismId = doc['organismId']
    fileId = doc['fileId']
    public = doc['public']
    version = cgmlst.get('__v', 0)
    matches = cgmlst['matches']
    minifiedMatches = { m["gene"]: m["id"] for m in matches }
    print(bson.dumps({
      "_id": _id,
      "organismId": organismId,
      "fileId": fileId,
      "public": public,
      "version": version,
      "matches": minifiedMatches
    }), file=sys.stdout)
  except:
    try:
      print("Problem parsing %s" % str(doc['_id']), file=sys.stderr)
      errors.append(str(doc['_id']))
    except:
      pass
  if (i + 1) % 500 == 0:
    print("Processed %s sequences" % (i+1), file=sys.stderr)

print("Errors:\n%s" % "\n".join(errors), file=sys.stderr)