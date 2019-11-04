# Clustering

This module measures the distance between cgMLST profiles and performs Single Linkage Clustering
using the SLINK algorithm.  For N documents, SLINK calculates two arrays of length N called `pi` 
and `lambda`.  The documents can be clustered quickly from these two arrays at arbitrary thresholds.

## Inputs

This takes documents of three types
1. Request - always 1
2. Cache - 1 or none
3. Profiles - more than one and depends on the request

All inputs are encoded in BSON

The request lists the STs which we would like to cluster together.  It also specifies the threshold
below which we should record a cache of distances between STs (i.e. if the distance is greater than
this value, the value is not recorded).

The cache is optional.  It includes the known scores between a set of documents, a list of STs which
those distances refer to, and the SLINK parameters (`pi` & `lambda`).  Note that the order of the STs in
the cache matter.  We can reuse the parameters `lambda` and `pi` if all of the cache STs are in the list
of request STs.  This requires us to create a new ordering of STs in the output which starts with the
cache STs and does not have any duplicates.  We can call these the `requestedSTs`, `cachedSTs` and 
`outputSTs` respectivly.

Profiles are just the analysis documents from the cgMLST tasks.  They may be supplied in any order.

## Outputs

It outputs
1. Progress events - normally more than one
2. Score documents - T + 2 where T is the "threshold"

All outputs are encoded in JSON 

Progress events are periodically sent and give an estimate of the progress as a percentage.

A score document is returned for each distance between 0 and T listing pairs of STs which are that 
distance from one another.  The pairs are encoded as the index into the array of `outputSTs`.  An 
additonal document is also sent which includes the SLINK parameters `pi` and `lambda`.

## Internals

The cache includes the SLINK parameters `pi` and `lambda` as well as the order of the STs which
they correspond to.  They also include the distances between STs below a specified threshold.
These are stored as a map of the distance and the pair of the edges which are that distance from
one another.  These are encoded as the index of the ST into the STs array.  Other data structures
might be simpler but this is probably one of the more scalable as the datasets get really big 
(i.e. 100k+ genomes) and it makes it easier to split the cache across multiple records in the
database (which can only hold documents of up to 16MB).

The index is an efficient storage structure which holds the cgMLST records as bitarrays.  These
can then be compared really quickly.

The scores code holds an array of all the STs we'd like to compare in the order in which they
will need to be given to the clustering code.  This datastructure records whether the STs have
already been scored (or taken from the cache) so that jobs can be distributed across the workers.

## Testdata

Rather than storing hundreds of MB of testdata in binary JSON, I've included a JS script which
you can use to deterministically generate testdata (somewhat randomly).

```
cd testdata
yarn
node createTestData.js
cd ..
go test
```