package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"
	"time"

	"gitlab.com/cgps/bsonkit"
)

type ClusterDetails struct {
	Genome  bsonkit.ObjectID `json:"genome"`
	Cluster bsonkit.ObjectID `json:"cluster"`
}

type ClusterOutput struct {
	Threshold int              `json:"threshold"`
	Genomes   []ClusterDetails `json:"genomes"`
}

type ClusterIndex struct {
	Pi     []int    `json:"pi"`
	Lambda []int    `json:"lambda"`
	Sts    []string `json:"sts"`
}

func isSmaller(a, b bsonkit.ObjectID) bool {
	return bytes.Compare(a[:], b[:]) < 0
}

func main() {
	r := (io.Reader)(os.Stdin)
	enc := json.NewEncoder(os.Stdout)
	progress := ProgressWorker(enc)
	defer func() { progress <- ProgressEvent{EXIT, 0} }()

	STs, _, profiles, scores, _, err := parse(r, progress)
	if err != nil {
		panic(err)
	}

	cacheDocs := make(chan CacheOutput, 1000)
	cacheFinished := make(chan bool, 2)
	go func() {
		for {
			doc, more := <-cacheDocs
			if !more {
				cacheFinished <- true
				return
			}
			enc.Encode(doc)
			progress <- ProgressEvent{CACHED_RESULT, 1}
		}
	}()

	scoreCache := MakeScoreCacher(&scores, cacheDocs)
	tick := time.Tick(time.Second)
	go func() {
		for {
			<-tick
			log.Printf("%d scores remaining\n", scoreCache.Remaining())
		}
	}()
	scoreComplete, errChan := scoreAll(scores, profiles, progress, scoreCache)

	select {
	case err := <-errChan:
		if err != nil {
			panic(err)
		}
	case <-scoreComplete:
	}

	progress <- ProgressEvent{DISTANCES_STARTED, 0}
	distances, err := scores.Distances()
	if err != nil {
		panic(err)
	}
	progress <- ProgressEvent{DISTANCES_COMPLETE, 0}

	progress <- ProgressEvent{CLUSTERING_STARTED, 0}
	clusters, err := NewClusters(len(scores.STs), distances)
	if err != nil {
		panic(err)
	}

	enc.Encode(ClusterIndex{
		clusters.pi,
		clusters.lambda,
		scores.STs,
	})

	<-cacheFinished

	log.Printf("%d scores remaining\n", scoreCache.Remaining())
	log.Printf("STs: %d; Scores: %d\n", len(STs), len(scores.scores))
}
