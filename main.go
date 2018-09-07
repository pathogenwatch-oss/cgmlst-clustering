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

func isSmaller(a, b bsonkit.ObjectID) bool {
	return bytes.Compare(a[:], b[:]) < 0
}

func main() {
	r := (io.Reader)(os.Stdin)
	enc := json.NewEncoder(os.Stdout)
	progress := ProgressWorker(enc)
	defer func() { progress <- ProgressEvent{EXIT, 0} }()

	STs, profiles, scores, maxThreshold, existingClusters, canReuseCache, err := parse(r, progress)
	if err != nil {
		panic(err)
	}

	tick := time.Tick(time.Second)
	go func() {
		for {
			<-tick
			log.Printf("%d scores remaining\n", scores.Todo())
		}
	}()
	scoreComplete, errChan := scoreAll(scores, profiles, progress)

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
	var clusters Clusters
	if canReuseCache {
		clusters, err = UpdateClusters(existingClusters, len(scores.STs), distances)
		if err != nil {
			panic(err)
		}
	} else {
		clusters, err = NewClusters(len(scores.STs), distances)
		if err != nil {
			panic(err)
		}
	}

	enc.Encode(clusters.Format(maxThreshold, distances, scores.STs))

	log.Printf("%d scores remaining\n", scores.Todo())
	log.Printf("STs: %d; Scores: %d\n", len(STs), len(scores.scores))
}
