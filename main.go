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
	progressIn, progressOut := NewProgressWorker()
	defer func() { progressIn <- ProgressEvent{EXIT, 0} }()
	results := make(chan ClusterOutput, 100)

	done := make(chan bool)
	go func() {
		for {
			select {
			case p := <-progressOut:
				enc.Encode(p)
			case r, more := <-results:
				if more {
					enc.Encode(r)
				} else {
					done <- true
				}
			}
		}
	}()

	profiles, scores, maxThreshold, existingClusters, canReuseCache, err := parse(r, progressIn)
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

	scoreComplete, errChan := scoreAll(&scores, &profiles, progressIn)

	select {
	case err := <-errChan:
		if err != nil {
			panic(err)
		}
	case <-scoreComplete:
	}
	log.Printf("%d scores remaining\n", scores.Todo())

	progressIn <- ProgressEvent{DISTANCES_STARTED, 0}
	distances, err := scores.Distances()
	if err != nil {
		panic(err)
	}

	progressIn <- ProgressEvent{CLUSTERING_STARTED, 0}
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

	progressIn <- ProgressEvent{RESULTS_TO_SAVE, maxThreshold + 1}
	for c := range clusters.Format(maxThreshold, distances, scores.STs) {
		results <- c
		progressIn <- ProgressEvent{SAVED_RESULT, 1}
	}

	close(results)
	<-done
}
