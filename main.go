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
	_main(os.Stdin, os.Stdout)
}

func _main(r io.Reader, w io.Writer) ([]CgmlstSt, Clusters, []int) {
	enc := json.NewEncoder(w)
	progressIn, progressOut := NewProgressWorker()
	defer func() { progressIn <- ProgressEvent{EXIT, 0} }()
	results := make(chan ClusterOutput, 100)

	done := make(chan bool)
	go func() {
		for {
			select {
			case progress := <-progressOut:
				enc.Encode(progress)
			case result, more := <-results:
				if more {
					enc.Encode(result)
				} else {
					done <- true
				}
			}
		}
	}()

	request, cache, index, err := parse(r, progressIn)
	if err := index.Complete(); err != nil {
		panic(err)
	}

	var scores scoresStore
	if scores, err = NewScores(request, cache, index); err != nil {
		panic(err)
	}

	progressIn <- ProgressEvent{CACHED_SCORES_EXPECTED, scores.Done()}

	tick := time.Tick(time.Second)
	go func() {
		for {
			<-tick
			log.Printf("%d scores remaining\n", scores.Todo())
		}
	}()

	scoreComplete, errChan := scores.Complete(index, progressIn)

	select {
	case err := <-errChan:
		if err != nil {
			panic(err)
		}
	case <-scoreComplete:
	}
	log.Printf("%d scores remaining\n", scores.Todo())

	progressIn <- ProgressEvent{CLUSTERING_STARTED, 0}

	var distances []int
	if distances, err = scores.Distances(); err != nil {
		panic(err)
	}
	nItems := len(scores.STs)

	var clusters Clusters
	if scores.canReuseCache {
		clusters, err = ClusterFromCache(distances, nItems, cache)
		if err != nil {
			panic(err)
		}
	} else {
		clusters, err = ClusterFromScratch(distances, nItems)
		if err != nil {
			panic(err)
		}
	}

	progressIn <- ProgressEvent{RESULTS_TO_SAVE, request.Threshold + 1}
	for c := range clusters.Format(request.Threshold, distances, scores.STs) {
		results <- c
		progressIn <- ProgressEvent{SAVED_RESULT, 1}
	}

	close(results)
	<-done
	return scores.STs, clusters, distances
}
