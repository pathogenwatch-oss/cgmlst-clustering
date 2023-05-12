package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}
	//var stdinScanner = bufio.NewScanner(os.Stdin)
	//file, err := os.Open("testdata/small_test.json")
	//if err != nil {
	//	panic(err)
	//}

	var stdinReader = bufio.NewReaderSize(os.Stdin, 16000000)
	_main(stdinReader, os.Stdout)
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
				err := enc.Encode(progress)
				if err != nil {
					return
				}
			case result, more := <-results:
				if more {
					err := enc.Encode(result)
					if err != nil {
						return
					}
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
	if scores, err = NewScores(request, &cache, index); err != nil {
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
		clusters, err = ClusterFromCache(distances, nItems, &cache)
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
