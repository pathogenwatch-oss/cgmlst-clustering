package main

import (
	"encoding/json"
	"time"
)

const (
	PARSING_STARTED      = iota
	PROFILES_EXPECTED    = iota
	PROFILE_PARSED       = iota
	PARSING_COMPLETE     = iota
	PROFILE_INDEXED      = iota
	SCORE_UPDATED        = iota
	SCORING_COMPLETE     = iota
	CACHED_RESULT        = iota
	DISTANCES_STARTED    = iota
	DISTANCES_COMPLETE   = iota
	CLUSTERING_STARTED   = iota
	CLUSTERING_COMPLETED = iota
	EXIT                 = iota
)

type ProgressEvent struct {
	EventType  int
	EventValue int
}

type ProgressMessage struct {
	Progress float32 `json:"progress"`
	Message  string  `json:"message"`
}

func ProgressWorker(output *json.Encoder) (input chan ProgressEvent) {
	input = make(chan ProgressEvent, 1000)
	var (
		progress         float32
		parsingStep      float32
		indexingStep     float32
		scoringStep      float32
		cachingStep      float32
		nextUpdate       float32
		parsingMessages  int
		scoringMessages  int
		indexingMessages int
		cachingMessages  int
		other            int
		nScores          int
		nSts             int
		message          string
	)

	go func() {
		message = "Loading data"

		for msg := range input {
			// PARSING_STARTED      = 1%
			// PROFILES_EXPECTED    = 1%
			// PROFILE_PARSED       = 8%
			// PARSING_COMPLETE     = 1%
			// PROFILE_INDEXED      = 14%
			// SCORE_UPDATED        = 52%
			// SCORING_COMPLETE     = 1%
			// CACHED_RESULT        = 17%
			// DISTANCES_STARTED    = 1%
			// DISTANCES_COMPLETE   = 1%
			// CLUSTERING_STARTED   = 1%
			// CLUSTERING_COMPLETED = 1%
			// EXIT                 = 1%
			switch msg.EventType {
			case PARSING_STARTED:
				message = "Loading data"
				other++
			case PROFILES_EXPECTED:
				other++
				nSts = msg.EventValue
				parsingStep = 8.0 / float32(nSts)
				indexingStep = 14.0 / float32(nSts)
				nScores = (nSts * (nSts - 1)) / 2
				scoringStep = 52.0 / float32(nScores)
				cachingStep = 17.0 / float32(nSts)
			case PROFILE_PARSED:
				message = "Loading data"
				parsingMessages++
			case PARSING_COMPLETE:
				other++
			case PROFILE_INDEXED:
				message = "Indexing data"
				indexingMessages++
			case SCORE_UPDATED:
				message = "Calculating distances"
				scoringMessages++
			case SCORING_COMPLETE:
				other++
			case CACHED_RESULT:
				message = "Saving results"
				cachingMessages++
			case DISTANCES_STARTED:
				message = "Clustering"
				other++
			case DISTANCES_COMPLETE:
				message = "Clustering"
				other++
			case CLUSTERING_STARTED:
				message = "Clustering"
				other++
			case CLUSTERING_COMPLETED:
				message = "Clustering"
				other++
			case EXIT:
				message = "Clustering"
				other++
			default:
			}
		}
	}()

	tick := time.Tick(time.Second)

	go func() {
		for {
			<-tick
			// This looks stupid (and it is) but I had to do this because of floating point arithmetic
			// If you have 7000 sequences, there are a lot of scores.  The step for each score gets
			// really small.  Once the progress gets to about 64, adding a really small number to it
			// results in the same number and the progress value stops going up :(
			// i.e. in float32 maths: 64 + 1e-6 === 64
			progress = float32(other)
			if parsingMessages > nSts {
				progress += parsingStep * float32(nSts)
			} else {
				progress += parsingStep * float32(parsingMessages)
			}
			if indexingMessages > nSts {
				progress += indexingStep * float32(nSts)
			} else {
				progress += indexingStep * float32(indexingMessages)
			}
			if scoringMessages > nScores {
				progress += scoringStep * float32(nScores)
			} else {
				progress += scoringStep * float32(scoringMessages)
			}
			if cachingMessages > nSts {
				progress += cachingStep * float32(nSts)
			} else {
				progress += cachingStep * float32(cachingMessages)
			}
			if progress > 99.999 {
				progress = 99.999
			}
			if progress > nextUpdate {
				output.Encode(ProgressMessage{progress, message})
				nextUpdate = progress + 0.1
			}
		}
	}()
	return
}
