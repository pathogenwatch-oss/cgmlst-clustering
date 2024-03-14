package main

import (
	"time"
)

// Messages
const (
	PROFILES_EXPECTED      = iota
	CACHE_DOC_PARSED       = iota
	CACHED_SCORES_EXPECTED = iota
	PROFILE_PARSED         = iota
	PROFILE_INDEXED        = iota
	SCORE_CALCULATED       = iota
	DISTANCES_STARTED      = iota
	CLUSTERING_STARTED     = iota
	RESULTS_TO_SAVE        = iota
	SAVED_RESULT           = iota
	EXIT                   = iota
)

// States
const (
	STARTING          = iota
	PARSING_CACHE     = iota
	PARSING_PROFILES  = iota
	INDEXING_PROFILES = iota
	SCORING           = iota
	CLUSTERING        = iota
	SAVING_RESULTS    = iota
	DONE              = iota
)

type ProgressEvent struct {
	EventType  int
	EventValue int
}

type ProgressMessage struct {
	Message  string  `json:"message"`
	Progress float32 `json:"progress"`
}

type ProgressWorker struct {
	lastProgress float32
	totalWork    int
	workDone     int
	state        int
	cachingCost  int
}

// Somewhat realistic relative costs
const (
	PARSE_COST = 46000
	// INDEX_COST   = 16000
	SCORE_COST   = 22
	CACHING_COST = SCORE_COST / 5
)

func (w *ProgressWorker) Update(msg ProgressEvent) {
	switch msg.EventType {
	case PROFILES_EXPECTED:
		nSts := msg.EventValue
		nScores := (nSts * (nSts - 1)) / 2
		w.totalWork = (PARSE_COST) * nSts
		w.totalWork += (SCORE_COST + w.cachingCost) * nScores
	case CACHE_DOC_PARSED:
		if w.state < PARSING_CACHE {
			w.state = PARSING_CACHE
		}
	case PROFILE_PARSED:
		if w.state < PARSING_PROFILES {
			w.state = PARSING_PROFILES
		} else if w.state > SAVING_RESULTS {
			return
		}
		w.workDone += PARSE_COST
	case CACHED_SCORES_EXPECTED:
		newTotal := w.totalWork - (msg.EventValue * SCORE_COST)
		w.workDone = w.workDone * newTotal / w.totalWork
		w.totalWork = newTotal
	case SCORE_CALCULATED:
		if w.state < SCORING {
			w.state = SCORING
		} else if w.state > SAVING_RESULTS {
			return
		}
		w.workDone += SCORE_COST
	case RESULTS_TO_SAVE:
		if w.state < SAVING_RESULTS {
			w.state = SAVING_RESULTS
		}
		w.cachingCost = (w.totalWork - w.workDone) / msg.EventValue
	case SAVED_RESULT:
		if w.state < SAVING_RESULTS {
			w.state = SAVING_RESULTS
		}
		w.workDone += w.cachingCost
	case EXIT:
		if w.state < DONE {
			w.state = DONE
		}
	default:
	}
}

func (w *ProgressWorker) Progress() ProgressMessage {
	if w.totalWork == 0 {
		return ProgressMessage{"Initialising", 0}
	}
	progress := 100.0 * (float32(w.workDone) / float32(w.totalWork))
	if progress > 99.999 {
		progress = 99.999
	}
	var message string
	switch w.state {
	case STARTING:
		message = "Initialising"
	case PARSING_CACHE:
		message = "Loading data from the cache"
	case PARSING_PROFILES:
		message = "Parsing CGMLST profilesMap"
	case INDEXING_PROFILES:
		message = "Indexing CGMLST profilesMap"
	case SCORING:
		message = "Calculating pairwise distances"
	case CLUSTERING:
		message = "Single-linkage clustering"
	case SAVING_RESULTS:
		message = "Saving results"
	case DONE:
		message = "Clustering complete"
	}
	return ProgressMessage{message, progress}
}

func NewProgressWorker() (chan ProgressEvent, chan ProgressMessage) {
	worker := ProgressWorker{cachingCost: CACHING_COST}
	input := make(chan ProgressEvent, 1000)
	output := make(chan ProgressMessage, 1000)

	go func() {
		for msg := range input {
			worker.Update(msg)
		}
	}()

	tick := time.Tick(time.Second)

	go func() {
		for {
			<-tick
			p := worker.Progress()
			if p.Progress > worker.lastProgress+0.1 {
				output <- p
				worker.lastProgress = p.Progress
			}
		}
	}()
	return input, output
}
