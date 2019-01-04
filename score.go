package main

import (
	"log"
	"sync"
)

type Comparer struct {
	indices []Index
}

func (c *Comparer) compare(stA int, stB int) int {
	indexA := c.indices[stA]
	indexB := c.indices[stB]
	geneCount := CompareBits(indexA.Genes, indexB.Genes)
	alleleCount := CompareBits(indexA.Alleles, indexB.Alleles)
	return geneCount - alleleCount
}

func scoreProfiles(workerID int, jobs chan int, scores *scoresStore, comparer Comparer, wg *sync.WaitGroup) {
	nScores := 0
	defer func() {
		log.Printf("Worker %d has computed %d scores", workerID, nScores)
	}()
	defer wg.Done()
	for {
		j, more := <-jobs
		if !more {
			return
		}
		score := &(scores.scores[j])
		scores.SetIdx(j, comparer.compare(score.stA, score.stB), COMPLETE)
		nScores++
		if nScores%100000 == 0 {
			log.Printf("Worker %d has computed %d scores", workerID, nScores)
		}
	}
}

type scoresResult struct {
	STs    []CgmlstSt
	Scores []int
}

func scoreAll(scores *scoresStore, indexer *Indexer, progress chan ProgressEvent) (done chan bool, err chan error) {
	numWorkers := 10
	var scoreWg sync.WaitGroup

	err = make(chan error)
	done = make(chan bool)

	_scoreTasks := make(chan int, 50)
	scoreTasks := make(chan int)
	go func() {
		for task := range _scoreTasks {
			scoreTasks <- task
			progress <- ProgressEvent{SCORE_CALCULATED, 1}
		}
		close(scoreTasks)
	}()

	go func() {
		for idx, score := range scores.scores {
			if score.status == PENDING {
				_scoreTasks <- idx
			}
		}
		close(_scoreTasks)
	}()

	for i := 1; i <= numWorkers; i++ {
		scoreWg.Add(1)
		go scoreProfiles(i, scoreTasks, scores, Comparer{indexer.indices}, &scoreWg)
	}

	go func() {
		scoreWg.Wait()
		done <- true
	}()

	return
}
