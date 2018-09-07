package main

import (
	"fmt"
	"log"
	"sync"
)

type Index struct {
	Genes   *BitArray
	Alleles *BitArray
	Ready   bool
}

type AlleleKey struct {
	Gene   string
	Allele interface{}
}

type Tokeniser struct {
	sync.Mutex
	lookup    map[AlleleKey]uint64
	nextValue chan uint64
	lastValue uint64
}

func NewTokeniser() *Tokeniser {
	t := Tokeniser{
		nextValue: make(chan uint64),
		lookup:    make(map[AlleleKey]uint64),
	}
	go func() {
		var i uint64
		for i = 0; ; i++ {
			t.nextValue <- i
		}
	}()
	return &t
}

func (t *Tokeniser) Get(key AlleleKey) uint64 {
	t.Lock()
	defer t.Unlock()
	if value, ok := t.lookup[key]; ok {
		return value
	}
	value := <-t.nextValue
	t.lookup[key] = value
	t.lastValue = value
	return value
}

type Indexer struct {
	sync.Mutex
	geneTokens   *Tokeniser
	alleleTokens *Tokeniser
	indices      []Index
	lookup       map[CgmlstSt]int
}

func NewIndexer(lookup map[CgmlstSt]int) *Indexer {
	nSts := len(lookup)
	return &Indexer{
		geneTokens:   NewTokeniser(),
		alleleTokens: NewTokeniser(),
		indices:      make([]Index, nSts),
		lookup:       lookup,
	}
}

func (i *Indexer) Index(profile Profile) *Index {
	var (
		offset int
		ok     bool
		index  *Index
	)

	defer i.Unlock()
	i.Lock()
	if offset, ok = i.lookup[profile.ST]; !ok {
		panic("Missing ST during indexing")
	}
	index = &i.indices[offset]
	if index.Ready {
		return index
	}

	index.Genes = NewBitArray(2500)
	if i.alleleTokens.lastValue < 2500 {
		index.Alleles = NewBitArray(2500)
	} else {
		index.Alleles = NewBitArray(i.alleleTokens.lastValue)
	}
	var bit uint64
	for gene, allele := range profile.Matches {
		bit = i.alleleTokens.Get(AlleleKey{
			gene,
			allele,
		})
		index.Alleles.SetBit(bit)
		bit := i.geneTokens.Get(AlleleKey{
			gene,
			nil,
		})
		index.Genes.SetBit(bit)
	}
	index.Ready = true
	return index
}

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
		score.value = comparer.compare(score.stA, score.stB)
		score.status = COMPLETE
		nScores++
		if nScores%100000 == 0 {
			log.Printf("Worker %d has computed %d scores", workerID, nScores)
		}
	}
}

func indexProfiles(workerID int, profiles chan Profile, indexer *Indexer, wg *sync.WaitGroup) {
	nIndexed := 0
	defer func() {
		log.Printf("Worker %d has indexed %d profiles", workerID, nIndexed)
	}()
	defer wg.Done()
	for {
		p, more := <-profiles
		if !more {
			return
		}
		indexer.Index(p)
		nIndexed++
		if nIndexed%100 == 0 {
			log.Printf("Worker %d has indexed %d profiles", workerID, nIndexed)
		}
	}
}

type scoresResult struct {
	STs    []CgmlstSt
	Scores []int
}

func toIndex(profiles ProfileStore, scores scoresStore, errChan chan error) chan Profile {
	queued := make([]bool, len(scores.STs))
	indexChan := make(chan Profile, 50)

	go func() {
		idx := 0
		for i := 1; i < len(scores.STs); i++ {
			for j := 0; j < i; j++ {
				if scores.scores[idx].status == PENDING {
					if !queued[i] {
						if !profiles.seen[i] {
							errChan <- fmt.Errorf("expected profile for %s", scores.STs[i])
							return
						} else {
							indexChan <- profiles.profiles[i]
							queued[i] = true
						}
					}
					if !queued[j] {
						if !profiles.seen[j] {
							errChan <- fmt.Errorf("expected profile for %s", scores.STs[j])
							return
						} else {
							indexChan <- profiles.profiles[j]
							queued[j] = true
						}
					}
				}
				idx++
			}
		}
		close(indexChan)
	}()

	return indexChan
}

func scoreAll(scores scoresStore, profiles ProfileStore, progress chan ProgressEvent) (done chan bool, err chan error) {
	numWorkers := 10
	indexer := NewIndexer(scores.lookup)
	var indexWg, scoreWg sync.WaitGroup

	err = make(chan error)
	done = make(chan bool)

	_profilesChan := toIndex(profiles, scores, err)
	profilesChan := make(chan Profile)
	go func() {
		for profile := range _profilesChan {
			profilesChan <- profile
			progress <- ProgressEvent{PROFILE_INDEXED, 1}
		}
		close(profilesChan)
	}()

	_scoreTasks := make(chan int, 50)
	scoreTasks := make(chan int)
	go func() {
		for task := range _scoreTasks {
			scoreTasks <- task
			progress <- ProgressEvent{SCORE_UPDATED, 1}
		}
		close(scoreTasks)
	}()

	for i := 1; i <= numWorkers; i++ {
		indexWg.Add(1)
		go indexProfiles(i, profilesChan, indexer, &indexWg)
	}

	go func() {
		indexWg.Wait()
		for idx, score := range scores.scores {
			if score.status == PENDING {
				_scoreTasks <- idx
			}
		}
		close(_scoreTasks)
	}()

	for i := 1; i <= numWorkers; i++ {
		scoreWg.Add(1)
		go scoreProfiles(i, scoreTasks, &scores, Comparer{indexer.indices}, &scoreWg)
	}

	go func() {
		scoreWg.Wait()
		progress <- ProgressEvent{SCORING_COMPLETE, 0}
		done <- true
	}()

	return
}
