package main

import (
	"fmt"
	"log"
	"sync"
)

type Index struct {
	Genes   *BitArray
	Alleles *BitArray
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
	lookup       map[string]Index
}

func NewIndexer() *Indexer {
	i := Indexer{
		geneTokens:   NewTokeniser(),
		alleleTokens: NewTokeniser(),
		lookup:       make(map[string]Index),
	}
	return &i
}

func (i *Indexer) Index(profile Profile) Index {
	defer i.Unlock()
	i.Lock()
	if index, ok := i.lookup[profile.FileID]; ok {
		return index
	}
	genesBa := NewBitArray(2500)
	var allelesBa *BitArray
	if i.alleleTokens.lastValue < 2500 {
		allelesBa = NewBitArray(2500)
	} else {
		allelesBa = NewBitArray(i.alleleTokens.lastValue)
	}
	var bit uint64
	for gene, allele := range profile.Matches {
		bit = i.alleleTokens.Get(AlleleKey{
			gene,
			allele,
		})
		allelesBa.SetBit(bit)
		bit := i.geneTokens.Get(AlleleKey{
			gene,
			nil,
		})
		genesBa.SetBit(bit)
	}
	index := Index{
		Genes:   genesBa,
		Alleles: allelesBa,
	}
	i.lookup[profile.FileID] = index
	return index
}

type Comparer struct {
	lookup map[string]Index
}

func (c *Comparer) compare(fileIDA string, fileIDB string) int {
	indexA, okA := c.lookup[fileIDA]
	indexB, okB := c.lookup[fileIDB]
	if !okA || !okB {
		panic("Missing index")
	}
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
		score.value = comparer.compare(score.fileA, score.fileB)
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
	FileIDs []string
	Scores  []int
}

func toIndex(profiles ProfileStore, scores scoresStore, errChan chan error) chan Profile {
	queued := make([]bool, len(scores.fileIDs))
	indexChan := make(chan Profile, 50)

	go func() {
		idx := 0
		for i := 1; i < len(scores.fileIDs); i++ {
			for j := 0; j < i; j++ {
				if scores.scores[idx].status == PENDING {
					if !queued[i] {
						if !profiles.seen[i] {
							errChan <- fmt.Errorf("expected profile for %s", scores.fileIDs[i])
							return
						} else {
							indexChan <- profiles.profiles[i]
							queued[i] = true
						}
					}
					if !queued[j] {
						if !profiles.seen[j] {
							errChan <- fmt.Errorf("expected profile for %s", scores.fileIDs[j])
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

type CacheOutput struct {
	FileID            string         `json:"fileId"`
	AlleleDifferences map[string]int `json:"alleleDifferences"`
}

func buildCacheOutputs(scores scoresStore) chan CacheOutput {
	outputChan := make(chan CacheOutput, 20)

	rangeStart := func(idx int) int {
		return (idx * (idx - 1)) / 2
	}

	go func() {
		defer close(outputChan)
		for i, fileA := range scores.fileIDs {
			if i == 0 {
				continue
			}
			start := rangeStart(i)
			end := rangeStart(i + 1)
			output := CacheOutput{fileA, make(map[string]int)}
			for _, score := range scores.scores[start:end] {
				output.AlleleDifferences[score.fileB] = score.value
			}
			outputChan <- output
		}
	}()

	return outputChan
}

func scoreAll(scores scoresStore, profiles ProfileStore) error {
	numWorkers := 10
	indexer := NewIndexer()
	var wg sync.WaitGroup

	errChan := make(chan error)
	profilesChan := toIndex(profiles, scores, errChan)

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go indexProfiles(i, profilesChan, indexer, &wg)
	}

	wg.Wait()

	jobs := make(chan int, 50)
	go func() {
		for idx, score := range scores.scores {
			if score.status == PENDING {
				jobs <- idx
			}
		}
		close(jobs)
	}()

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go scoreProfiles(i, jobs, &scores, Comparer{lookup: indexer.lookup}, &wg)
	}

	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case err := <-errChan:
		if err != nil {
			return err
		}
	case <-done:
	}
	return nil
}
