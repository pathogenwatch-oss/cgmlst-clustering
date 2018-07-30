package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
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
	lookup       map[CgmlstSt]Index
}

func NewIndexer() *Indexer {
	i := Indexer{
		geneTokens:   NewTokeniser(),
		alleleTokens: NewTokeniser(),
		lookup:       make(map[CgmlstSt]Index),
	}
	return &i
}

func (i *Indexer) Index(profile Profile) Index {
	defer i.Unlock()
	i.Lock()
	if index, ok := i.lookup[profile.ST]; ok {
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
	i.lookup[profile.ST] = index
	return index
}

type Comparer struct {
	lookup map[CgmlstSt]Index
}

func (c *Comparer) compare(stA string, stB string) int {
	indexA, okA := c.lookup[stA]
	indexB, okB := c.lookup[stB]
	if !okA || !okB {
		panic("Missing index")
	}
	geneCount := CompareBits(indexA.Genes, indexB.Genes)
	alleleCount := CompareBits(indexA.Alleles, indexB.Alleles)
	return geneCount - alleleCount
}

func scoreProfiles(workerID int, jobs chan int, scores *scoresStore, comparer Comparer, wg *sync.WaitGroup, sc *scoreCacher) {
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
		sc.Done(score.stA)
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

type CacheOutput struct {
	ST                CgmlstSt         `json:"st"`
	AlleleDifferences map[CgmlstSt]int `json:"alleleDifferences"`
}

type scoreCacher struct {
	stScoresCompleted []int32
	scores            *scoresStore
	output            chan CacheOutput
	finished          bool
}

func MakeScoreCacher(scores *scoresStore, output chan CacheOutput) scoreCacher {
	nSts := len(scores.STs)
	stScoresCompleted := make([]int32, nSts)
	return scoreCacher{stScoresCompleted, scores, output, false}
}

func (s *scoreCacher) Done(st CgmlstSt) {
	idx, ok := s.scores.lookup[st]
	if !ok {
		return
	}
	count := atomic.AddInt32(&s.stScoresCompleted[idx], 1)
	if count == int32(idx) {
		s.cache(idx)
	}
}

func (s *scoreCacher) cache(idx int) {
	if idx == 0 {
		return
	}
	stA := s.scores.STs[idx]
	start := (idx * (idx - 1)) / 2
	end := ((idx + 1) * idx) / 2
	output := CacheOutput{stA, make(map[string]int)}
	for _, score := range s.scores.scores[start:end] {
		if score.status == COMPLETE {
			output.AlleleDifferences[score.stB] = score.value
		}
	}
	if len(output.AlleleDifferences) > 0 {
		s.output <- output
	}
}

func (s *scoreCacher) Close() {
	if !s.finished {
		close(s.output)
		s.finished = true
	}
}

func (s *scoreCacher) Remaining() int {
	var count int
	for idx, n := range s.stScoresCompleted {
		count += (idx - int(n))
	}
	return count
}

func estimateScoreTasks(scores scoresStore) int {
	var tasks, idx int
	toIndex := make(map[int]bool)
	for i := 1; i < len(scores.STs); i++ {
		for j := 0; j < i; j++ {
			if scores.scores[idx].status == PENDING {
				tasks++ // A score task
				toIndex[i] = true
				toIndex[j] = true
			}
			idx++
		}
	}
	tasks += len(toIndex) // Add the profile indexing tasks
	return tasks
}

func scoreAll(scores scoresStore, profiles ProfileStore, progress chan ProgressEvent, sc scoreCacher) (done chan bool, err chan error) {
	numWorkers := 10
	indexer := NewIndexer()
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
			} else {
				progress <- ProgressEvent{SCORE_UPDATED, 1}
				sc.Done(score.stA)
			}
		}
		close(_scoreTasks)
	}()

	for i := 1; i <= numWorkers; i++ {
		scoreWg.Add(1)
		go scoreProfiles(i, scoreTasks, &scores, Comparer{lookup: indexer.lookup}, &scoreWg, &sc)
	}

	go func() {
		scoreWg.Wait()
		sc.Close()
		progress <- ProgressEvent{SCORING_COMPLETE, 0}
		done <- true
	}()

	return
}
