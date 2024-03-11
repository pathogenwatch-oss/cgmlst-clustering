package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type Comparer struct {
	profiles         []Index
	minMatchingGenes int
}

func (c *Comparer) compare(stA int, stB int) int {
	profileA := c.profiles[stA]
	profileB := c.profiles[stB]
	geneCount := CompareBits(profileA.Genes, profileB.Genes)
	if geneCount < c.minMatchingGenes {
		return ALMOST_INF
	}
	alleleCount := int(profileA.Alleles.AndCardinality(profileB.Alleles))
	return geneCount - alleleCount
}

func scoreProfiles(workerID int, jobs chan [3]int, scores *scoresStore, comparer Comparer, wg *sync.WaitGroup) {
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
		compare := comparer.compare(j[0], j[1])
		err := scores.SetIdx(j[2], compare)
		if err != nil {
			panic(err)
		}
		nScores++
		if nScores%1000000 == 0 {
			log.Printf("Worker %d has computed %d scores", workerID, nScores)
		}
	}
}

//type scoreDetails struct {
//	stA, stB      int
//	value, status int
//}

type scoresStore struct {
	STs           []CgmlstSt
	scores        []int
	statuses      []int32
	todo          int32 // remaining scores to compute
	canReuseCache bool  // can reuse the cached clustering
}

func (s *scoresStore) Done() int {
	return len(s.scores) - int(s.Todo())
}

// Orders the STs by cache (retaining the order in the cache) first and then other STs in the request.
// The location of the ST links to `cacheToScoresMap`.
func sortSts(request Request, cache *Cache, index *IndexMap) (canReuseCache bool, STs []CgmlstSt, cacheToScoresMap []int) {
	if len(cache.Sts) == 0 {
		canReuseCache = false
	} else {
		canReuseCache = true
	}

	STs = make([]CgmlstSt, len(request.STs))
	cacheToScoresMap = make([]int, len(cache.Sts))

	seenSTs := make(map[CgmlstSt]int)

	var scoresIdx int
	for cacheIdx, st := range cache.Sts {
		if previousIdx, seen := seenSTs[st]; seen {
			// duplicate ST in cache is not good
			canReuseCache = false
			cacheToScoresMap[cacheIdx] = previousIdx
		} else if _, needed := index.lookup[st]; !needed {
			// The cache contains STs we don't need
			canReuseCache = false
			cacheToScoresMap[cacheIdx] = -1
		} else {
			seenSTs[st] = scoresIdx
			STs[scoresIdx] = st
			cacheToScoresMap[cacheIdx] = scoresIdx
			scoresIdx++
		}
	}

	for _, st := range request.STs {
		if _, seen := seenSTs[st]; !seen {
			seenSTs[st] = scoresIdx
			STs[scoresIdx] = st
			scoresIdx++
		}
	}

	STs = STs[:scoresIdx]
	return
}

func NewScores(request Request, cache *Cache, index *IndexMap) (s scoresStore, err error) {
	var cacheToScoresMap []int
	s.canReuseCache, s.STs, cacheToScoresMap = sortSts(request, cache, index)
	nSTs := len(s.STs)
	s.scores = make([]int, nSTs*(nSTs-1)/2)

	scoresToIndexMap := make([]int, nSTs)

	var (
		scoreDetailsIndex int
		stA               int
		found             bool
	)
	for scoresIdx, st := range s.STs {
		if stA, found = index.lookup[st]; !found {
			err = fmt.Errorf("could not find ST '%s' in index", st)
			return
		}
		scoresToIndexMap[scoresIdx] = stA
		for _, _ = range scoresToIndexMap[:scoresIdx] {
			s.scores[scoreDetailsIndex] = -1
			scoreDetailsIndex++
		}
	}

	if err = s.UpdateFromCache(request, cache, cacheToScoresMap); err != nil {
		return
	}

	return
}

func (s scoresStore) getIndex(stA int, stB int) (int, error) {
	minIdx, maxIdx := stA, stB
	if stA == stB {
		return 0, fmt.Errorf("STs shouldn't both be %d", stA)
	} else if stA > stB {
		minIdx = stB
		maxIdx = stA
	}
	scoreIdx := ((maxIdx * (maxIdx - 1)) / 2) + minIdx
	return scoreIdx, nil
}

func (s *scoresStore) SetIdx(idx int, score int) error {
	s.scores[idx] = score
	atomic.AddInt32(&s.todo, -1)
	return nil
}

func (s *scoresStore) Set(stA int, stB int, score int) error {
	idx, err := s.getIndex(stA, stB)
	if err != nil {
		return err
	}
	return s.SetIdx(idx, score)
}

func (s scoresStore) Distances() (*[]int, error) {
	return &s.scores, nil

	//distances := make([]int, len(s.scores))
	//
	//for i := 0; i < len(distances); i++ {
	//	score := s.scores[i]
	//	if score.status == PENDING {
	//		return distances, errors.New("haven't found scores for all pairs of STs")
	//	}
	//	distances[i] = score.value
	//}
	//
	//return distances, nil
}

func (s scoresStore) Todo() int32 {
	return atomic.LoadInt32(&s.todo)
}

func (s *scoresStore) UpdateFromCache(request Request, c *Cache, cacheToScoresMap []int) (err error) {
	var (
		distance             int
		aInCache, bInCache   int // These are indexes into the cache
		aInScores, bInScores int // These are indexes into the index
		pairs                [][2]int
		nStsReusedFromCache  int
	)

	if c.Threshold >= request.Threshold {
		for aInCache, aInScores := range cacheToScoresMap {
			if aInScores < 0 {
				continue
			} else if aInScores > nStsReusedFromCache {
				nStsReusedFromCache = aInScores
			}
			for _, bInScores := range cacheToScoresMap[:aInCache] {
				if bInScores >= 0 {
					if aInScores == bInScores {
						continue
					}
					// TODO: we can make this a little faster using s.SetIdx
					// and we know that the cached scores are always
					// in a continuous block at the beginning of the
					// output.
					s.Set(aInScores, bInScores, ALMOST_INF)
				}
			}
		}
	}

	nStsReusedFromCache++ // This was the index of the last cached ST in the index

	s.todo = int32(len(s.scores))

	for distance, pairs = range c.Edges {
		for _, pair := range pairs {
			aInCache = pair[0]
			bInCache = pair[1]

			aInScores = cacheToScoresMap[aInCache]
			bInScores = cacheToScoresMap[bInCache]

			if aInScores < 0 || bInScores < 0 {
				// These values are -1 if we don't want this cached value in the results
				continue
			}

			if err = s.Set(aInScores, bInScores, distance); err != nil {
				return err
			}
		}
	}

	if c.Threshold >= request.Threshold {
		nCached := (nStsReusedFromCache * (nStsReusedFromCache - 1)) / 2
		s.todo = int32(len(s.scores) - nCached)
	}

	return
}

func (s *scoresStore) Complete(indexer *IndexMap, progress chan ProgressEvent) (done chan bool, err chan error) {
	numWorkers := 1
	var scoreWg sync.WaitGroup

	err = make(chan error)
	done = make(chan bool)

	_scoreTasks := make(chan [3]int, 50)
	scoreTasks := make(chan [3]int)
	go func() {
		for task := range _scoreTasks {
			scoreTasks <- task
			progress <- ProgressEvent{SCORE_CALCULATED, 1}
		}
		close(scoreTasks)
	}()

	go func() {
		scoreIndex := 0
		for i := 1; i < len(s.STs); i++ {
			for j := 0; j < i; j++ {
				if s.scores[scoreIndex] == -1 {
					_scoreTasks <- [3]int{j, i, scoreIndex}
				}
				scoreIndex++
			}
		}
		//for idx, score := range s.scores {
		//	if score.status == PENDING {
		//		_scoreTasks <- idx
		//	}
		//}
		close(_scoreTasks)
	}()

	minMatchingGenes := int(indexer.schemeSize * 8 / 10)
	for i := 1; i <= numWorkers; i++ {
		scoreWg.Add(1)
		go scoreProfiles(i, scoreTasks, s, Comparer{indexer.indices, minMatchingGenes}, &scoreWg)
	}

	go func() {
		scoreWg.Wait()
		done <- true
	}()

	return
}
