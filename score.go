package main

import (
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
)

import _ "go.uber.org/automaxprocs"

type Comparer struct {
	profilesMap      ProfilesMap
	minMatchingGenes int
}

func (c *Comparer) compare(stA int, stB int) int {
	profileA := c.profilesMap.indices[stA]
	profileB := c.profilesMap.indices[stB]
	geneCount := CompareBits(profileA.Genes, profileB.Genes)
	if geneCount < c.minMatchingGenes {
		return ALMOST_INF
	}
	alleleCount := int(profileA.Alleles.AndCardinality(profileB.Alleles))
	return geneCount - alleleCount
}

func scoreProfiles(workerID int, jobs chan [3]int, scores *ScoresStore, comparer *Comparer, wg *sync.WaitGroup) {
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

type ScoresStore struct {
	STs           []CgmlstSt
	scores        []int
	todo          int32 // remaining scores to compute
	canReuseCache bool  // can reuse the cached clustering
	cacheSize     int
}

func (s *ScoresStore) Done() int {
	return len(s.scores) - int(s.Todo())
}

// Orders the STs by cache (retaining the order in the cache) first and then other STs in the request.
// The location of the ST links to `cacheToScoresMap`.
func sortSts(requestSts []CgmlstSt, cache *Cache, profiles *ProfilesMap) (canReuseCache bool, STs []CgmlstSt, cacheToScoresMap []int, cacheSize int) {
	if len(cache.Sts) == 0 {
		canReuseCache = false
	} else {
		canReuseCache = true
	}

	STs = make([]CgmlstSt, len(requestSts))
	cacheToScoresMap = make([]int, len(cache.Sts)) // Allows to populate the cache scores later.

	seenSTs := make(map[CgmlstSt]int)

	var scoresIdx int
	for cacheIdx, st := range cache.Sts {
		if previousIdx, seen := seenSTs[st]; seen {
			// duplicate ST in cache is not good
			canReuseCache = false
			cacheToScoresMap[cacheIdx] = previousIdx
		} else if _, needed := profiles.lookup[st]; !needed {
			// The cache contains STs we don't need
			canReuseCache = false
			cacheToScoresMap[cacheIdx] = -1
			fmt.Println("Skipping ST in cache: ", st)
		} else {
			seenSTs[st] = scoresIdx
			STs[scoresIdx] = st
			cacheToScoresMap[cacheIdx] = scoresIdx // point position in cache ST array to order in STs array
			scoresIdx++
		}
	}

	cacheSize = scoresIdx
	//fmt.Println(scoresIdx)

	for _, st := range requestSts {
		if _, seen := seenSTs[st]; !seen {
			seenSTs[st] = scoresIdx
			STs[scoresIdx] = st
			scoresIdx++
		}
	}

	STs = STs[:scoresIdx]
	return
}

func NewScores(request Request, cache *Cache, profiles *ProfilesMap) (s ScoresStore, err error) {

	//fmt.Println("STs in cache: ", len(cache.Sts))
	var cacheToScoresMap []int
	s.canReuseCache, s.STs, cacheToScoresMap, s.cacheSize = sortSts(request.STs, cache, profiles)
	nSTs := len(s.STs)
	s.scores = make([]int, nSTs*(nSTs-1)/2)

	scoresToProfileMap := make([]int, nSTs)

	var (
		scoresIndex int
		stA         int
		found       bool
	)
	for scoresIdx, st := range s.STs {
		if stA, found = profiles.lookup[st]; !found {
			err = fmt.Errorf("could not find ST '%s' in profilesMap", st)
			return
		}
		scoresToProfileMap[scoresIdx] = stA
		for range scoresToProfileMap[:scoresIdx] {
			s.scores[scoresIndex] = -1
			scoresIndex++
		}
	}

	if err = s.UpdateFromCache(request.Threshold, cache, cacheToScoresMap); err != nil {
		return
	}

	return
}

func GetIndex(stA int, stB int) (int, error) {
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

func (s ScoresStore) getIndex(stA int, stB int) (int, error) {
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

func (s *ScoresStore) SetIdx(idx int, score int) error {
	s.scores[idx] = score
	atomic.AddInt32(&s.todo, -1)
	return nil
}

func (s *ScoresStore) Set(stA int, stB int, score int) error {
	idx, err := s.getIndex(stA, stB)
	if err != nil {
		return err
	}
	return s.SetIdx(idx, score)
}

func (s ScoresStore) Distances() (*[]int, error) {
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

func (s ScoresStore) Todo() int32 {
	return atomic.LoadInt32(&s.todo)
}

func (s *ScoresStore) UpdateFromCache(threshold int, c *Cache, cacheToScoresMap []int) (err error) {
	var (
		distance             int
		aInCache, bInCache   int // These are indexes into the cache
		aInScores, bInScores int // These are indexes into the index
		pairs                [][2]int
		nStsReusedFromCache  int
	)

	if c.Threshold >= threshold {
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
					err = s.Set(aInScores, bInScores, ALMOST_INF)
					if err != nil {
						return err
					}
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

	if c.Threshold >= threshold {
		nCached := (nStsReusedFromCache * (nStsReusedFromCache - 1)) / 2
		s.todo = int32(len(s.scores) - nCached)
	}

	return
}

// Sets up the ST links for those in the cache and sets the score index to the correct position
func indexCache(STs *[]CgmlstSt, stIndexMap *map[CgmlstSt]int, cacheSize int) (int, *[]int) {
	profileIndex := make([]int, len(*STs))
	var i int
	var st string
	if cacheSize == 0 {
		return 0, &profileIndex
	}
	for i, st = range (*STs)[:cacheSize] {
		profileIndex[i] = (*stIndexMap)[st]
	}
	scoreIndex, err := GetIndex(i-1, i)
	if err != nil {
		panic(err)
	}
	scoreIndex++
	return scoreIndex, &profileIndex
}

func (s *ScoresStore) RunScoring(profileMap ProfilesMap, progress chan ProgressEvent) (done chan bool, err chan error) {
	numWorkers := runtime.NumCPU() + 1
	var scoreWg sync.WaitGroup

	err = make(chan error)
	done = make(chan bool)

	_scoreTasks := make(chan [3]int, 5000)
	scoreTasks := make(chan [3]int, 5000)
	go func() {
		for task := range _scoreTasks {
			scoreTasks <- task
			progress <- ProgressEvent{SCORE_CALCULATED, 1}
		}
		close(scoreTasks)
	}()

	go func() {
		scoreIndex, profileIndex := indexCache(&s.STs, &profileMap.lookup, s.cacheSize)
		profileIndexD := *profileIndex
		stCount := len(s.STs)
		for i := s.cacheSize; i < stCount; i++ {
			stAIndex := profileMap.lookup[s.STs[i]]
			profileIndexD[i] = stAIndex
			for j := 0; j < i; j++ {
				_scoreTasks <- [3]int{stAIndex, profileIndexD[j], scoreIndex}
				scoreIndex++
			}
		}
		close(_scoreTasks)
	}()

	minMatchingGenes := int(profileMap.schemeSize * 8 / 10)
	for i := 1; i <= numWorkers; i++ {
		scoreWg.Add(1)
		go scoreProfiles(i, scoreTasks, s, &Comparer{profileMap, minMatchingGenes}, &scoreWg)
	}

	go func() {
		scoreWg.Wait()
		done <- true
	}()

	return
}
