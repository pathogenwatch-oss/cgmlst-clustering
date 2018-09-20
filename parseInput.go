package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"
	"sync/atomic"

	"gitlab.com/cgps/bsonkit"
)

const (
	PENDING    int = 0
	COMPLETE   int = 1
	FROM_CACHE int = 2
)

type GenomeID = bsonkit.ObjectID
type CgmlstSt = string
type M = map[string]interface{}
type L = []interface{}

func parseCgmlstStSlice(d *bsonkit.Document) (output []CgmlstSt, err error) {
	d.Seek(0)
	output = make([]CgmlstSt, 0, 1000)
	var v CgmlstSt
	for d.Next() {
		if err = d.Value(&v); err != nil {
			return
		}
		output = append(output, v)
	}
	if d.Err != nil {
		err = d.Err
		return
	}
	return
}

func parseIntSlice(d *bsonkit.Document) (output []int, err error) {
	d.Seek(0)
	output = make([]int, 0, 1000)
	var v int32
	for d.Next() {
		if err = d.Value(&v); err != nil {
			return
		}
		output = append(output, int(v))
	}
	if d.Err != nil {
		err = d.Err
		return
	}
	return
}

type Cache struct {
	sync.RWMutex
	Pi        []int
	Lambda    []int
	Sts       []string
	Threshold int
	Edges     map[int][][2]int
	nEdges    int
}

func NewCache() Cache {
	return Cache{Edges: make(map[int][][2]int), nEdges: 0}
}

func (c *Cache) Update(cacheDoc *bsonkit.Document, maxThreshold int) (err error) {
	cacheDoc.Seek(0)
	piDoc := new(bsonkit.Document)
	lambdaDoc := new(bsonkit.Document)
	stsDoc := new(bsonkit.Document)
	edgesDoc := new(bsonkit.Document)

	for cacheDoc.Next() {
		switch string(cacheDoc.Key()) {
		case "pi":
			err = cacheDoc.Value(piDoc)
		case "lambda":
			err = cacheDoc.Value(lambdaDoc)
		case "STs":
			err = cacheDoc.Value(stsDoc)
		case "edges":
			err = cacheDoc.Value(edgesDoc)
		case "threshold":
			var v int32
			err = cacheDoc.Value(&v)
			if err != nil {
				return
			}
			if v > 0 {
				if c.Threshold == int(v) {
					// nop
				} else if c.Threshold > 0 {
					err = errors.New("Already got a different threshold set for cache")
				} else {
					c.Threshold = int(v)
				}
			}
		}
		if err != nil {
			return
		}
	}
	if cacheDoc.Err != nil {
		err = cacheDoc.Err
		return
	}

	err = func() error {
		c.Lock()
		defer c.Unlock()
		if piDoc.Size() > 1 {
			if len(c.Pi) > 0 {
				return errors.New("Already got a Pi")
			}
			if c.Pi, err = parseIntSlice(piDoc); err != nil {
				return err
			}
		}
		if lambdaDoc.Size() > 1 {
			if len(c.Lambda) > 0 {
				return errors.New("Already got a Lambda")
			}
			if c.Lambda, err = parseIntSlice(lambdaDoc); err != nil {
				return err
			}
		}
		if stsDoc.Size() > 1 {
			if len(c.Sts) > 0 {
				return errors.New("Already got a STs")
			}
			if c.Sts, err = parseCgmlstStSlice(stsDoc); err != nil {
				return err
			}
		}
		return nil
	}()
	if err != nil {
		return
	}

	if edgesDoc.Size() > 1 {
		err = c.AddEdges(edgesDoc, maxThreshold)
	}

	return
}

func (c *Cache) AddEdges(doc *bsonkit.Document, maxThreshold int) (err error) {
	doc.Seek(0)
	var (
		distance int
		a, b     int32
	)
	listOfPairs := new(bsonkit.Document)
	pairOfSts := new(bsonkit.Document)

	for doc.Next() {
		atDistance := make([][2]int, 0, 100)
		if distance, err = strconv.Atoi(string(doc.Key())); err != nil {
			break
		}

		c.RLock()
		if _, found := c.Edges[distance]; found {
			err = errors.New("Edges already set at this distance")
		}
		c.RUnlock()
		if err != nil {
			return err
		}

		if distance > maxThreshold {
			continue
		}
		if err = doc.Value(listOfPairs); err != nil {
			break
		}
		for listOfPairs.Next() {
			if err = listOfPairs.Value(pairOfSts); err != nil {
				break
			}

			if !pairOfSts.Next() {
				err = errors.New("Expected a pair of STs")
				break
			}
			if err = pairOfSts.Value(&a); err != nil {
				break
			}

			if !pairOfSts.Next() {
				err = errors.New("Expected a pair of STs")
				break
			}
			if err = pairOfSts.Value(&b); err != nil {
				break
			}

			if pairOfSts.Next() {
				err = errors.New("Expected a pair of STs")
				break
			}
			if pairOfSts.Err != nil {
				err = pairOfSts.Err
				break
			}
			atDistance = append(atDistance, [2]int{int(a), int(b)})
		}
		if err != nil {
			break
		}
		if listOfPairs.Err != nil {
			err = listOfPairs.Err
			break
		}
		c.Lock()
		c.Edges[distance] = atDistance
		c.nEdges++
		c.Unlock()
	}
	if err != nil {
		return
	}
	if doc.Err != nil {
		return doc.Err
	}
	return
}

func (c *Cache) Complete(maxThreshold int) error {
	c.RLock()
	defer c.RUnlock()
	threshold := c.Threshold
	if maxThreshold < c.Threshold {
		threshold = maxThreshold
	}
	if c.nEdges != threshold+1 {
		return errors.New("Not enough edges")
	}
	if c.Threshold == 0 {
		return errors.New("Threshold not set")
	}
	if len(c.Sts) == 0 {
		return errors.New("Sts not set")
	}
	if len(c.Pi) == 0 {
		return errors.New("Pi not set")
	}
	if len(c.Lambda) == 0 {
		return errors.New("Lambda not set")
	}

	for t := 0; t <= threshold; t++ {
		if _, found := c.Edges[t]; !found {
			return fmt.Errorf("Edges are missing at threshold of %d", t)
		}
	}
	return nil
}

func (c Cache) CountDistances(maxThreshold int) int {
	c.RLock()
	defer c.RUnlock()
	distances := 0
	threshold := c.Threshold
	if maxThreshold < c.Threshold {
		threshold = maxThreshold
	}
	for t := 0; t <= threshold; t++ {
		if atThreshold, found := c.Edges[t]; found {
			distances += len(atThreshold)
		}
	}
	return distances
}

type Profile struct {
	ST      CgmlstSt
	Matches M
}

type ProfileStore struct {
	lookup   map[CgmlstSt]int
	profiles []Profile
	seen     []bool
}

func NewProfileStore(STs []CgmlstSt) (profiles ProfileStore) {
	profiles.lookup = make(map[CgmlstSt]int)
	for i, st := range STs {
		profiles.lookup[st] = i
	}
	profiles.profiles = make([]Profile, len(STs))
	profiles.seen = make([]bool, len(STs))
	return
}

func (profiles *ProfileStore) Add(p Profile) (duplicate bool, err error) {
	idx, known := profiles.lookup[p.ST]
	if !known {
		return false, fmt.Errorf("unknown fileId %s", p.ST)
	}

	if profiles.seen[idx] {
		// This is a duplicate of something we've already parsed
		return true, nil
	}

	profiles.profiles[idx] = p
	profiles.seen[idx] = true
	return false, nil
}

func (profiles *ProfileStore) Get(ST CgmlstSt) (Profile, error) {
	idx, known := profiles.lookup[ST]
	if !known {
		return Profile{}, fmt.Errorf("unknown ST %s", ST)
	}
	if seen := profiles.seen[idx]; !seen {
		return Profile{}, fmt.Errorf("unknown ST %s", ST)
	}
	return profiles.profiles[idx], nil
}

func (profiles *ProfileStore) AddFromDoc(doc *bsonkit.Document) (bool, error) {
	p, err := parseProfile(doc)
	if err != nil {
		return false, err
	}

	if p.ST == "" {
		return false, errors.New("Profile doc had an invalid fileId")
	}

	return profiles.Add(p)
}

type scoreDetails struct {
	stA, stB      int
	value, status int
}

type scoresStore struct {
	lookup map[CgmlstSt]int
	scores []scoreDetails
	STs    []CgmlstSt
	todo   int32
}

func NewScores(STs []CgmlstSt) (s scoresStore) {
	s.STs = STs
	s.scores = make([]scoreDetails, len(STs)*(len(STs)-1)/2)
	s.lookup = make(map[CgmlstSt]int)
	for idx, st := range STs {
		s.lookup[st] = idx
	}
	// TODO: Do we need to do this initialisation.  PENDING is probably the default value
	idx := 0
	for a := 1; a < len(STs); a++ {
		for b := 0; b < a; b++ {
			s.scores[idx] = scoreDetails{a, b, 0, PENDING}
			idx++
		}
	}
	s.todo = int32(len(s.scores))
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

func (s *scoresStore) SetIdx(idx int, score int, status int) error {
	s.scores[idx].value = score
	s.scores[idx].status = status
	atomic.AddInt32(&s.todo, -1)
	return nil
}

func (s *scoresStore) Set(stA int, stB int, score int, status int) error {
	idx, err := s.getIndex(stA, stB)
	if err != nil {
		return err
	}
	return s.SetIdx(idx, score, status)
}

func (s scoresStore) Get(stA int, stB int) (scoreDetails, error) {
	idx, err := s.getIndex(stA, stB)
	if err != nil {
		return scoreDetails{}, err
	}
	return s.scores[idx], nil
}

func (s scoresStore) Distances() ([]int, error) {
	distances := make([]int, len(s.scores))

	for i := 0; i < len(distances); i++ {
		score := s.scores[i]
		if score.status == PENDING {
			return distances, errors.New("Haven't found scores for all pairs of STs")
		}
		distances[i] = score.value
	}

	return distances, nil
}

func (s scoresStore) Todo() int32 {
	return atomic.LoadInt32(&s.todo)
}

func (s *scoresStore) UpdateFromCache(c Cache, mapExistingToSts map[int]int, maxThreshold int) (err error) {
	var (
		distance                 int
		aInExisting, bInExisting int
		aInSts, bInSts           int
		found                    bool
	)

	for distance = range c.Edges {
		pairs := c.Edges[distance]
		for _, pair := range pairs {
			aInExisting = pair[0]
			bInExisting = pair[1]

			if aInSts, found = mapExistingToSts[aInExisting]; !found {
				continue
			}
			if bInSts, found = mapExistingToSts[bInExisting]; !found {
				continue
			}

			if err = s.Set(aInSts, bInSts, distance, FROM_CACHE); err != nil {
				return err
			}
		}
	}

	if c.Threshold < maxThreshold {
		// The cache is missing some edges because it was generated with a smaller
		// threshold.
		return
	}

	var maxExisting int
	for _, v := range mapExistingToSts {
		if v > maxExisting {
			maxExisting = v
		}
	}
	maxToSet := maxExisting * (maxExisting + 1) / 2
	for idx := 0; idx < maxToSet; idx++ {
		if s.scores[idx].status != FROM_CACHE {
			s.scores[idx].value = ALMOST_INF
			s.scores[idx].status = FROM_CACHE
			atomic.AddInt32(&s.todo, -1)
		}
	}
	return
}

func parseRequestDoc(doc *bsonkit.Document) (STs []CgmlstSt, maxThreshold int, err error) {
	stsDoc := new(bsonkit.Document)
	var foundSts bool

	doc.Seek(0)
	for doc.Next() {
		if string(doc.Key()) == "STs" {
			err = doc.Value(stsDoc)
			foundSts = true
		} else if string(doc.Key()) == "maxThreshold" {
			var v int32
			err = doc.Value(&v)
			maxThreshold = int(v)
		}
		if err != nil {
			return
		}
	}
	if doc.Err != nil {
		err = doc.Err
	} else if !foundSts {
		err = errors.New("Expected sts field in document")
	} else if maxThreshold == 0 {
		err = errors.New("Expected maxThreshold field in document")
	}
	if err != nil {
		return
	}

	STs, err = parseCgmlstStSlice(stsDoc)
	return
}

func parseMatch(matchDoc *bsonkit.Document) (gene string, id interface{}, err error) {
	for matchDoc.Next() {
		switch string(matchDoc.Key()) {
		case "gene":
			if err = matchDoc.Value(&gene); err != nil {
				err = errors.New("Bad value for gene")
				return
			}
		case "id":
			if err = matchDoc.Value(&id); err != nil {
				err = errors.New("Bad value for allele id")
				return
			}
		}
	}
	if matchDoc.Err != nil {
		err = matchDoc.Err
	}
	return
}

func parseMatches(matchesDoc *bsonkit.Document, p *Profile) error {
	p.Matches = make(M)
	match := new(bsonkit.Document)

	for matchesDoc.Next() {
		if err := matchesDoc.Value(match); err != nil {
			return errors.New("Couldn't get match")
		}
		gene, id, err := parseMatch(match)
		if err != nil {
			return err
		}
		p.Matches[gene] = id
	}
	if matchesDoc.Err != nil {
		return matchesDoc.Err
	}
	return nil
}

func parseCgMlst(cgmlstDoc *bsonkit.Document, p *Profile) (err error) {
	matches := new(bsonkit.Document)
	for cgmlstDoc.Next() {
		switch string(cgmlstDoc.Key()) {
		case "st":
			if err = cgmlstDoc.Value(&p.ST); err != nil {
				return errors.New("Bad value for st")
			}
		case "matches":
			if err = cgmlstDoc.Value(matches); err != nil {
				return errors.New("Bad value for matches")
			}
			if err = parseMatches(matches, p); err != nil {
				return errors.New("Bad value for matches")
			}
		}
		if err != nil {
			return
		}
	}
	if cgmlstDoc.Err != nil {
		return cgmlstDoc.Err
	}
	if p.ST == "" {
		return errors.New("st not found")
	}
	if len(p.Matches) == 0 {
		return errors.New("No matches parsed")
	}
	return
}

func parseProfile(doc *bsonkit.Document) (profile Profile, err error) {
	cgmlstDoc := new(bsonkit.Document)
	doc.Seek(0)
	for doc.Next() {
		if string(doc.Key()) == "results" {
			if err = doc.Value(cgmlstDoc); err != nil {
				return profile, errors.New("Bad value for analysis")
			}
			err = parseCgMlst(cgmlstDoc, &profile)
			return profile, err
		}
	}
	if doc.Err != nil {
		return profile, doc.Err
	}
	return profile, errors.New("Could not find cgmlst in analysis")
}

type GenomeSTPair struct {
	ID bsonkit.ObjectID
	ST CgmlstSt
}

func sortSts(existing []CgmlstSt, requested []CgmlstSt) (isSubset bool, output []CgmlstSt, mapExistingToOutput map[int]int) {
	// We have a list of STs in the 'existing' cache and a list of 'requested' STs.
	// If 'existing' is a subset of 'requested' we can reuse the cache, otherwise we can't

	// We need a deduplicated list of all of the STs.  To reuse the cache, the 'existing' STs
	// must come first and their order must be preserved.

	// Then, to use the existing edges, we need a map between their position in 'existing' and
	// their position in the 'output'.  Normally that'll be 1->1, 2->2 except when one of
	// them is deleted and then it goes 1->1, 3->2, 4->3 etc.
	if len(existing) == 0 {
		return false, requested, make(map[int]int)
	}
	isSubset = true // We're optimistic
	output = make([]CgmlstSt, 0, len(existing)+len(requested))
	// Seen is a bit overloaded
	// If it's false, we know its one of the requested STs but not yet in the output
	// If it's true, it's one of the requested STs and it's already in the output
	// If it's not set, it isn't one of the requested STs.
	seen := make(map[CgmlstSt]bool)
	for _, st := range requested {
		seen[st] = false
	}
	mapExistingToOutput = make(map[int]int)
	for i, st := range existing {
		if duplicate, found := seen[st]; found && !duplicate {
			output = append(output, st)
			mapExistingToOutput[i] = len(output) - 1
			seen[st] = true
		} else {
			isSubset = false
		}
	}
	for _, st := range requested {
		if alreadyInOutput := seen[st]; !alreadyInOutput {
			output = append(output, st)
		}
		seen[st] = true // true means it's in the requested STs
	}
	return
}

func estimateCacheReusability(cacheSTs []CgmlstSt, requestedSts []CgmlstSt) int {
	count := 0
	inReq := map[CgmlstSt]bool{}
	for _, st := range requestedSts {
		inReq[st] = true
	}
	for _, st := range cacheSTs {
		if inReq[st] {
			count++
		}
	}
	return count
}

func parse(r io.Reader, progress chan ProgressEvent) (profiles ProfileStore, scores scoresStore, maxThreshold int, existingClusters Clusters, canReuseCache bool, err error) {
	err = nil
	errChan := make(chan error)
	numWorkers := 5
	var workerWg sync.WaitGroup

	docIter := bsonkit.GetDocuments(r)
	docChan := make(chan *bsonkit.Document, 50)
	go func() {
		for docIter.Next() {
			docChan <- docIter.Doc
		}
		if docIter.Err != nil {
			errChan <- docIter.Err
		}
		close(docChan)
	}()

	var firstDoc *bsonkit.Document
	select {
	case err = <-errChan:
		if err != nil {
			return
		}
	case d := <-docChan:
		firstDoc = d
	}

	var requestedSts []CgmlstSt
	for firstDoc.Next() {
		if string(firstDoc.Key()) == "STs" {
			requestedSts, maxThreshold, err = parseRequestDoc(firstDoc)
			if err != nil {
				return
			}
			break
		}
	}
	if firstDoc.Err != nil {
		err = firstDoc.Err
		return
	}

	if len(requestedSts) == 0 {
		err = errors.New("No STs found in first doc")
		return
	}

	log.Printf("Found %d STs\n", len(requestedSts))
	progress <- ProgressEvent{PROFILES_EXPECTED, len(requestedSts)}

	cache := NewCache()
	profiles = NewProfileStore(requestedSts)

	worker := func(workerID int) {
		nDocs := 0
		nProfiles := 0
		defer workerWg.Done()
		defer func() {
			log.Printf("Worker %d finished parsing %d profile docs and %d docs\n", workerID, nProfiles, nDocs)
		}()

		log.Printf("Worker %d started\n", workerID)

		for doc := range docChan {
			for doc.Next() {
				switch string(doc.Key()) {
				case "pi":
					fallthrough
				case "lambda":
					fallthrough
				case "STs":
					fallthrough
				case "edges":
					fallthrough
				case "threshold":
					err = cache.Update(doc, maxThreshold)
					if err != nil {
						log.Println(err)
					}
					if cache.Complete(maxThreshold) == nil {
						overlap := estimateCacheReusability(cache.Sts, requestedSts)
						if cache.Threshold >= maxThreshold {
							progress <- ProgressEvent{CACHED_SCORES_EXPECTED, overlap * (overlap - 1) / 2}
						} else {
							progress <- ProgressEvent{CACHED_SCORES_EXPECTED, cache.CountDistances(maxThreshold)}
						}
					}
				case "results":
					if duplicate, err := profiles.AddFromDoc(doc); err == nil && !duplicate {
						nProfiles++
						progress <- ProgressEvent{PROFILE_PARSED, 1}
					} else if err != nil {
						log.Println(err)
					}
					break
				}
			}
			nDocs++
			if nDocs%100 == 0 {
				log.Printf("Worker %d parsed %d profile docs and %d docs\n", workerID, nProfiles, nDocs)
			}
		}
	}

	for workerID := 0; workerID < numWorkers; workerID++ {
		workerWg.Add(1)
		go worker(workerID)
	}

	done := make(chan bool)
	go func() {
		workerWg.Wait()
		done <- true
		return
	}()

	select {
	case err = <-errChan:
		if err != nil {
			return
		}
	case <-done:
		log.Println("Workers have all finished")
	}

	cacheError := cache.Complete(maxThreshold)
	existingClusters = Clusters{[]int{}, []int{}, 0}
	STs := requestedSts
	if cacheError != nil {
		log.Println(cacheError)
		scores = NewScores(STs)
	} else {
		var mapExistingToSts map[int]int
		canReuseCache, STs, mapExistingToSts = sortSts(cache.Sts, requestedSts)
		scores = NewScores(STs)

		if canReuseCache {
			existingClusters = Clusters{cache.Pi, cache.Lambda, len(cache.Pi)}
		}
		// We should add the edges from the edge doc
		if err := scores.UpdateFromCache(cache, mapExistingToSts, maxThreshold); err != nil {
			errChan <- err
		}
	}

	return
}
