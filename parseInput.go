package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"

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

type Request struct {
	STs       []CgmlstSt
	Threshold int
}

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

func NewCache() *Cache {
	c := Cache{Edges: make(map[int][][2]int), nEdges: 0}
	return &c
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
	ST         CgmlstSt
	Matches    M
	schemeSize int32
}

func parseRequestDoc(doc *bsonkit.Document) (request Request, err error) {
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
			request.Threshold = int(v)
		}
		if err != nil {
			return
		}
	}
	if doc.Err != nil {
		err = doc.Err
	} else if !foundSts {
		err = errors.New("Expected sts field in document")
	} else if request.Threshold == 0 {
		err = errors.New("Expected maxThreshold field in document")
	}
	if err != nil {
		return
	}

	request.STs, err = parseCgmlstStSlice(stsDoc)

	if len(request.STs) == 0 {
		err = errors.New("No STs found in first doc")
		return
	}

	return
}

func parseMatch(matchDoc *bsonkit.Document) (gene string, id interface{}, err error) {
	for matchDoc.Next() {
		switch string(matchDoc.Key()) {
		case "gene":
			var g interface{}
			if g, err = matchDoc.RawValue(); err != nil {
				err = errors.New("Bad value for gene")
				return
			}
			gene = g.(string)
		case "id":
			if id, err = matchDoc.RawValue(); err != nil {
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
	var rawValue interface{}
	for cgmlstDoc.Next() {
		switch string(cgmlstDoc.Key()) {
		case "st":
			if rawValue, err = cgmlstDoc.RawValue(); err != nil {
				return errors.New("Bad value for st")
			}
			p.ST = rawValue.(string)
		case "matches":
			if err = cgmlstDoc.Value(matches); err != nil {
				return errors.New("Bad value for matches")
			}
			if err = parseMatches(matches, p); err != nil {
				return errors.New("Bad value for matches")
			}
		case "schemeSize":
			if rawValue, err = cgmlstDoc.RawValue(); err != nil {
				return errors.New("Bad value for schemeSize")
			}
			p.schemeSize = rawValue.(int32)
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

func parseProfile(doc *bsonkit.Document, profile *Profile) (err error) {
	cgmlstDoc := new(bsonkit.Document)
	doc.Seek(0)
	for doc.Next() {
		if string(doc.Key()) == "results" {
			if err = doc.Value(cgmlstDoc); err != nil {
				return errors.New("Bad value for analysis")
			}
			err = parseCgMlst(cgmlstDoc, profile)
			return err
		}
	}
	if doc.Err != nil {
		return doc.Err
	}
	return errors.New("Could not find cgmlst in analysis")
}

type GenomeSTPair struct {
	ID bsonkit.ObjectID
	ST CgmlstSt
}

func parseAndIndex(doc *bsonkit.Document, index *Indexer) (bool, error) {
	p := Profile{}
	err := parseProfile(doc, &p)
	if err != nil {
		return false, err
	}

	if p.ST == "" {
		return false, errors.New("Profile doc had an invalid fileId")
	}

	return index.Index(&p)
}

func parse(r io.Reader, progress chan ProgressEvent) (request Request, cache *Cache, index *Indexer, err error) {
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

	if request, err = parseRequestDoc(firstDoc); err != nil {
		return
	}

	log.Printf("Found %d STs\n", len(request.STs))
	progress <- ProgressEvent{PROFILES_EXPECTED, len(request.STs)}

	cache = NewCache()
	index = NewIndexer(request.STs)

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
					err = cache.Update(doc, request.Threshold)
					if err != nil {
						log.Println(err)
					}
				case "results":
					if duplicate, err := parseAndIndex(doc, index); err == nil && !duplicate {
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

	return
}
