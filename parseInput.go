package main

import (
	"errors"
	"fmt"
	"io"
	"log"
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

func updateScores(scores scoresStore, s *bsonkit.Document) error {
	var (
		stA, stB CgmlstSt
		score    int32
	)

	scoresDoc := new(bsonkit.Document)

	s.Seek(0)
	for s.Next() {
		switch string(s.Key()) {
		case "st":
			if err := s.Value(&stA); err != nil {
				return errors.New("Couldn't parse st")
			}
		case "alleleDifferences":
			if err := s.Value(scoresDoc); err != nil {
				return errors.New("Couldn't parse alleleDifferences")
			}
		}
	}
	if s.Err != nil {
		return s.Err
	}
	if stA == "" {
		return errors.New("Couldn't find a st")
	}
	stAIdx, ok := scores.lookup[stA]
	if !ok {
		return errors.New("Couldn't lookup a st")
	}

	for scoresDoc.Next() {
		stB = string(scoresDoc.Key())
		stBIdx, ok := scores.lookup[stB]
		if !ok {
			return errors.New("Couldn't lookup a st")
		}
		if err := scoresDoc.Value(&score); err != nil {
			return errors.New("Couldn't parse score")
		}
		scores.Set(stAIdx, stBIdx, int(score), FROM_CACHE)
	}
	if scoresDoc.Err != nil {
		return scoresDoc.Err
	}

	return nil
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

func NewProfileStore(scores *scoresStore) (profiles ProfileStore) {
	profiles.lookup = scores.lookup
	profiles.profiles = make([]Profile, len(scores.lookup))
	profiles.seen = make([]bool, len(profiles.profiles))
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

func updateProfiles(profiles ProfileStore, doc *bsonkit.Document) (bool, error) {
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

func (s *scoresStore) Set(stA int, stB int, score int, status int) error {
	idx, err := s.getIndex(stA, stB)
	if err != nil {
		return err
	}
	s.scores[idx].value = score
	s.scores[idx].status = status
	return nil
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

func parseGenomeDoc(doc *bsonkit.Document) (STs []CgmlstSt, IDs []GenomeSTPair, thresholds []int, err error) {
	IDs = make([]GenomeSTPair, 0, 100)
	STs = make([]string, 0, 100)
	thresholds = make([]int, 0, 10)

	seenSTs := make(map[CgmlstSt]bool)
	d := new(bsonkit.Document)
	genomes := new(bsonkit.Document)
	thresholdsDoc := new(bsonkit.Document)
	var foundGenomes, foundThresholds bool

	doc.Seek(0)
	for doc.Next() {
		if string(doc.Key()) == "genomes" {
			err = doc.Value(genomes)
			foundGenomes = true
		} else if string(doc.Key()) == "thresholds" {
			err = doc.Value(thresholdsDoc)
			foundThresholds = true
		}
		if err != nil {
			return
		}
	}
	if doc.Err != nil {
		err = doc.Err
		return
	} else if err != nil {
		return
	} else if !foundGenomes {
		err = errors.New("Expected genome field in document")
		return
	} else if !foundThresholds {
		err = errors.New("Expected thresholds field in document")
		return
	}

	for genomes.Next() {
		if err = genomes.Value(d); err != nil {
			return
		}

		var (
			id           GenomeID
			ST           CgmlstSt
			setST, setID bool
		)

		for d.Next() {
			switch string(d.Key()) {
			case "st":
				if err = d.Value(&ST); err != nil {
					return
				}
				setST = true
			case "_id":
				if err = d.Value(&id); err != nil {
					return
				}
				setID = true
			}
		}

		if d.Err != nil {
			err = d.Err
			return
		} else if !setST || !setID {
			err = errors.New("Couldn't parse genome ids")
			return
		}

		IDs = append(IDs, GenomeSTPair{id, ST})
		if _, seen := seenSTs[ST]; !seen {
			seenSTs[ST] = true
			STs = append(STs, ST)
		}
	}

	if genomes.Err != nil {
		err = genomes.Err
		return
	}

	for thresholdsDoc.Next() {
		var threshold int32
		if err = thresholdsDoc.Value(&threshold); err != nil {
			return
		}

		thresholds = append(thresholds, int(threshold))
	}

	if thresholdsDoc.Err != nil {
		err = thresholdsDoc.Err
	} else if len(thresholds) == 0 {
		err = errors.New("Expected at least one threshold")
	}

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

func parseAnalysis(analysisDoc *bsonkit.Document, p *Profile) (err error) {
	cgmlstDoc := new(bsonkit.Document)
	for analysisDoc.Next() {
		switch string(analysisDoc.Key()) {
		case "cgmlst":
			if err = analysisDoc.Value(cgmlstDoc); err != nil {
				return
			}
			err = parseCgMlst(cgmlstDoc, p)
			return
		}
	}
	if analysisDoc.Err != nil {
		return analysisDoc.Err
	}
	return errors.New("Could not find cgmlst in analysis")
}

func parseProfile(doc *bsonkit.Document) (profile Profile, err error) {
	analysisDoc := new(bsonkit.Document)
	doc.Seek(0)
	for doc.Next() {
		switch string(doc.Key()) {
		case "_id":
			if err = doc.Value(&profile.ID); err != nil {
				err = errors.New("Bad value for _id")
			}
		case "analysis":
			if err = doc.Value(analysisDoc); err != nil {
				err = errors.New("Bad value for analysis")
			} else {
				err = parseAnalysis(analysisDoc, &profile)
			}
		}
		if err != nil {
			return
		}
	}
	if doc.Err != nil {
		err = doc.Err
	}
	return
}

type GenomeSTPair struct {
	ID bsonkit.ObjectID
	ST CgmlstSt
}

func parse(r io.Reader, progress chan ProgressEvent) (STs []CgmlstSt, IDs []GenomeSTPair, profiles ProfileStore, scores scoresStore, thresholds []int, err error) {
	progress <- ProgressEvent{PARSING_STARTED, 0}
	defer func() { progress <- ProgressEvent{PARSING_COMPLETE, 0} }()
	err = nil
	errChan := make(chan error)
	numWorkers := 5
	var wg sync.WaitGroup

	docIter := bsonkit.GetDocuments(r)
	docChan := make(chan *bsonkit.Document, 50)
	go func() {
		defer close(docChan)
		for docIter.Next() {
			docChan <- docIter.Doc
		}
		if docIter.Err != nil {
			errChan <- docIter.Err
		}
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

	for firstDoc.Next() {
		switch string(firstDoc.Key()) {
		case "genomes":
			STs, IDs, thresholds, err = parseGenomeDoc(firstDoc)
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

	if len(IDs) == 0 {
		err = errors.New("No ids found in first doc")
		return
	}

	log.Printf("Found %d STs\n", len(STs))
	progress <- ProgressEvent{PROFILES_EXPECTED, len(STs)}

	scores = NewScores(STs)
	profiles = NewProfileStore(&scores)

	worker := func(workerID int) {
		nDocs := 0
		nProfiles := 0
		nScores := 0
		defer wg.Done()
		defer func() {
			log.Printf("Worker %d finished parsing %d scores and %d profile docs\n", workerID, nScores, nProfiles)
		}()

		log.Printf("Worker %d started\n", workerID)

		for doc := range docChan {
			for doc.Next() {
				switch string(doc.Key()) {
				case "alleleDifferences":
					if err := updateScores(scores, doc); err != nil {
						errChan <- err
						return
					}
					nScores++
					break
				case "analysis":
					if duplicate, err := updateProfiles(profiles, doc); err != nil {
						errChan <- err
						return
					} else if !duplicate {
						progress <- ProgressEvent{PROFILE_PARSED, 1}
					}
					nProfiles++
					break
				}
			}
			if doc.Err != nil {
				errChan <- doc.Err
				return
			}
			nDocs++
			if nDocs%100 == 0 {
				log.Printf("Worker %d parsed %d scores and %d profile docs\n", workerID, nScores, nProfiles)
			}
		}
	}

	for workerID := 0; workerID < numWorkers; workerID++ {
		wg.Add(1)
		go worker(workerID)
	}

	done := make(chan bool)
	go func() {
		wg.Wait()
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
