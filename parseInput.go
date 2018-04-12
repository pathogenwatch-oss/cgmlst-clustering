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
	PENDING  int = iota
	COMPLETE int = iota
)

type M = map[string]interface{}
type L = []interface{}

func updateScores(scores scoresStore, s *bsonkit.Document) error {
	var (
		fileA, fileB string
		score        int32
	)

	scoresDoc := new(bsonkit.Document)

	s.Seek(0)
	for s.Next() {
		switch string(s.Key()) {
		case "fileId":
			if err := s.Value(&fileA); err != nil {
				return errors.New("Couldn't parse fileId")
			}
		case "scores":
			if err := s.Value(scoresDoc); err != nil {
				return errors.New("Couldn't parse scores")
			}
		}
	}
	if s.Err != nil {
		return s.Err
	}
	if fileA == "" {
		return errors.New("Couldn't find a fileId")
	}

	for scoresDoc.Next() {
		fileB = string(scoresDoc.Key())
		if err := scoresDoc.Value(&score); err != nil {
			return errors.New("Couldn't parse score")
		}
		scores.Set(scoreDetails{fileA, fileB, int(score), COMPLETE})
	}
	if scoresDoc.Err != nil {
		return scoresDoc.Err
	}

	return nil
}

type Profile struct {
	ID         bsonkit.ObjectID
	OrganismID string
	FileID     string
	Public     bool
	Version    string
	Matches    M
}

type ProfileStore struct {
	lookup   map[string]int
	profiles []Profile
	seen     []bool
}

func NewProfileStore(scores *scoresStore) (profiles ProfileStore) {
	profiles.lookup = scores.lookup
	profiles.profiles = make([]Profile, len(scores.lookup))
	profiles.seen = make([]bool, len(profiles.profiles))
	return
}

func (profiles *ProfileStore) Add(p Profile) error {
	idx, known := profiles.lookup[p.FileID]
	if !known {
		return fmt.Errorf("unknown fileId %s", p.FileID)
	}

	if profiles.seen[idx] {
		// This is a duplicate of something we've already parsed
		return nil
	}

	profiles.profiles[idx] = p
	profiles.seen[idx] = true
	return nil
}

func (profiles *ProfileStore) Get(fileID string) (Profile, error) {
	idx, known := profiles.lookup[fileID]
	if !known {
		return Profile{}, fmt.Errorf("unknown fileId %s", fileID)
	}
	if seen := profiles.seen[idx]; !seen {
		return Profile{}, fmt.Errorf("unknown fileId %s", fileID)
	}
	return profiles.profiles[idx], nil
}

func updateProfiles(profiles ProfileStore, doc *bsonkit.Document) error {
	p, err := parseProfile(doc)
	if err != nil {
		return err
	}

	if p.FileID == "" {
		return errors.New("Profile doc had an invalid fileId")
	}

	return profiles.Add(p)
}

type scoreDetails struct {
	fileA, fileB  string
	value, status int
}

type scoresStore struct {
	lookup  map[string]int
	scores  []scoreDetails
	fileIDs []string
}

func NewScores(fileIDs []string) (s scoresStore) {
	s.fileIDs = fileIDs
	s.scores = make([]scoreDetails, len(fileIDs)*(len(fileIDs)-1)/2)
	s.lookup = make(map[string]int)
	for idx, fileID := range fileIDs {
		s.lookup[fileID] = idx
	}
	idx := 0
	for i, fileA := range fileIDs {
		for _, fileB := range fileIDs[:i] {
			s.scores[idx] = scoreDetails{fileA, fileB, 0, PENDING}
			idx++
		}
	}
	return
}

func (s scoresStore) getIndex(fileA string, fileB string) (int, error) {
	idxA, ok := s.lookup[fileA]
	if !ok {
		return 0, fmt.Errorf("unknown fileId %s", fileA)
	}
	idxB, ok := s.lookup[fileB]
	if !ok {
		return 0, fmt.Errorf("unknown fileId %s", fileB)
	}
	minIdx, maxIdx := idxA, idxB
	if idxA == idxB {
		return 0, fmt.Errorf("fileIds shouldn't both be %s", fileA)
	} else if idxA > idxB {
		minIdx = idxB
		maxIdx = idxA
	}
	scoreIdx := ((maxIdx * (maxIdx - 1)) / 2) + minIdx
	return scoreIdx, nil
}

func (s *scoresStore) Set(score scoreDetails) error {
	idx, err := s.getIndex(score.fileA, score.fileB)
	if err != nil {
		return err
	}
	s.scores[idx] = score
	return nil
}

func (s scoresStore) Get(fileA string, fileB string) (scoreDetails, error) {
	idx, err := s.getIndex(fileA, fileB)
	if err != nil {
		return scoreDetails{}, err
	}
	return s.scores[idx], nil
}

func parseGenomeDoc(doc *bsonkit.Document) (fileIDs []string, err error) {
	fileIDs = make([]string, 0)
	seen := make(map[string]bool)
	var (
		fileID string
	)

	d := new(bsonkit.Document)
	genomes := new(bsonkit.Document)

	doc.Seek(0)
	for doc.Next() {
		if string(doc.Key()) == "genomes" {
			err = doc.Value(genomes)
			break
		}
	}
	if doc.Err != nil {
		err = doc.Err
		return
	} else if err != nil {
		return
	} else if string(doc.Key()) != "genomes" {
		err = errors.New("Not a genomes document")
		return
	}

	for genomes.Next() {
		if err = genomes.Value(d); err != nil {
			return
		}
		fileID = ""
		for d.Next() {
			if string(d.Key()) != "fileId" {
				continue
			}
			if err = d.Value(&fileID); err != nil {
				return
			}
			if _, duplicate := seen[fileID]; !duplicate {
				fileIDs = append(fileIDs, fileID)
				seen[fileID] = true
			}
			break
		}
		if d.Err != nil {
			err = d.Err
			return
		} else if fileID == "" {
			err = errors.New("Document didn't contain a fileId")
			return
		}
	}

	if genomes.Err != nil {
		err = genomes.Err
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
		case "__v":
			if err = cgmlstDoc.Value(&p.Version); err != nil {
				return errors.New("Bad value for __v")
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
	if p.Version == "" {
		return errors.New("version not found")
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
		case "fileId":
			if err = doc.Value(&profile.FileID); err != nil {
				err = errors.New("Bad value for fileId")
			}
		case "organismId":
			if err = doc.Value(&profile.OrganismID); err != nil {
				err = errors.New("Bad value for organismId")
			}
		case "public":
			if err = doc.Value(&profile.Public); err != nil {
				err = errors.New("Bad value for public")
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

func parse(r io.Reader) (fileIDs []string, profiles ProfileStore, scores scoresStore, err error) {
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
			fileIDs, err = parseGenomeDoc(firstDoc)
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

	if len(fileIDs) == 0 {
		err = errors.New("No fileIds found in first doc")
		return
	}

	log.Printf("Found %d fileIds\n", len(fileIDs))

	scores = NewScores(fileIDs)
	profiles = NewProfileStore(&scores)

	worker := func(workerID int) {
		nDocs := 0
		defer wg.Done()
		defer func() {
			log.Printf("Worker %d finished parsing %d docs\n", workerID, nDocs)
		}()

		log.Printf("Worker %d started\n", workerID)

		for doc := range docChan {
			for doc.Next() {
				switch string(doc.Key()) {
				case "scores":
					if err := updateScores(scores, doc); err != nil {
						errChan <- err
						return
					}
					break
				case "analysis":
					if err := updateProfiles(profiles, doc); err != nil {
						errChan <- err
						return
					}
					break
				}
			}
			if doc.Err != nil {
				errChan <- doc.Err
				return
			}
			nDocs++
			if nDocs%100 == 0 {
				log.Printf("Worker %d parsed %d docs\n", workerID, nDocs)
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
