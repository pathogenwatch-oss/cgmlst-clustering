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
	QUEUED   int = iota
	COMPLETE int = iota
)

type M = map[string]interface{}
type L = []interface{}

func updateScores(scores scoresStore, s *bsonkit.Document) error {
	var (
		fileA, fileB string
		ok           bool
		scoresDoc    *bsonkit.Document
		score        int
	)

	s.Seek(0)
	for s.Next() {
		switch string(s.Key()) {
		case "fileId":
			if fileA, ok = s.Value().(string); !ok {
				return errors.New("Couldn't parse fileId")
			}
		case "scores":
			if scoresDoc, ok = s.Value().(*bsonkit.Document); !ok {
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
		score = int(scoresDoc.Value().(int32))
		scores.Set(scoreDetails{fileA, fileB, score, COMPLETE})
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
		for _, fileB := range fileIDs[i+1:] {
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
	scoreIdx := (maxIdx * (maxIdx - 1) / 2) + minIdx
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

func checkProfiles(profiles ProfileStore, scores scoresStore) error {
	idx := 0
	for i := 1; i < len(scores.fileIDs); i++ {
		for j := 0; j < i; j++ {
			if scores.scores[idx].status == PENDING {
				if !profiles.seen[i] {
					return fmt.Errorf("expected profile for %s", scores.fileIDs[i])
				} else if !profiles.seen[j] {
					return fmt.Errorf("expected profile for %s", scores.fileIDs[j])
				}
			}
			idx++
		}
	}
	return nil
}

func parseGenomeDoc(doc *bsonkit.Document) (fileIDs []string, err error) {
	fileIDs = make([]string, 0)
	seen := make(map[string]bool)
	var (
		ok     bool
		fileID string
		d      *bsonkit.Document
	)

	doc.Seek(0)
	var value interface{} = errors.New("Missing a list of genomes")
	for doc.Next() {
		if string(doc.Key()) == "genomes" {
			value = doc.Value()
		}
		continue
	}
	if doc.Err != nil {
		err = doc.Err
		return
	}
	genomes, ok := value.(*bsonkit.Document)
	if !ok {
		err = value.(error)
		return
	}

	for genomes.Next() {
		if d, ok = genomes.Value().(*bsonkit.Document); !ok {
			err = errors.New("Couldn't parse genome document")
			return
		}
		fileID = ""
		for d.Next() {
			if string(d.Key()) != "fileId" {
				continue
			}
			if fileID, ok = d.Value().(string); !ok {
				err = errors.New("Couldn't parse fileId")
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
	var ok bool
	for matchDoc.Next() {
		switch string(matchDoc.Key()) {
		case "gene":
			if gene, ok = matchDoc.Value().(string); !ok {
				err = errors.New("Bad value for gene")
				return
			}
		case "id":
			id = matchDoc.Value()
		}
	}
	if matchDoc.Err != nil {
		err = matchDoc.Err
	}
	return
}

func parseMatches(matchesDoc *bsonkit.Document, p *Profile) error {
	p.Matches = make(M)
	for matchesDoc.Next() {
		gene, id, err := parseMatch(matchesDoc.Value().(*bsonkit.Document))
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
	var ok bool
	for cgmlstDoc.Next() {
		switch string(cgmlstDoc.Key()) {
		case "__v":
			if p.Version, ok = cgmlstDoc.Value().(string); !ok {
				err = errors.New("Bad value for __v")
			}
		case "matches":
			err = parseMatches(cgmlstDoc.Value().(*bsonkit.Document), p)
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
	for analysisDoc.Next() {
		switch string(analysisDoc.Key()) {
		case "cgmlst":
			err = parseCgMlst(analysisDoc.Value().(*bsonkit.Document), p)
			return
		}
	}
	if analysisDoc.Err != nil {
		return analysisDoc.Err
	}
	return errors.New("Could not find cgmlst in analysis")
}

func parseProfile(doc *bsonkit.Document) (profile Profile, err error) {
	var ok bool
	doc.Seek(0)
	for doc.Next() {
		switch string(doc.Key()) {
		case "_id":
			if profile.ID, ok = doc.Value().(bsonkit.ObjectID); !ok {
				err = errors.New("Bad value for _id")
			}
		case "fileId":
			if profile.FileID, ok = doc.Value().(string); !ok {
				err = errors.New("Bad value for fileId")
			}
		case "organismId":
			if profile.OrganismID, ok = doc.Value().(string); !ok {
				err = errors.New("Bad value for organismId")
			}
		case "public":
			if profile.Public, ok = doc.Value().(bool); !ok {
				err = errors.New("Bad value for public")
			}
		case "analysis":
			err = parseAnalysis(doc.Value().(*bsonkit.Document), &profile)
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

func parse(r io.Reader) (fileIDs []string, profiles map[string]Profile, scores scoresStore, err error) {
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
	case firstDoc = <-docChan:
		break
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
	profilesStore := NewProfileStore(&scores)
	profiles = make(map[string]Profile)

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
					if err := updateProfiles(profilesStore, doc); err != nil {
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
		err = checkProfiles(profilesStore, scores)
	}

	if err == nil {
		for _, p := range profilesStore.profiles {
			if p.FileID != "" {
				profiles[p.FileID] = p
			}
		}
	}

	return
}
