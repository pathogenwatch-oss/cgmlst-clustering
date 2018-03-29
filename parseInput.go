package main

import (
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/pkg/bson"
)

const (
	PENDING  int = iota
	QUEUED   int = iota
	COMPLETE int = iota
)

type M = map[string]interface{}
type L = []interface{}

func parseGenomeDoc(doc M) (fileIDs []string, err error) {
	fileIDs = make([]string, 0)
	seen := make(map[string]bool)

	genomes, ok := doc["genomes"].(L)
	if !ok {
		return nil, errors.New("Not a genomes doc")
	}
	for _, rawGenome := range genomes {
		genome := rawGenome.(M)
		fileID, ok := genome["fileId"].(string)
		if !ok {
			return nil, errors.New("Couldn't parse fileId")
		}
		if _, isSeen := seen[fileID]; isSeen {
			continue
		}
		fileIDs = append(fileIDs, fileID)
		seen[fileID] = true
	}
	return
}

func updateScores(scores scoresStore, doc M) error {
	fileA, ok := doc["fileId"].(string)
	if !ok {
		return errors.New("Score doc doesn't have a fileId")
	}
	docScores, ok := doc["scores"].(M)
	if !ok {
		return errors.New("Score doc doesn't have scores")
	}
	for fileB, rScore := range docScores {
		if value, ok := rScore.(int32); !ok {
			// Log the error but keep parsing as many scores as possible
			return errors.New("Problem parsing one or more scores")
		} else {
			err := scores.Set(scoreDetails{fileA, fileB, int(value), COMPLETE})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type Profile struct {
	ID         bson.ObjectId
	OrganismID string
	FileID     string
	Public     bool
	Version    string
	Matches    M
}

func updateProfiles(profiles map[string]Profile, doc M) error {
	var (
		ok               bool
		analysis, cgmlst M
		rMatches         L
		gene             string
		allele           interface{}
	)
	var p Profile
	if p.FileID, ok = doc["fileId"].(string); !ok {
		return errors.New("No fileId in doc")
	}

	if _, exists := profiles[p.FileID]; exists {
		// This is a duplicate of something we've already parsed
		return nil
	}

	if p.ID, ok = doc["_id"].(bson.ObjectId); !ok {
		return errors.New("No _id in doc")
	}
	if p.OrganismID, ok = doc["organismId"].(string); !ok {
		return errors.New("No organismId in doc")
	}
	if p.Public, ok = doc["public"].(bool); !ok {
		return errors.New("No public field in doc")
	}

	analysis, ok = doc["analysis"].(M)
	if !ok {
		return errors.New("No analysis in doc")
	}
	cgmlst, ok = analysis["cgmlst"].(M)
	if !ok {
		return errors.New("No cgmlst in doc")
	}
	if p.Version, ok = cgmlst["__v"].(string); !ok {
		return errors.New("No version in doc")
	}
	if rMatches, ok = cgmlst["matches"].(L); !ok {
		return errors.New("No matches in doc")
	}

	p.Matches = make(M)
	for _, rMatch := range rMatches {
		match, ok := rMatch.(M)
		if !ok {
			return errors.New("Couldn't parse match")
		}
		if gene, ok = match["gene"].(string); !ok {
			return errors.New("Couldn't parse gene in profile")
		}
		if allele, ok = match["id"]; !ok {
			return errors.New("Couldn't parse allele in profile")
		}
		p.Matches[gene] = allele
	}

	profiles[p.FileID] = p
	return nil
}

type scoreDetails struct {
	fileA, fileB  string
	value, status int
}

type scoresStore struct {
	lookup map[string]int
	scores []scoreDetails
}

func NewScores(fileIDs []string) (s scoresStore) {
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
		return 0, errors.New(fmt.Sprintf("Unknown fileId %s\n", fileA))
	}
	idxB, ok := s.lookup[fileB]
	if !ok {
		return 0, errors.New(fmt.Sprintf("Unknown fileId %s\n", fileB))
	}
	minIdx, maxIdx := idxA, idxB
	if idxA == idxB {
		return 0, errors.New(fmt.Sprintf("fileIds shouldn't both be %\n", fileA))
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

func checkProfiles(profiles map[string]Profile, scores scoresStore) error {
	for _, score := range scores.scores {
		if score.status != PENDING {
			continue
		}
		if _, exists := profiles[score.fileA]; !exists {
			return errors.New(fmt.Sprintf("Needed profile for %s\n", score.fileA))
		}
		if _, exists := profiles[score.fileB]; !exists {
			return errors.New(fmt.Sprintf("Needed profile for %s\n", score.fileB))
		}
	}
	return nil
}

func parse(r io.Reader) (fileIDs []string, profiles map[string]Profile, scores scoresStore, err error) {
	dec := bson.NewDecoder(r)
	doc := make(M)
	err = nil

	if err = dec.Decode(&doc); err != nil {
		return
	} else if _, ok := doc["genomes"]; ok {
		fileIDs, err = parseGenomeDoc(doc)
	}
	if err != nil {
		err = errors.New("Expected 'genomes' in first document")
		return
	}
	log.Printf("Received %d fileIds\n", len(fileIDs))

	scores = NewScores(fileIDs)
	profiles = make(map[string]Profile)

	for docCount := 1; ; docCount++ {
		doc = make(M)
		if err = dec.Decode(&doc); err == io.EOF {
			log.Printf("Finished parsing %d documents\n", docCount-1)
			break
		} else if err != nil {
			return
		} else if _, ok := doc["scores"]; ok {
			updateScores(scores, doc)
		} else if _, ok := doc["analysis"]; ok {
			updateProfiles(profiles, doc)
		} else {
			log.Printf("Document %d is an unknown type\n", docCount)
		}
		if docCount%1000 == 0 {
			log.Printf("Parsed %d documents so far\n", docCount)
		}
	}

	err = checkProfiles(profiles, scores)
	return
}
