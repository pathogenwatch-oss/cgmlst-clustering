package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"gopkg.in/mgo.v2/bson"
)

const (
	PENDING  int = iota
	QUEUED   int = iota
	COMPLETE int = iota
)

type M = map[string]interface{}
type L = []interface{}

type GenomesDoc struct {
	Genomes []struct {
		FileID string `bson:"fileId"`
	} `bson:"genomes"`
}

type ProfileDoc struct {
	ID         bson.ObjectId `bson:"_id"`
	OrganismID string        `bson:"organismId"`
	FileId     string        `bson:"fileId"`
	Public     bool          `bson:"public"`
	Analysis   struct {
		CgMlst struct {
			Version string `bson:"__v"`
			Matches []struct {
				Gene string      `bson:"gene"`
				Id   interface{} `bson:"id"`
			} `bson:"matches"`
		} `bson:"cgmlst"`
	} `bson:"analysis"`
}

type ScoreDoc struct {
	FileId string           `bson:"fileId"`
	Scores map[string]int32 `bson:"scores"`
}

func parseGenomeDoc(doc GenomesDoc) (fileIDs []string, err error) {
	fileIDs = make([]string, 0)
	seen := make(map[string]bool)

	for _, g := range doc.Genomes {
		if _, isSeen := seen[g.FileID]; isSeen {
			continue
		}
		fileIDs = append(fileIDs, g.FileID)
		seen[g.FileID] = true
	}
	return
}

func updateScores(scores scoresStore, doc ScoreDoc) error {
	if doc.FileId == "" {
		return errors.New("Profile doc had an invalid fileId")
	}

	fileA := doc.FileId
	for fileB, value := range doc.Scores {
		err := scores.Set(scoreDetails{fileA, fileB, int(value), COMPLETE})
		if err != nil {
			return err
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

func updateProfiles(profiles ProfileStore, doc ProfileDoc) error {
	if doc.FileId == "" {
		return errors.New("Profile doc had an invalid fileId")
	}

	idx, known := profiles.lookup[doc.FileId]
	if !known {
		return fmt.Errorf("unknown fileId %s", doc.FileId)
	}

	if profiles.seen[idx] {
		// This is a duplicate of something we've already parsed
		return nil
	}

	var p Profile
	p.FileID = doc.FileId
	p.ID = doc.ID
	p.OrganismID = doc.OrganismID
	p.Public = doc.Public
	p.Version = doc.Analysis.CgMlst.Version
	p.Matches = make(M)
	for _, m := range doc.Analysis.CgMlst.Matches {
		p.Matches[m.Gene] = m.Id
	}

	profiles.profiles[idx] = p
	profiles.seen[idx] = true
	return nil
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

func readBsonDocs(r io.Reader, errChan chan error) chan []byte {
	docs := make(chan []byte, 50)

	go func() {
		defer close(docs)
		for {
			// Loop based on https://github.com/pkg/bson/blob/af6d2c694850d177d255eef9610935cb2e5e6a7b/bson.go#L59
			// by [Dave Cheney](https://github.com/davecheney)
			// Copyright (c) 2014, David Cheney <dave@cheney.net>
			// BSD 2-Clause "Simplified" License
			var header [4]byte
			n, err := io.ReadFull(r, header[:])
			if err == io.EOF {
				return
			} else if err != nil {
				log.Println(err)
				errChan <- err
				return
			}
			if n < 4 {
				log.Println("bson document too short")
				errChan <- errors.New("bson document too short")
				return
			}
			doclen := int64(binary.LittleEndian.Uint32(header[:])) - 4
			r := io.LimitReader(r, doclen)
			buf := bytes.NewBuffer(header[:])
			nn, err := io.Copy(buf, r)
			if err != nil {
				log.Println(err)
				errChan <- err
				return
			}
			if nn != int64(doclen) {
				log.Println(err)
				errChan <- io.ErrUnexpectedEOF
				return
			}
			docs <- buf.Bytes()
		}
	}()

	return docs
}

func parse(r io.Reader) (fileIDs []string, profiles map[string]Profile, scores scoresStore, err error) {
	err = nil
	errChan := make(chan error)
	docs := readBsonDocs(r, errChan)
	numWorkers := 5
	var wg sync.WaitGroup

	firstDoc := <-docs
	var genomes GenomesDoc
	if parseErr := bson.Unmarshal(firstDoc, &genomes); parseErr != nil {
		err = errors.New("Could not get list of genomes from first document")
		return
	}
	if fileIDs, err = parseGenomeDoc(genomes); err != nil {
		return
	}

	scores = NewScores(fileIDs)
	profilesStore := NewProfileStore(&scores)
	profiles = make(map[string]Profile)

	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var (
				doc  []byte
				more bool
				p    ProfileDoc
				s    ScoreDoc
			)
			for nDocs := 1; ; nDocs++ {
				if doc, more = <-docs; !more {
					log.Printf("Worker %d is done after %d documents\n", worker, nDocs)
					return
				}
				if bytes.Contains(doc, []byte("cgmlst")) {
					if err := bson.Unmarshal(doc, &p); err != nil {
						errChan <- err
						return
					}
					if err := updateProfiles(profilesStore, p); err != nil {
						errChan <- err
						return
					}
				} else if bytes.Contains(doc, []byte("scores")) {
					if err := bson.Unmarshal(doc, &s); err != nil {
						errChan <- err
						return
					}
					if err := updateScores(scores, s); err != nil {
						errChan <- err
						return
					}
				} else {
					errChan <- fmt.Errorf("unknown document type found by worker %d", worker)
					return
				}
				if nDocs%100 == 0 {
					log.Printf("Worker %d parsed %d docs", worker, nDocs)
				}
			}
		}(worker)
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
			profiles[p.FileID] = p
		}
	}

	return
}
