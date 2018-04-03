package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

// func TestParseGenomeDoc(t *testing.T) {
// 	testFile, err := os.Open("testdata/TestParseGenomeDoc.bson")
// 	if err != nil {
// 		t.Fatal("Couldn't load test data")
// 	}
// 	dec := bson.NewDecoder(testFile)

// 	// This should parse fine
// 	doc := make(map[string]interface{})
// 	if err := dec.Decode(&doc); err != nil {
// 		t.Fatal(err)
// 	}
// 	if fileIDs, err := parseGenomeDoc(doc); err != nil {
// 		t.Fatal(err)
// 	} else if len(fileIDs) != 3 {
// 		t.Fatal("Expected 3 fileIds")
// 	} else {
// 		expected := []string{"abc", "def", "ghi"}
// 		for i, fileID := range fileIDs {
// 			if fileID != expected[i] {
// 				t.Fatalf("%d: got %s, expected %s\n", i, fileID, expected[i])
// 			}
// 		}
// 	}

// 	// This has a duplicate fileId
// 	doc = make(map[string]interface{})
// 	if err := dec.Decode(&doc); err != nil {
// 		t.Fatal(err)
// 	}
// 	if fileIDs, err := parseGenomeDoc(doc); err != nil {
// 		t.Fatal(err)
// 	} else if len(fileIDs) != 2 {
// 		t.Fatal("Expected 2 fileIds")
// 	} else {
// 		expected := []string{"abc", "ghi"}
// 		for i, fileID := range fileIDs {
// 			if fileID != expected[i] {
// 				t.Fatalf("%d: got %s, expected %s\n", i, fileID, expected[i])
// 			}
// 		}
// 	}

// 	// This doesn't have a fileId
// 	doc = make(map[string]interface{})
// 	if err := dec.Decode(&doc); err != nil {
// 		t.Fatal(err)
// 	}
// 	if _, err := parseGenomeDoc(doc); err == nil {
// 		t.Fatal("Should have thrown an error")
// 	}

// 	// This isn't a genomes document
// 	doc = make(map[string]interface{})
// 	if err := dec.Decode(&doc); err != nil {
// 		t.Fatal(err)
// 	}
// 	if _, err := parseGenomeDoc(doc); err == nil {
// 		t.Fatal("Should have thrown an error")
// 	}
// }

// func TestUpdateScores(t *testing.T) {
// 	testFile, err := os.Open("testdata/TestUpdateScores.bson")
// 	if err != nil {
// 		t.Fatal("Couldn't load test data")
// 	}
// 	dec := bson.NewDecoder(testFile)
// 	doc := make(map[string]interface{})
// 	if err := dec.Decode(&doc); err != nil {
// 		t.Fatal(err)
// 	}

// 	scores := NewScores([]string{"abc", "bcd", "cde", "xyz"})
// 	scores.Set(scoreDetails{"bcd", "abc", 7, PENDING})
// 	scores.Set(scoreDetails{"xyz", "abc", 5, QUEUED})
// 	if err := updateScores(scores, doc); err != nil {
// 		t.Fatal(err)
// 	}

// 	var testCases = []struct {
// 		fileA          string
// 		fileB          string
// 		expectedValue  int
// 		expectedStatus int
// 	}{
// 		{"abc", "bcd", 1, COMPLETE},
// 		{"bcd", "abc", 1, COMPLETE},
// 		{"abc", "cde", 2, COMPLETE},
// 		{"cde", "abc", 2, COMPLETE},
// 		{"abc", "xyz", 5, QUEUED},
// 		{"xyz", "abc", 5, QUEUED},
// 	}

// 	for _, tc := range testCases {
// 		actual, err := scores.Get(tc.fileA, tc.fileB)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if actual.value != tc.expectedValue {
// 			t.Fatalf("Got %d, expected %d", actual.value, tc.expectedValue)
// 		}
// 		if actual.status != tc.expectedStatus {
// 			t.Fatalf("Got %d, expected %d", actual.status, tc.expectedStatus)
// 		}
// 	}
// }

// func TestUpdateProfiles(t *testing.T) {
// 	testFile, err := os.Open("testdata/TestUpdateProfiles.bson")
// 	if err != nil {
// 		t.Fatal("Couldn't load test data")
// 	}
// 	dec := bson.NewDecoder(testFile)
// 	doc := make(map[string]interface{})
// 	if err := dec.Decode(&doc); err != nil {
// 		t.Fatal(err)
// 	}

// 	profiles := make(map[string]Profile)
// 	if err := updateProfiles(profiles, doc); err != nil {
// 		t.Fatal(err)
// 	}

// 	var (
// 		p  Profile
// 		ok bool
// 	)
// 	if p, ok = profiles["abc"]; !ok {
// 		t.Fatal("doc is missing")
// 	}

// 	if actual, expected := p.FileID, "abc"; actual != expected {
// 		t.Fatalf("Expected %s, got %s\n", expected, actual)
// 	}
// 	if actual, expected := p.OrganismID, "1280"; actual != expected {
// 		t.Fatalf("Expected %s, got %s\n", expected, actual)
// 	}
// 	if actual, expected := len(p.Matches), 2; actual != expected {
// 		t.Fatalf("Expected %d, got %d\n", expected, actual)
// 	}
// 	if actual, expected := p.Matches["foo"], 1; int(actual.(int32)) != expected {
// 		t.Fatalf("Expected %d, got %d\n", expected, actual)
// 	}
// 	if actual, expected := p.Matches["bar"], "xyz"; actual.(string) != expected {
// 		t.Fatalf("Expected %s, got %s\n", expected, actual)
// 	}
// }

// func TestCheckProfiles(t *testing.T) {
// 	profiles := make(map[string]Profile)
// 	scores := NewScores([]string{"abc", "def"})

// 	scores.Set(scoreDetails{"abc", "def", 0, COMPLETE})
// 	if err := checkProfiles(profiles, scores); err != nil {
// 		t.Fatal("Should be OK")
// 	}

// 	profiles["abc"] = Profile{
// 		bson.ObjectId{byte(0)},
// 		"1280",
// 		"abc",
// 		true,
// 		"v0",
// 		make(map[string]interface{}),
// 	}

// 	if err := checkProfiles(profiles, scores); err != nil {
// 		t.Fatal("Should be OK")
// 	}

// 	scores.Set(scoreDetails{"abc", "def", 0, PENDING})
// 	if err := checkProfiles(profiles, scores); err == nil {
// 		t.Fatal("Should has failed")
// 	}

// 	profiles["def"] = Profile{
// 		bson.ObjectId{byte(0)},
// 		"1280",
// 		"def",
// 		true,
// 		"v0",
// 		make(map[string]interface{}),
// 	}
// 	if err := checkProfiles(profiles, scores); err != nil {
// 		t.Fatal("Should be OK")
// 	}
// }

// func TestParse(t *testing.T) {
// 	testFile, err := os.Open("testdata/TestParse.bson")
// 	if err != nil {
// 		t.Fatal("Couldn't load test data")
// 	}
// 	fileIDs, profiles, scores, err := parse(testFile)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if len(fileIDs) != 4 {
// 		t.Fatal("Expected 4 fileIds")
// 	}
// 	if len(profiles) != 2 {
// 		t.Fatal("Expected 2 profiles")
// 	}
// 	if len(scores.scores) != 6 {
// 		t.Fatal("Expected 6 scores")
// 	}
// }

func TestAllParse(t *testing.T) {
	var nFileIDs, expected int
	testFile, err := os.Open("testdata/all_staph.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	fileIDs, profiles, scores, err := parse(testFile)
	if err != nil {
		t.Fatal(err)
	}
	if nFileIDs, expected = len(fileIDs), 12056; nFileIDs != expected {
		t.Fatalf("Expected %d fileIds, got %d\n", expected, nFileIDs)
	}
	if actual, expected := len(profiles), nFileIDs; actual != expected {
		t.Fatalf("Expected %d profiles, got %d\n", expected, actual)
	}
	if actual, expected := len(scores.scores), nFileIDs*(nFileIDs-1)/2; actual != expected {
		t.Fatalf("Expected %d scores, got %d\n", expected, actual)
	}
}

func BenchmarkScores(b *testing.B) {
	fileIDs := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		fileIDs[i] = fmt.Sprintf("file%d", i)
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		scores := NewScores(fileIDs)
		for i, fileA := range fileIDs {
			for _, fileB := range fileIDs[:i] {
				scores.Set(scoreDetails{fileA, fileB, 0, PENDING})
			}
		}
	}
}

func TestNewScores(t *testing.T) {
	fileIDs := make([]string, 1000)
	for j := 0; j < 1000; j++ {
		fileIDs[j] = fmt.Sprintf("file%d", j)
	}

	scores := NewScores(fileIDs)

	idx := 0
	for i, fileA := range fileIDs {
		for _, fileB := range fileIDs[:i] {
			if err := scores.Set(scoreDetails{fileA, fileB, idx, PENDING}); err != nil {
				t.Fatal(err)
			}
			if score, err := scores.Get(fileA, fileB); score.value != idx || err != nil {
				t.Fatalf("Couldn't get the score for %s:%s", fileA, fileB)
			}
			if calc, err := scores.getIndex(fileA, fileB); calc != idx || err != nil {
				t.Fatalf("Got %d, expected %d", calc, idx)
			}
			idx++
		}
	}
}

func readBsonDocs(r io.Reader) (chan []byte, chan error) {
	docs := make(chan []byte, 50)
	errChan := make(chan error)

	go func() {
		defer close(docs)
		defer close(errChan)
		for nDoc := 0; nDoc < 5000; nDoc++ {
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

	return docs, errChan
}

func TestParseStructs(t *testing.T) {
	type Genomes struct {
		Genomes []struct {
			FileID string `bson:"fileId"`
		} `bson:"genomes"`
	}

	type Profile struct {
		ID         string `bson:"_id"`
		OrganismID string `bson:"organismId"`
		FileId     string `bson:"fileId"`
		Public     bool   `bson:"public"`
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

	testFile, err := os.Open("testdata/all_staph.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}

	docs, errChan := readBsonDocs(testFile)

	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var (
				more bool
				doc  []byte
			)
			var gene string
			for docCount := 0; ; docCount++ {
				select {
				case doc, more = <-docs:
					if !more {
						log.Printf("Worker %d saw gene %s\n", worker, gene)
						return
					}
					if bytes.Contains(doc, []byte("cgmlst")) {
						var p Profile
						if err := bson.Unmarshal(doc, &p); err != nil {
							t.Fatal(err)
							return
						}
						for _, m := range p.Analysis.CgMlst.Matches {
							gene = m.Gene
						}
					} else if bytes.Contains(doc, []byte("genomes")) {
						var g Genomes
						if err := bson.Unmarshal(doc, &g); err != nil {
							t.Fatal(err)
							return
						}
					}
					if docCount%100 == 0 {
						log.Printf("Worker %d parsed %d docs\n", worker, docCount)
					}
				case err, more := <-errChan:
					if more {
						log.Println(err)
						t.Fatal(err)
					}
					return
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestLoadsOfGoRoutines(t *testing.T) {
	r, err := os.Open("testdata/all_staph.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}

	type Genomes struct {
		Genomes []struct {
			FileID string `bson:"fileId"`
		} `bson:"genomes"`
	}

	type Profile struct {
		ID         string `bson:"_id"`
		OrganismID string `bson:"organismId"`
		FileId     string `bson:"fileId"`
		Public     bool   `bson:"public"`
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

	genomes := make(chan Genomes, 10)
	profiles := make(chan Profile, 10)
	errChan := make(chan error)
	var (
		wgController sync.WaitGroup
		wgParser     sync.WaitGroup
	)

	wgController.Add(1)
	go func() {
		defer wgController.Done()
		for {
			if _, more := <-profiles; !more {
				return
			}
		}
	}()

	wgController.Add(1)
	go func() {
		defer wgController.Done()
		for {
			if _, more := <-genomes; !more {
				return
			}
		}
	}()

	for nDoc := 0; nDoc < 5000; nDoc++ {
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
		wgParser.Add(1)
		go func(doc []byte) {
			defer wgParser.Done()
			if bytes.Contains(doc, []byte("cgmlst")) {
				var p Profile
				if err := bson.Unmarshal(doc, &p); err != nil {
					errChan <- err
					return
				}
				profiles <- p
			} else if bytes.Contains(doc, []byte("genomes")) {
				var g Genomes
				if err := bson.Unmarshal(doc, &g); err != nil {
					errChan <- err
					return
				}
				genomes <- g
			} else {
				errChan <- errors.New("Unexpected document type")
			}
		}(buf.Bytes())
		if nDoc%100 == 0 {
			log.Printf("Parsed %d docs", nDoc)
		}
	}
	wgParser.Wait()
	close(profiles)
	close(genomes)
	close(errChan)

	wgController.Wait()
	if err, _ := <-errChan; err != nil {
		t.Fatal(err)
	}
}
