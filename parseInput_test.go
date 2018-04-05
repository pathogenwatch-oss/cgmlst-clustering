package main

import (
	"fmt"
	"os"
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
	if actual, expected := len(profiles["cdc283e48ee0f027fc5761d9f1e63ed9806d01a3"].Matches), 2208; actual != expected {
		t.Fatalf("Expected %d matches, got %d\n", expected, actual)
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

type _ProfileDoc struct {
	Id         bson.ObjectId
	Organismid string
	Fileid     string
	Public     bool
	Analysis   struct {
		Cgmlst struct {
			Version string
			Matches []struct {
				Gene string
				// Id   int
			}
		}
	}
}

func TestStructParse(t *testing.T) {
	testFile, err := os.Open("testdata/genomes.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	errc := make(chan error)
	c := readBsonDocs(testFile, errc)
	doc := <-c
	var p _ProfileDoc
	if err := bson.Unmarshal(doc, &p); err != nil {
		t.Fatal(err)
	}
	if p.Fileid != "1b5be653a771dd81b5d9c290a6ac1c17f8999c97" {
		t.Fatal("Wrong fileId")
	}
	if p.Organismid != "1280" {
		t.Fatal("Wrong organismId")
	}
	t.Fatal(len(p.Analysis.Cgmlst.Matches))
	// if len(p.Analysis.Cgmlst.Matches) != 2226 {
	// 	t.Fatalf("Wrong number of matches %d", len(p.Analysis.Cgmlst.Matches))
	// }
}

func BenchmarkStructParse(b *testing.B) {
	testFile, err := os.Open("testdata/genomes.bson")
	if err != nil {
		b.Fatal("Couldn't load test data")
	}
	errc := make(chan error)
	c := readBsonDocs(testFile, errc)
	doc := <-c
	var p _ProfileDoc
	if err := bson.Unmarshal(doc, &p); err != nil {
		b.Fatal(err)
	}
	if p.Fileid != "1b5be653a771dd81b5d9c290a6ac1c17f8999c97" {
		b.Fatal("Wrong fileId")
	}
	if p.Organismid != "1280" {
		b.Fatal("Wrong organismId")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p1 _ProfileDoc
		if err := bson.Unmarshal(doc, &p1); err != nil {
			b.Fatal(err)
		}
	}
}
