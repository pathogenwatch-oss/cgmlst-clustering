package main

import (
	"fmt"
	"os"
	"testing"

	"gitlab.com/cgps/bsonkit"
)

func TestParseGenomeDoc(t *testing.T) {
	testFile, err := os.Open("testdata/TestParseGenomeDoc.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	docs := bsonkit.GetDocuments(testFile)

	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc := docs.Doc

	if fileIDs, err := parseGenomeDoc(doc); err != nil {
		t.Fatal(err)
	} else if len(fileIDs) != 3 {
		t.Fatal("Expected 3 fileIds got", fileIDs)
	} else {
		expected := []string{"abc", "def", "ghi"}
		for i, fileID := range fileIDs {
			if fileID != expected[i] {
				t.Fatalf("%d: got %s, expected %s\n", i, fileID, expected[i])
			}
		}
	}

	// This has a duplicate fileId
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if fileIDs, err := parseGenomeDoc(doc); err != nil {
		t.Fatal(err)
	} else if len(fileIDs) != 2 {
		t.Fatal("Expected 2 fileIds")
	} else {
		expected := []string{"abc", "ghi"}
		for i, fileID := range fileIDs {
			if fileID != expected[i] {
				t.Fatalf("%d: got %s, expected %s\n", i, fileID, expected[i])
			}
		}
	}

	// This doesn't have a fileId
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if _, err := parseGenomeDoc(doc); err == nil {
		t.Fatal("Should have thrown an error")
	}

	// This isn't a genomes document
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if _, err := parseGenomeDoc(doc); err == nil {
		t.Fatal("Should have thrown an error")
	}
}

func TestUpdateScores(t *testing.T) {
	testFile, err := os.Open("testdata/TestUpdateScores.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}

	docs := bsonkit.GetDocuments(testFile)
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc := docs.Doc

	scores := NewScores([]string{"abc", "bcd", "cde", "xyz"})
	scores.Set(scoreDetails{"bcd", "abc", 7, PENDING})
	scores.Set(scoreDetails{"xyz", "abc", 5, COMPLETE})
	if err := updateScores(scores, doc); err != nil {
		t.Fatal(err)
	}

	var testCases = []struct {
		fileA          string
		fileB          string
		expectedValue  int
		expectedStatus int
	}{
		{"abc", "bcd", 1, COMPLETE},
		{"bcd", "abc", 1, COMPLETE},
		{"abc", "cde", 2, COMPLETE},
		{"cde", "abc", 2, COMPLETE},
		{"abc", "xyz", 5, COMPLETE},
		{"xyz", "abc", 5, COMPLETE},
	}

	for _, tc := range testCases {
		actual, err := scores.Get(tc.fileA, tc.fileB)
		if err != nil {
			t.Fatal(err)
		}
		if actual.value != tc.expectedValue {
			t.Fatalf("Got %d, expected %d", actual.value, tc.expectedValue)
		}
		if actual.status != tc.expectedStatus {
			t.Fatalf("Got %d, expected %d", actual.status, tc.expectedStatus)
		}
	}
}

func TestParseProfile(t *testing.T) {
	testFile, err := os.Open("testdata/TestUpdateProfiles.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}

	scores := NewScores([]string{"abc", "def"})
	profilesStore := NewProfileStore(&scores)

	docs := bsonkit.GetDocuments(testFile)
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}

	if err := updateProfiles(profilesStore, docs.Doc); err != nil {
		t.Fatal(err)
	}

	var (
		p Profile
	)
	if p, err = profilesStore.Get("abc"); err != nil {
		t.Fatal("profile is missing")
	}

	if actual, expected := p.FileID, "abc"; actual != expected {
		t.Fatalf("Expected %s, got %s\n", expected, actual)
	}
	if actual, expected := p.OrganismID, "1280"; actual != expected {
		t.Fatalf("Expected %s, got %s\n", expected, actual)
	}
	if actual, expected := len(p.Matches), 2; actual != expected {
		t.Fatalf("Expected %d, got %d\n", expected, actual)
	}
	if actual, expected := p.Matches["foo"], 1; int(actual.(int32)) != expected {
		t.Fatalf("Expected %d, got %d\n", expected, actual)
	}
	if actual, expected := p.Matches["bar"], "xyz"; actual.(string) != expected {
		t.Fatalf("Expected %s, got %s\n", expected, actual)
	}
}

func TestParse(t *testing.T) {
	testFile, err := os.Open("testdata/TestParse.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	fileIDs, profiles, scores, err := parse(testFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(fileIDs) != 4 {
		t.Fatal("Expected 4 fileIds")
	}
	nProfiles := 0
	for _, seen := range profiles.seen {
		if seen {
			nProfiles++
		}
	}
	if nProfiles != 2 {
		t.Fatalf("Expected 2 profiles, got %v\n", profiles.profiles)
	}
	if len(scores.scores) != 6 {
		t.Fatal("Expected 6 scores")
	}
}

func TestAllParse(t *testing.T) {
	var nFileIDs, expected int
	testFile, err := os.Open("testdata/FakeProfiles.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	fileIDs, profiles, scores, err := parse(testFile)
	if err != nil {
		t.Fatal(err)
	}
	p, err := profiles.Get("000000000000000000005000")
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := len(p.Matches), 1994; actual != expected {
		t.Fatalf("Expected %d matches, got %d\n", expected, actual)
	}
	if nFileIDs, expected = len(fileIDs), 7000; nFileIDs != expected {
		t.Fatalf("Expected %d fileIds, got %d\n", expected, nFileIDs)
	}
	if actual, expected := len(profiles.profiles), nFileIDs; actual != expected {
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

func TestScoresOrder(t *testing.T) {
	fileIDs := []string{"fileId1", "fileId2", "fileId3", "fileId4"}
	scores := NewScores(fileIDs)
	expected := []struct {
		a, b string
	}{
		{"fileId2", "fileId1"},
		{"fileId3", "fileId1"},
		{"fileId3", "fileId2"},
		{"fileId4", "fileId1"},
		{"fileId4", "fileId2"},
		{"fileId4", "fileId3"},
	}

	if len(scores.scores) != len(expected) {
		t.Fatalf("Expected %d scores, got %d\n", len(expected), len(scores.scores))
	}
	for i, score := range scores.scores {
		if score.fileA != expected[i].a || score.fileB != expected[i].b {
			t.Fatalf("Failed at %d: %v, got %v\n", i, expected[i], score)
		}
	}
}

func TestGetIndex(t *testing.T) {
	fileIDs := []string{"fileId1", "fileId2", "fileId3", "fileId4"}
	scores := NewScores(fileIDs)
	testCases := []struct {
		a, b string
	}{
		{"fileId2", "fileId1"},
		{"fileId3", "fileId1"},
		{"fileId3", "fileId2"},
		{"fileId4", "fileId1"},
		{"fileId4", "fileId2"},
		{"fileId4", "fileId3"},
	}

	for i, tc := range testCases {
		if v, err := scores.getIndex(tc.a, tc.b); err != nil {
			t.Fatal(err)
		} else if i != v {
			t.Fatalf("Expected %d, got %d\n", i, v)
		}
		if v, err := scores.getIndex(tc.b, tc.a); err != nil {
			t.Fatal(err)
		} else if i != v {
			t.Fatalf("Expected %d, got %d\n", i, v)
		}
	}
}

func TestGetScore(t *testing.T) {
	fileIDs := []string{"fileId1", "fileId2", "fileId3", "fileId4"}
	scores := NewScores(fileIDs)
	testCases := []struct {
		a, b string
	}{
		{"fileId2", "fileId1"},
		{"fileId3", "fileId1"},
		{"fileId3", "fileId2"},
		{"fileId4", "fileId1"},
		{"fileId4", "fileId2"},
		{"fileId4", "fileId3"},
	}

	for _, tc := range testCases {
		if v, err := scores.Get(tc.a, tc.b); err != nil {
			t.Fatal(err)
		} else if v.fileA != tc.a || v.fileB != tc.b {
			t.Fatalf("Expected %v, got %v\n", tc, v)
		}
		if v, err := scores.Get(tc.b, tc.a); err != nil {
			t.Fatal(err)
		} else if v.fileA != tc.a || v.fileB != tc.b {
			t.Fatalf("Expected %v, got %v\n", tc, v)
		}
	}
}
