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

	if STs, IDs, err := parseGenomeDoc(doc); err != nil {
		t.Fatal(err)
	} else if len(STs) != 3 {
		t.Fatal("Expected 3 STs got", STs)
	} else if len(IDs) != 3 {
		t.Fatal("Expected 2 STs")
	} else {
		expected := []string{"abc", "def", "ghi"}
		for i, ST := range STs {
			if ST != expected[i] {
				t.Fatalf("%d: got %s, expected %s\n", i, ST, expected[i])
			}
		}
		for i, ID := range IDs {
			if ID.ST != expected[i] {
				t.Fatalf("%d: got %s, expected %s\n", i, ID, expected[i])
			}
		}
	}

	// This has a duplicate ST
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if STs, IDs, err := parseGenomeDoc(doc); err != nil {
		t.Fatal(err)
	} else if len(STs) != 2 {
		t.Fatal("Expected 2 STs")
	} else if len(IDs) != 3 {
		t.Fatal("Expected 2 STs")
	} else {
		expected := []string{"abc", "ghi"}
		for i, ST := range STs {
			if ST != expected[i] {
				t.Fatalf("%d: got %s, expected %s\n", i, ST, expected[i])
			}
		}
		expected = []string{"abc", "abc", "ghi"}
		for i, ID := range IDs {
			if ID.ST != expected[i] {
				t.Fatalf("%d: got %s, expected %s\n", i, ID, expected[i])
			}
		}
	}

	// This doesn't have a ST
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if _, _, err := parseGenomeDoc(doc); err == nil {
		t.Fatal("Should have thrown an error")
	}

	// This isn't a genomes document
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if _, _, err := parseGenomeDoc(doc); err == nil {
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
		stA            string
		stB            string
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
		actual, err := scores.Get(tc.stA, tc.stB)
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

	if actual, expected := p.ST, "abc"; actual != expected {
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
	STs, IDs, profiles, scores, err := parse(testFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(IDs) != 4 {
		t.Fatal("Expected 4 IDs")
	}
	if len(STs) != 4 {
		t.Fatal("Expected 4 STs")
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
	var nSTs, expected int
	testFile, err := os.Open("testdata/FakeProfiles.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	STs, _, profiles, scores, err := parse(testFile)
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
	if nSTs, expected = len(STs), 7000; nSTs != expected {
		t.Fatalf("Expected %d STs, got %d\n", expected, nSTs)
	}
	if actual, expected := len(profiles.profiles), nSTs; actual != expected {
		t.Fatalf("Expected %d profiles, got %d\n", expected, actual)
	}
	if actual, expected := len(scores.scores), nSTs*(nSTs-1)/2; actual != expected {
		t.Fatalf("Expected %d scores, got %d\n", expected, actual)
	}
}

func TestRead(t *testing.T) {
	testFile, err := os.Open("testdata/FakeProfiles.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}

	docs := bsonkit.GetDocuments(testFile)
	docs.Next()

	doc := docs.Doc
	if !doc.Next() {
		t.Fatal("Expected a key")
	} else if doc.Err != nil {
		t.Fatal(doc.Err)
	}

	scores := 0
	profiles := 0
	for docs.Next() {
		doc = docs.Doc
		for doc.Next() {
			if string(doc.Key()) == "alleleDifferences" {
				scores++
				break
			} else if string(doc.Key()) == "analysis" {
				profiles++
				break
			}
		}
		if doc.Err != nil {
			t.Fatal(doc.Err)
		}
	}
	if docs.Err != nil {
		t.Fatal(doc.Err)
	}

	if profiles != 7000 {
		t.Fatalf("Expected 10000 profiles, got %d\n", profiles)
	}
	if scores != 5572 {
		t.Fatalf("Expected 5572 alleleDifferences got %d\n", scores)
	}
}

func BenchmarkScores(b *testing.B) {
	STs := make([]CgmlstSt, 1000)
	for i := 0; i < 1000; i++ {
		STs[i] = fmt.Sprintf("st%d", i)
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		scores := NewScores(STs)
		for i, stA := range STs {
			for _, stB := range STs[:i] {
				scores.Set(scoreDetails{stA, stB, 0, PENDING})
			}
		}
	}
}

func TestNewScores(t *testing.T) {
	STs := make([]CgmlstSt, 1000)
	for j := 0; j < 1000; j++ {
		STs[j] = fmt.Sprintf("st%d", j)
	}

	scores := NewScores(STs)

	idx := 0
	for i, stA := range STs {
		for _, stB := range STs[:i] {
			if err := scores.Set(scoreDetails{stA, stB, idx, PENDING}); err != nil {
				t.Fatal(err)
			}
			if score, err := scores.Get(stA, stB); score.value != idx || err != nil {
				t.Fatalf("Couldn't get the score for %s:%s", stA, stB)
			}
			if calc, err := scores.getIndex(stA, stB); calc != idx || err != nil {
				t.Fatalf("Got %d, expected %d", calc, idx)
			}
			idx++
		}
	}
}

func TestScoresOrder(t *testing.T) {
	STs := []string{"st1", "st2", "st3", "st4"}
	scores := NewScores(STs)
	expected := []struct {
		a, b string
	}{
		{"st2", "st1"},
		{"st3", "st1"},
		{"st3", "st2"},
		{"st4", "st1"},
		{"st4", "st2"},
		{"st4", "st3"},
	}

	if len(scores.scores) != len(expected) {
		t.Fatalf("Expected %d scores, got %d\n", len(expected), len(scores.scores))
	}
	for i, score := range scores.scores {
		if score.stA != expected[i].a || score.stB != expected[i].b {
			t.Fatalf("Failed at %d: %v, got %v\n", i, expected[i], score)
		}
	}
}

func TestGetIndex(t *testing.T) {
	STs := []string{"st1", "st2", "st3", "st4"}
	scores := NewScores(STs)
	testCases := []struct {
		a, b string
	}{
		{"st2", "st1"},
		{"st3", "st1"},
		{"st3", "st2"},
		{"st4", "st1"},
		{"st4", "st2"},
		{"st4", "st3"},
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
	STs := []string{"st1", "st2", "st3", "st4"}
	scores := NewScores(STs)
	testCases := []struct {
		a, b string
	}{
		{"st2", "st1"},
		{"st3", "st1"},
		{"st3", "st2"},
		{"st4", "st1"},
		{"st4", "st2"},
		{"st4", "st3"},
	}

	for _, tc := range testCases {
		if v, err := scores.Get(tc.a, tc.b); err != nil {
			t.Fatal(err)
		} else if v.stA != tc.a || v.stB != tc.b {
			t.Fatalf("Expected %v, got %v\n", tc, v)
		}
		if v, err := scores.Get(tc.b, tc.a); err != nil {
			t.Fatal(err)
		} else if v.stA != tc.a || v.stB != tc.b {
			t.Fatalf("Expected %v, got %v\n", tc, v)
		}
	}
}
