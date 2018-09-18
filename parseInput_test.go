package main

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"gitlab.com/cgps/bsonkit"
)

func TestParseRequestDoc(t *testing.T) {
	testFile, err := os.Open("testdata/TestParseRequestDoc.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	docs := bsonkit.GetDocuments(testFile)

	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc := docs.Doc

	if STs, maxThreshold, err := parseRequestDoc(doc); err != nil {
		t.Fatal(err)
	} else if len(STs) != 3 {
		t.Fatal("Expected 3 STs got", STs)
	} else if maxThreshold != 50 {
		t.Fatalf("Expected %v got %v\n", 50, maxThreshold)
	} else {
		expected := []string{"abc", "def", "ghi"}
		if !reflect.DeepEqual(STs, expected) {
			t.Fatalf("Expected %v got %v\n", expected, STs)
		}
	}

	// This has a duplicate ST
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if STs, maxThreshold, err := parseRequestDoc(doc); err != nil {
		t.Fatal(err)
	} else if len(STs) != 3 {
		t.Fatal("Expected 3 STs")
	} else if maxThreshold != 50 {
		t.Fatalf("Expected %v got %v\n", 50, maxThreshold)
	} else {
		expected := []string{"abc", "abc", "ghi"}
		if !reflect.DeepEqual(STs, expected) {
			t.Fatalf("Expected %v got %v\n", expected, STs)
		}
	}

	// This doesn't have a ST
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if _, _, err := parseRequestDoc(doc); err == nil {
		t.Fatal("This doesn't have a ST. Should have thrown an error")
	}

	// Doesn't have a maxThresholds key
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if _, _, err := parseRequestDoc(doc); err == nil {
		t.Fatal("Doesn't have a thresholds key. Should have thrown an error")
	}

	if docs.Next() {
		t.Fatal("Unexpected extra document")
	}
}

func TestParseCache(t *testing.T) {
	testFile, err := os.Open("testdata/TestParseCache.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}

	docs := bsonkit.GetDocuments(testFile)
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc := docs.Doc

	cache := NewCache()
	if err = cache.Update(doc, 5); err != nil {
		t.Fatal(err)
	}

	expected := []int{2, 3, 3, 3}
	if !reflect.DeepEqual(cache.Pi, expected) {
		t.Fatalf("Expected %v, got %v", expected, cache.Pi)
	}

	expected = []int{1, 1, 2, ALMOST_INF}
	if !reflect.DeepEqual(cache.Lambda, expected) {
		t.Fatalf("Expected %v, got %v", expected, cache.Lambda)
	}

	expectedStrings := []string{"a", "b", "c", "d"}
	if !reflect.DeepEqual(cache.Sts, expectedStrings) {
		t.Fatalf("Expected %v, got %v", expectedStrings, cache.Sts)
	}

	if cache.Threshold != 5 {
		t.Fatalf("Expected 5, got %v", cache.Threshold)
	}

	if docs.Next() {
		t.Fatal("Unexpected document")
	}
}

func TestSortSts(t *testing.T) {
	isSubset, STs, mapExistingToSts := sortSts([]CgmlstSt{}, []CgmlstSt{"a", "b", "c"})
	expected := []CgmlstSt{"a", "b", "c"}
	if !reflect.DeepEqual(STs, expected) {
		t.Fatalf("Expected %v, got %v\n", expected, STs)
	}
	if isSubset {
		t.Fatal("Wrong")
	}
	if len(mapExistingToSts) != 0 {
		t.Fatal("Wrong")
	}

	isSubset, STs, mapExistingToSts = sortSts([]CgmlstSt{"b", "a"}, []CgmlstSt{"a", "b", "c"})
	expected = []CgmlstSt{"b", "a", "c"}
	if !reflect.DeepEqual(STs, expected) {
		t.Fatalf("Expected %v, got %v\n", expected, STs)
	}
	if !isSubset {
		t.Fatal("Wrong")
	}
	if len(mapExistingToSts) != 2 {
		t.Fatal("Wrong")
	}
	if mapExistingToSts[0] != 0 || mapExistingToSts[1] != 1 {
		t.Fatalf("Didn't expect %v\n", mapExistingToSts)
	}

	isSubset, STs, mapExistingToSts = sortSts([]CgmlstSt{"b", "d", "a"}, []CgmlstSt{"a", "b", "c"})
	expected = []CgmlstSt{"b", "a", "c"}
	if !reflect.DeepEqual(STs, expected) {
		t.Fatalf("Expected %v, got %v\n", expected, STs)
	}
	if isSubset {
		t.Fatal("Wrong")
	}
	if len(mapExistingToSts) != 2 {
		t.Fatal("Wrong")
	}
	if mapExistingToSts[0] != 0 || mapExistingToSts[2] != 1 {
		t.Fatalf("Didn't expect %v\n", mapExistingToSts)
	}

	isSubset, STs, mapExistingToSts = sortSts([]CgmlstSt{"b", "a", "b"}, []CgmlstSt{"c", "a", "b", "c"})
	expected = []CgmlstSt{"b", "a", "c"}
	if !reflect.DeepEqual(STs, expected) {
		t.Fatalf("Expected %v, got %v\n", expected, STs)
	}
	if isSubset {
		t.Fatal("Wrong")
	}
	if len(mapExistingToSts) != 2 {
		t.Fatal("Wrong")
	}
	if mapExistingToSts[0] != 0 || mapExistingToSts[1] != 1 {
		t.Fatalf("Didn't expect %v\n", mapExistingToSts)
	}
}

func TestUpdateScores(t *testing.T) {
	STs := []CgmlstSt{"a", "b", "d", "e"}
	mapExistingToSts := map[int]int{0: 0, 1: 1, 3: 2}
	scores := NewScores(STs)

	testFile, err := os.Open("testdata/TestParseCache.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}

	docs := bsonkit.GetDocuments(testFile)
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc := docs.Doc

	cache := NewCache()
	if err = cache.Update(doc, 5); err != nil {
		t.Fatal(err)
	}

	if err := scores.UpdateFromCache(cache, mapExistingToSts); err != nil {
		t.Fatal(err)
	}

	expectedValues := []int{5, ALMOST_INF, 1, 0, 0, 0}
	for i, v := range expectedValues {
		if scores.scores[i].value != v {
			t.Fatal(i, v, scores.scores[i].value)
		}
	}

	expectedStatuses := []int{FROM_CACHE, FROM_CACHE, FROM_CACHE, PENDING, PENDING, PENDING}
	for i, v := range expectedStatuses {
		if scores.scores[i].status != v {
			t.Fatal(i, v, scores.scores[i].status)
		}
	}
}

func TestParseProfile(t *testing.T) {
	testFile, err := os.Open("testdata/TestUpdateProfiles.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}

	profilesStore := NewProfileStore([]string{"def", "abc"})

	docs := bsonkit.GetDocuments(testFile)
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}

	if _, err := profilesStore.AddFromDoc(docs.Doc); err != nil {
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

func ProgressSinkHole() chan ProgressEvent {
	hole := make(chan ProgressEvent)
	go func() {
		for range hole {
		}
	}()
	return hole
}

func TestParse(t *testing.T) {
	testFile, err := os.Open("testdata/TestParse.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	STs, profiles, scores, maxThreshold, existingClusters, canReuseCache, err := parse(testFile, ProgressSinkHole())
	if err != nil {
		t.Fatal(err)
	}
	if canReuseCache != true {
		t.Fatal("Expected true")
	}
	if len(STs) != 5 {
		t.Fatalf("Expected 5 STs, got %v", STs)
	}
	expected := []CgmlstSt{"a", "b", "c", "d", "e"}
	if !reflect.DeepEqual(expected, STs) {
		t.Fatalf("Got %v\n", STs)
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
	if len(scores.scores) != 10 {
		t.Fatal("Expected 10 scores")
	}
	if maxThreshold != 5 {
		t.Fatalf("Expected 50, got %v\n", maxThreshold)
	}
	if scores.scores[0].value != 5 {
		t.Fatalf("Got %v", scores.scores[0])
	}
	if scores.scores[3].value != ALMOST_INF {
		t.Fatalf("Got %v", scores.scores[3])
	}
	if scores.scores[6].status != PENDING {
		t.Fatalf("Got %v", scores.scores[6])
	}
	if existingClusters.nItems != 4 {
		t.Fatalf("Got %v\n", existingClusters.nItems)
	}
}

func TestParseNoCache(t *testing.T) {
	testFile, err := os.Open("testdata/TestParseNoCache.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	STs, profiles, scores, maxThreshold, existingClusters, canReuseCache, err := parse(testFile, ProgressSinkHole())
	if err != nil {
		t.Fatal(err)
	}
	if canReuseCache == true {
		t.Fatal("Expected false")
	}
	if len(STs) != 5 {
		t.Fatalf("Expected 5 STs, got %v", STs)
	}
	expected := []CgmlstSt{"a", "e", "b", "c", "d"}
	if !reflect.DeepEqual(expected, STs) {
		t.Fatalf("Got %v\n", STs)
	}
	nProfiles := 0
	for _, seen := range profiles.seen {
		if seen {
			nProfiles++
		}
	}
	if nProfiles != 2 {
		t.Fatalf("Expected 2 profiles, got %v\n", profiles.seen)
	}
	if len(scores.scores) != 10 {
		t.Fatal("Expected 10 scores")
	}
	if maxThreshold != 5 {
		t.Fatalf("Expected 50, got %v\n", maxThreshold)
	}
	for _, score := range scores.scores {
		if score.status != PENDING {
			t.Fatalf("Got %v", score)
		}
	}
	if existingClusters.nItems != 0 {
		t.Fatalf("Got %v\n", existingClusters.nItems)
	}
}

func TestParsePartialCache(t *testing.T) {
	testFile, err := os.Open("testdata/TestParsePartialCache.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	_, _, _, _, _, canReuseCache, err := parse(testFile, ProgressSinkHole())
	if err != nil {
		t.Fatal(err)
	}
	if canReuseCache != false {
		t.Fatal("Expected false")
	}
}

func TestParseDuplicates(t *testing.T) {
	testFile, err := os.Open("testdata/TestDuplicatePi.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	cache := NewCache()
	docs := bsonkit.GetDocuments(testFile)

	docs.Next()
	doc := docs.Doc
	if err := cache.Update(doc, 50); err != nil {
		t.Fatal(err)
	}

	docs.Next()
	doc = docs.Doc
	if err = cache.Update(doc, 50); err != nil {
		t.Fatal(err)
	}

	docs.Next()
	doc = docs.Doc
	if err = cache.Update(doc, 50); err == nil {
		t.Fatal("Expected a duplicate pi error")
	}
}

func TestParseThresholds(t *testing.T) {
	testFile, err := os.Open("testdata/TestParse.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	cache := NewCache()
	docs := bsonkit.GetDocuments(testFile)

	docs.Next()
	docs.Next()
	doc := docs.Doc
	if err := cache.Update(doc, 5); err != nil {
		t.Fatal(err)
	}

	if cache.Threshold != 5 {
		t.Fatal("Expected 5")
	}
}

func TestAllParse(t *testing.T) {
	var nSTs, expected int
	testFile, err := os.Open("testdata/FakeProfiles.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	STs, profiles, scores, maxThreshold, _, canReuseCache, err := parse(testFile, ProgressSinkHole())
	if err != nil {
		t.Fatal(err)
	}
	if !canReuseCache {
		t.Fatal("Expected true")
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
	if maxThreshold != 50 {
		t.Fatalf("Expected 50, got %v\n", maxThreshold)
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

	profiles := 0
	for docs.Next() {
		doc = docs.Doc
		for doc.Next() {
			if string(doc.Key()) == "results" {
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
}

func BenchmarkScores(b *testing.B) {
	STs := make([]CgmlstSt, 1000)
	for i := 0; i < 1000; i++ {
		STs[i] = fmt.Sprintf("st%d", i)
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		scores := NewScores(STs)
		for a := 1; a < len(STs); a++ {
			for b := 0; b < a; b++ {
				scores.Set(a, b, 0, PENDING)
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
	for a := 1; a < len(STs); a++ {
		for b := 0; b < a; b++ {
			if err := scores.Set(a, b, idx, PENDING); err != nil {
				t.Fatal(err)
			}
			if score, err := scores.Get(a, b); score.value != idx || err != nil {
				t.Fatalf("Couldn't get the score for %d:%d", a, b)
			}
			if calc, err := scores.getIndex(a, b); calc != idx || err != nil {
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
		a, b int
	}{
		{1, 0},
		{2, 0},
		{2, 1},
		{3, 0},
		{3, 1},
		{3, 2},
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
		a, b int
	}{
		{1, 0},
		{2, 0},
		{2, 1},
		{3, 0},
		{3, 1},
		{3, 2},
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
		a, b int
	}{
		{1, 0},
		{2, 0},
		{2, 1},
		{3, 0},
		{3, 1},
		{3, 2},
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
