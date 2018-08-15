package main

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

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

	if STs, IDs, thresholds, err := parseGenomeDoc(doc); err != nil {
		t.Fatal(err)
	} else if len(STs) != 3 {
		t.Fatal("Expected 3 STs got", STs)
	} else if len(IDs) != 3 {
		t.Fatal("Expected 2 STs")
	} else if !reflect.DeepEqual(thresholds, []int{5, 50}) {
		t.Fatalf("Expected %v got %v\n", []int{5, 50}, thresholds)
	} else {
		expected := []string{"abc", "def", "ghi"}
		if !reflect.DeepEqual(STs, expected) {
			t.Fatalf("Expected %v got %v\n", expected, STs)
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

	if STs, IDs, thresholds, err := parseGenomeDoc(doc); err != nil {
		t.Fatal(err)
	} else if len(STs) != 2 {
		t.Fatal("Expected 2 STs")
	} else if len(IDs) != 3 {
		t.Fatal("Expected 2 STs")
	} else if !reflect.DeepEqual(thresholds, []int{5, 50}) {
		t.Fatalf("Expected %v got %v\n", []int{5, 50}, thresholds)
	} else {
		expected := []string{"abc", "ghi"}
		if !reflect.DeepEqual(STs, expected) {
			t.Fatalf("Expected %v got %v\n", expected, STs)
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

	if _, _, _, err := parseGenomeDoc(doc); err == nil {
		t.Fatal("This doesn't have a ST. Should have thrown an error")
	}

	// This isn't a genomes document
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if _, _, _, err := parseGenomeDoc(doc); err == nil {
		t.Fatal("This isn't a genomes document. Should have thrown an error")
	}

	// Doesn't have a thresholds key
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if _, _, _, err := parseGenomeDoc(doc); err == nil {
		t.Fatal("Doesn't have a thresholds key. Should have thrown an error")
	}

	// Thresholds are empty
	docs.Next()
	if docs.Err != nil {
		t.Fatal(docs.Err)
	}
	doc = docs.Doc

	if _, _, _, err := parseGenomeDoc(doc); err == nil {
		t.Fatal("Thresholds are empty. Should have thrown an error")
	}

	if docs.Next() {
		t.Fatal("Unexpected extra document")
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
	scores.Set(1, 0, 7, PENDING)
	scores.Set(3, 0, 5, COMPLETE)
	if err := updateScores(scores, doc); err != nil {
		t.Fatal(err)
	}

	var testCases = []struct {
		stA            int
		stB            int
		expectedValue  int
		expectedStatus int
	}{
		{0, 1, 1, FROM_CACHE},
		{1, 0, 1, FROM_CACHE},
		{0, 2, 2, FROM_CACHE},
		{2, 0, 2, FROM_CACHE},
		{0, 3, 5, COMPLETE},
		{3, 0, 5, COMPLETE},
	}

	for _, tc := range testCases {
		actual, err := scores.Get(tc.stA, tc.stB)
		if err != nil {
			t.Fatal(err)
		}
		if actual.value != tc.expectedValue {
			t.Fatalf("Got %v, expected %v", actual, tc)
		}
		if actual.status != tc.expectedStatus {
			t.Fatalf("Got %v, expected %v", actual, tc)
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

	if _, err := updateProfiles(profilesStore, docs.Doc); err != nil {
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
	STs, IDs, profiles, scores, thresholds, err := parse(testFile, ProgressSinkHole())
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
	expectedThresholds := []int{5, 50, 200, 500}
	if !reflect.DeepEqual(expectedThresholds, thresholds) {
		t.Fatalf("Expected thresholds %v, got %v", expectedThresholds, thresholds)
	}
}

func TestAllParse(t *testing.T) {
	var nSTs, expected int
	testFile, err := os.Open("testdata/FakeProfiles.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	STs, _, profiles, scores, thresholds, err := parse(testFile, ProgressSinkHole())
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
	expectedThresholds := []int{5, 50, 200, 500}
	if !reflect.DeepEqual(expectedThresholds, thresholds) {
		t.Fatalf("Expected thresholds %v, got %v", expectedThresholds, thresholds)
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

func BenchmarkBsonScores(b *testing.B) {
	stsFile, err := os.Open("testdata/scoresDocSts.csv")
	if err != nil {
		b.Fatal("Couldn't load test data")
	}
	r := csv.NewReader(stsFile)
	sts, err := r.Read()
	if err != nil {
		b.Error(err)
	}
	store := NewScores(sts)

	testFile, err := os.Open("testdata/scoresDoc.bson")
	if err != nil {
		b.Fatal("Couldn't load test data")
	}
	docs := bsonkit.GetDocuments(testFile)
	docs.Next()
	doc := docs.Doc
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := updateScores(store, doc); err != nil {
			b.Error(err)
		}
	}
}

func updateScoresCsv(store scoresStore, doc string) error {
	var (
		err           error
		inc, end, mid int
		score         int
	)

	first := strings.Index(doc, ",")
	second := strings.Index(doc[first+1:], ",")
	stA, ok := store.lookup[doc[first+1:second+first+1]]
	if !ok {
		return errors.New("Couldn't lookup a st")
	}

	offset := first + second + 2
	for {
		if offset > len(doc) {
			return nil
		}
		inc = strings.Index(doc[offset:], ",")
		end = offset + inc
		if inc == -1 {
			end = len(doc)
		}
		mid = offset + strings.Index(doc[offset:end], "=")
		score, err = strconv.Atoi(doc[mid+1 : end])
		if err != nil {
			return err
		}
		stB, ok := store.lookup[doc[offset:mid]]
		if !ok {
			return errors.New("Couldn't lookup a st")
		}
		store.Set(stA, stB, score, FROM_CACHE)
		offset = end + 1
	}
}

func BenchmarkCsvScores(b *testing.B) {
	stsFile, err := os.Open("testdata/scoresDocSts.csv")
	if err != nil {
		b.Fatal("Couldn't load test data")
	}
	r := csv.NewReader(stsFile)
	sts, err := r.Read()
	if err != nil {
		b.Error(err)
	}
	store := NewScores(sts)

	testFile, err := os.Open("testdata/scoresDoc.csv")
	if err != nil {
		b.Fatal("Couldn't load test data")
	}
	scanner := bufio.NewScanner(testFile)
	scanner.Scan()
	doc := scanner.Text()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := updateScoresCsv(store, doc); err != nil {
			b.Error(err)
		}
	}
}

func updateScoresSqlite(store scoresStore, db *sql.DB) error {
	rows, err := db.Query("select stA, stB, distance from distances")
	var (
		stA, stB string
		score    int
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&stA, &stB, &score)
		if err != nil {
			return err
		}
		stAIdx, ok := store.lookup[stA]
		if !ok {
			return errors.New("Couldn't lookup a st")
		}
		stBIdx, ok := store.lookup[stB]
		if !ok {
			return errors.New("Couldn't lookup a st")
		}

		store.Set(stAIdx, stBIdx, score, FROM_CACHE)
	}
	return rows.Err()
}

func BenchmarkSqliteScores(b *testing.B) {
	db, err := sql.Open("sqlite3", "testdata/scoresDoc.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	stsFile, err := os.Open("testdata/scoresDocSts.csv")
	if err != nil {
		b.Fatal("Couldn't load test data")
	}
	r := csv.NewReader(stsFile)
	sts, err := r.Read()
	if err != nil {
		b.Error(err)
	}
	store := NewScores(sts)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := updateScoresSqlite(store, db); err != nil {
			b.Error(err)
		}
	}
}

func updateScoresBinary(store scoresStore, doc []byte) error {
	const PREFIX = 12
	const ST_LENGTH = 20
	var stB string
	offset := PREFIX
	stA := hex.EncodeToString(doc[offset : offset+ST_LENGTH])
	a, ok := store.lookup[stA]
	if !ok {
		return errors.New("Couldn't lookup a st")
	}
	var score uint32
	offset += ST_LENGTH
	for offset < len(doc) {
		stB = hex.EncodeToString(doc[offset : offset+ST_LENGTH])
		score = binary.LittleEndian.Uint32(doc[offset+ST_LENGTH : offset+ST_LENGTH+4])
		b, ok := store.lookup[stB]
		if !ok {
			return errors.New("Couldn't lookup a st")
		}
		store.Set(a, b, int(score), FROM_CACHE)
		offset += (ST_LENGTH + 4)
	}
	return nil
}

func BenchmarkBinaryScores(b *testing.B) {
	stsFile, err := os.Open("testdata/scoresDocSts.csv")
	if err != nil {
		b.Fatal("Couldn't load test data")
	}
	r := csv.NewReader(stsFile)
	sts, err := r.Read()
	if err != nil {
		b.Error(err)
	}
	store := NewScores(sts)

	doc, err := ioutil.ReadFile("testdata/scoresDoc.bin")
	if err != nil {
		b.Fatal("Couldn't load test data")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := updateScoresBinary(store, doc); err != nil {
			b.Error(err)
		}
	}
}

func updateScoresTxt(store scoresStore, doc string) error {
	var (
		err   error
		score int
	)

	const PREFIX_LENGTH = 12
	const ST_LENGTH = 40
	const DISTANCE_LENGTH = 5
	stA, ok := store.lookup[doc[PREFIX_LENGTH:PREFIX_LENGTH+ST_LENGTH]]
	if !ok {
		return errors.New("Couldn't lookup a st")
	}

	offset := PREFIX_LENGTH + ST_LENGTH
	for offset < len(doc) {
		stB, ok := store.lookup[doc[offset:offset+ST_LENGTH]]
		if !ok {
			return errors.New("Couldn't lookup a st")
		}
		offset += ST_LENGTH
		score, err = strconv.Atoi(doc[offset : offset+DISTANCE_LENGTH])
		if err != nil {
			return err
		}
		store.Set(stA, stB, score, FROM_CACHE)
		offset += DISTANCE_LENGTH
	}
	return nil
}

func BenchmarkTxtScores(b *testing.B) {
	stsFile, err := os.Open("testdata/scoresDocSts.csv")
	if err != nil {
		b.Fatal("Couldn't load test data")
	}
	r := csv.NewReader(stsFile)
	sts, err := r.Read()
	if err != nil {
		b.Error(err)
	}
	store := NewScores(sts)

	testFile, err := os.Open("testdata/scoresDoc.txt")
	if err != nil {
		b.Fatal("Couldn't load test data")
	}
	scanner := bufio.NewScanner(testFile)
	scanner.Scan()
	doc := scanner.Text()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := updateScoresTxt(store, doc); err != nil {
			b.Error(err)
		}
	}
}
