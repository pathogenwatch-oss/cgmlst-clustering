package main

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"gitlab.com/cgps/bsonkit"
)

func TestIndexer(t *testing.T) {
	indexer := NewIndexer()
	indexer.Index(Profile{
		ID: bsonkit.ObjectID{byte(0)},
		ST: "abc123",
		Matches: map[string]interface{}{
			"gene1": 1,
			"gene2": 1,
			"gene3": 1,
		},
	})
	index := indexer.lookup["abc123"]
	if value := index.Genes.blocks[0]; value != 7 {
		t.Fatalf("Got %d, expected 7\n", value)
	}

	indexer.Index(Profile{
		ID: bsonkit.ObjectID{byte(1)},
		ST: "bcd234",
		Matches: map[string]interface{}{
			"gene1": 2,
			"gene2": 2,
			"gene4": 1,
		},
	})

	valueOfGene3 := (1 << indexer.geneTokens.Get(AlleleKey{
		"gene3",
		nil,
	}))
	valueOfGene4 := (1 << indexer.geneTokens.Get(AlleleKey{
		"gene4",
		nil,
	}))
	expectedValue := 7 - valueOfGene3 + valueOfGene4

	index = indexer.lookup["bcd234"]
	if value := index.Genes.blocks[0]; value != uint64(expectedValue) {
		t.Fatalf("Got %d, expected %d\n", value, expectedValue)
	}
	if value := index.Alleles.blocks[0]; value != 56 {
		t.Fatalf("Got %d, expected 56\n", value)
	}
}

func TestComparer(t *testing.T) {
	profiles := [...]Profile{
		Profile{
			ID: bsonkit.ObjectID{byte(0)},
			ST: "abc123",
			Matches: map[string]interface{}{
				"gene1": 1,
				"gene2": 1,
				"gene3": 1,
			},
		},
		Profile{
			ID: bsonkit.ObjectID{byte(1)},
			ST: "bcd234",
			Matches: map[string]interface{}{
				"gene1": 2,
				"gene2": 2,
				"gene4": 1,
			},
		},
		Profile{
			ID: bsonkit.ObjectID{byte(2)},
			ST: "cde345",
			Matches: map[string]interface{}{
				"gene1": 1,
				"gene2": 2,
				"gene3": 1,
				"gene4": 1,
			},
		},
	}

	indexer := NewIndexer()
	for i, p := range profiles {
		indexer.Index(p)
		for j := 0; j < 10000; j++ {
			indexer.alleleTokens.Get(AlleleKey{
				fmt.Sprintf("fake-%d", i),
				j,
			})
		}
	}

	if nBlocks := len(indexer.lookup["bcd234"].Alleles.blocks); nBlocks != 157 {
		t.Fatalf("Expected 157 blocks, got %d\n", nBlocks)
	}
	if nBlocks := len(indexer.lookup["cde345"].Alleles.blocks); nBlocks != 313 {
		t.Fatalf("Expected 313 blocks, got %d\n", nBlocks)
	}

	comparer := Comparer{lookup: indexer.lookup}
	if value := comparer.compare("abc123", "bcd234"); value != 2 {
		t.Fatalf("Expected 2, got %d\n", value)
	}
	if value := comparer.compare("abc123", "cde345"); value != 1 {
		t.Fatalf("Expected 1, got %d\n", value)
	}
	if value := comparer.compare("bcd234", "cde345"); value != 1 {
		t.Fatalf("Expected 1, got %d\n", value)
	}
}

func TestScoreAll(t *testing.T) {
	testProfiles := [...]Profile{
		Profile{
			ID: bsonkit.ObjectID{byte(0)},
			ST: "abc123",
			Matches: map[string]interface{}{
				"gene1": 1,
				"gene2": 1,
				"gene3": 1,
			},
		},
		Profile{
			ID: bsonkit.ObjectID{byte(1)},
			ST: "bcd234",
			Matches: map[string]interface{}{
				"gene1": 2,
				"gene2": 2,
				"gene4": 1,
			},
		},
		Profile{
			ID: bsonkit.ObjectID{byte(2)},
			ST: "cde345",
			Matches: map[string]interface{}{
				"gene1": 1,
				"gene2": 2,
				"gene3": 1,
				"gene4": 1,
			},
		},
	}

	STs := []string{"abc123", "bcd234", "cde345"}
	scores := NewScores(STs)
	profiles := NewProfileStore(&scores)
	for _, p := range testProfiles {
		profiles.Add(p)
	}

	_, _, scoreComplete, errChan := scoreAll(scores, profiles)
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatal(err)
		}
	case <-scoreComplete:
	}

	nSTs := len(scores.STs)
	if nSTs != len(testProfiles) {
		t.Fatal("Not enough STs")
	}
	if len(scores.scores) != nSTs*(nSTs-1)/2 {
		t.Fatal("Not enough scores")
	}
	expectedScores := []int{2, 1, 1}
	for i, score := range expectedScores {
		if score != scores.scores[i].value {
			t.Fatalf("Score %d was %v should be %d\n", i, scores.scores[i], score)
		}
	}
}

func TestTokeniser(t *testing.T) {
	tokens := NewTokeniser()
	if token := tokens.Get(AlleleKey{"foo", 1}); token != 0 {
		t.Fatal("Wanted 0")
	}
	if token := tokens.Get(AlleleKey{"foo", 1}); token != 0 {
		t.Fatal("Wanted 0")
	}
	if token := tokens.Get(AlleleKey{"bar", 1}); token != 1 {
		t.Fatal("Wanted 1")
	}
	if token := tokens.Get(AlleleKey{"foo", 1}); token != 0 {
		t.Fatal("Wanted 0")
	}
	if token := tokens.Get(AlleleKey{"foo", "1"}); token != 2 {
		t.Fatal("Wanted 2")
	}
}

func TestScoreAllFakeData(t *testing.T) {
	testFile, err := os.Open("testdata/FakeProfiles.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}

	_, _, profiles, scores, _, err := parse(testFile)
	if err != nil {
		t.Fatal(err)
	}

	_, _, scoreComplete, errChan := scoreAll(scores, profiles)
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatal(err)
		}
	case <-scoreComplete:
	}

	nSTs := len(scores.STs)
	nScores := len(scores.scores)
	if nSTs != 7000 {
		t.Fatal("Expected some STs")
	}
	if nScores != nSTs*(nSTs-1)/2 {
		t.Fatal("Expected some scores")
	}

	for _, s := range scores.scores {
		if s.status != COMPLETE {
			t.Fatalf("Expected all scores to be complete: %v", s)
		}
	}

	distances, err := scores.Distances()
	if err != nil {
		t.Fatal(err)
	}

	clusters, err := NewClusters(nSTs, distances)
	if err != nil {
		t.Fatal(err)
	}

	thresholds := []int{10, 50, 100, 200}
	for _, threshold := range thresholds {
		c := clusters.Get(threshold)
		log.Println(threshold, countClusters(c))
	}
}

func TestBuildCacheOuputs(t *testing.T) {
	STs := []string{"1", "2", "3", "4"}
	scores := NewScores(STs)
	testCases := []scoreDetails{
		{"2", "1", 0, COMPLETE},
		{"3", "1", 1, COMPLETE},
		{"3", "2", 2, COMPLETE},
		{"4", "1", 3, COMPLETE},
		{"4", "2", 4, COMPLETE},
		{"4", "3", 5, COMPLETE},
	}
	expected := []CacheOutput{
		{"2", map[string]int{"1": 0}},
		{"3", map[string]int{"1": 1, "2": 2}},
		{"4", map[string]int{"1": 3, "2": 4, "3": 5}},
	}

	for _, tc := range testCases {
		scores.Set(tc)
	}

	output := buildCacheOutputs(scores)
	timeOut := time.After(5 * time.Second)

	var (
		actual CacheOutput
		more   bool
	)

	for _, tc := range expected {
		select {
		case actual, more = <-output:
			if !more {
				t.Fatal("Expected more")
			}
		case <-timeOut:
			t.Fatal("Shouldn't take this long")
		}

		if tc.ST != actual.ST {
			t.Fatalf("Expected %v, got %v", tc, actual)
		}
		if len(tc.AlleleDifferences) != len(actual.AlleleDifferences) {
			t.Fatalf("Expected %v, got %v", tc, actual)
		}
		for k, v := range tc.AlleleDifferences {
			if v != actual.AlleleDifferences[k] {
				t.Fatalf("Expected %v, got %v", tc, actual)
			}
		}
	}

	select {
	case actual, more = <-output:
		if more {
			t.Fatal("Expected no more")
		}
	case <-timeOut:
		t.Fatal("Shouldn't take this long")
	}

}
