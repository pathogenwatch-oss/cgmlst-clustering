package main

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestIndexer(t *testing.T) {
	lookup := make(map[CgmlstSt]int)
	lookup["abc123"] = 0
	lookup["bcd234"] = 1
	indexer := NewIndexer(lookup)
	indexer.Index(Profile{
		ST: "abc123",
		Matches: map[string]interface{}{
			"gene1": 1,
			"gene2": 1,
			"gene3": 1,
		},
	})
	index := indexer.indices[indexer.lookup["abc123"]]
	if value := index.Genes.blocks[0]; value != 7 {
		t.Fatalf("Got %d, expected 7\n", value)
	}

	indexer.Index(Profile{
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

	index = indexer.indices[indexer.lookup["bcd234"]]
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
			ST: "abc123",
			Matches: map[string]interface{}{
				"gene1": 1,
				"gene2": 1,
				"gene3": 1,
			},
		},
		Profile{
			ST: "bcd234",
			Matches: map[string]interface{}{
				"gene1": 2,
				"gene2": 2,
				"gene4": 1,
			},
		},
		Profile{
			ST: "cde345",
			Matches: map[string]interface{}{
				"gene1": 1,
				"gene2": 2,
				"gene3": 1,
				"gene4": 1,
			},
		},
	}

	lookup := make(map[CgmlstSt]int)
	lookup["abc123"] = 0
	lookup["bcd234"] = 1
	lookup["cde345"] = 2

	indexer := NewIndexer(lookup)
	for i, p := range profiles {
		indexer.Index(p)
		for j := 0; j < 10000; j++ {
			indexer.alleleTokens.Get(AlleleKey{
				fmt.Sprintf("fake-%d", i),
				j,
			})
		}
	}

	if nBlocks := len(indexer.indices[indexer.lookup["bcd234"]].Alleles.blocks); nBlocks != 157 {
		t.Fatalf("Expected 157 blocks, got %d\n", nBlocks)
	}
	if nBlocks := len(indexer.indices[indexer.lookup["cde345"]].Alleles.blocks); nBlocks != 313 {
		t.Fatalf("Expected 313 blocks, got %d\n", nBlocks)
	}

	comparer := Comparer{indexer.indices}
	if value := comparer.compare(0, 1); value != 2 {
		t.Fatalf("Expected 2, got %d\n", value)
	}
	if value := comparer.compare(0, 2); value != 1 {
		t.Fatalf("Expected 1, got %d\n", value)
	}
	if value := comparer.compare(1, 2); value != 1 {
		t.Fatalf("Expected 1, got %d\n", value)
	}
}

func TestScoreAll(t *testing.T) {
	testProfiles := [...]Profile{
		Profile{
			ST: "abc123",
			Matches: map[string]interface{}{
				"gene1": 1,
				"gene2": 1,
				"gene3": 1,
			},
		},
		Profile{
			ST: "bcd234",
			Matches: map[string]interface{}{
				"gene1": 2,
				"gene2": 2,
				"gene4": 1,
			},
		},
		Profile{
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
	scoreComplete, errChan := scoreAll(scores, profiles, ProgressSinkHole())
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

	_, profiles, scores, _, _, _, err := parse(testFile, ProgressSinkHole())
	if err != nil {
		t.Fatal(err)
	}
	scoreComplete, errChan := scoreAll(scores, profiles, ProgressSinkHole())
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
		if s.status == PENDING {
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
