package main

import (
	"fmt"
	"log"
	"os"
	"testing"
)

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

	indexer := NewIndexer([]string{"abc123", "bcd234", "cde345"})
	for i, p := range profiles {
		indexer.Index(&p)
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

	indexer := NewIndexer(STs)
	for _, p := range testProfiles {
		indexer.Index(&p)
	}

	scoreComplete, errChan := scoreAll(&scores, indexer, ProgressSinkHole())
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

func TestScoreAllFakeData(t *testing.T) {
	testFile, err := os.Open("testdata/FakeProfiles.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}

	index, scores, _, _, _, err := parse(testFile, ProgressSinkHole())
	if err != nil {
		t.Fatal(err)
	}
	scoreComplete, errChan := scoreAll(&scores, index, ProgressSinkHole())
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
