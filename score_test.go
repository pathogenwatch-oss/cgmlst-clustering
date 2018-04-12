package main

import (
	"fmt"
	"os"
	"testing"

	"gitlab.com/cgps/bsonkit"
)

func TestIndexer(t *testing.T) {
	indexer := NewIndexer()
	indexer.Index(Profile{
		ID:         bsonkit.ObjectID{byte(0)},
		OrganismID: "1280",
		FileID:     "abc123",
		Public:     false,
		Version:    "v1",
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
		ID:         bsonkit.ObjectID{byte(1)},
		OrganismID: "1280",
		FileID:     "bcd234",
		Public:     false,
		Version:    "v1",
		Matches: map[string]interface{}{
			"gene1": 2,
			"gene2": 2,
			"gene4": 1,
		},
	})
	index = indexer.lookup["bcd234"]
	if value := index.Genes.blocks[0]; value != 11 {
		t.Fatalf("Got %d, expected 11\n", value)
	}
	if value := index.Alleles.blocks[0]; value != 56 {
		t.Fatalf("Got %d, expected 56\n", value)
	}
}

func TestComparer(t *testing.T) {
	profiles := [...]Profile{
		Profile{
			ID:         bsonkit.ObjectID{byte(0)},
			OrganismID: "1280",
			FileID:     "abc123",
			Public:     false,
			Version:    "v1",
			Matches: map[string]interface{}{
				"gene1": 1,
				"gene2": 1,
				"gene3": 1,
			},
		},
		Profile{
			ID:         bsonkit.ObjectID{byte(1)},
			OrganismID: "1280",
			FileID:     "bcd234",
			Public:     false,
			Version:    "v1",
			Matches: map[string]interface{}{
				"gene1": 2,
				"gene2": 2,
				"gene4": 1,
			},
		},
		Profile{
			ID:         bsonkit.ObjectID{byte(2)},
			OrganismID: "1280",
			FileID:     "cde345",
			Public:     false,
			Version:    "v1",
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
			ID:         bsonkit.ObjectID{byte(0)},
			OrganismID: "1280",
			FileID:     "abc123",
			Public:     false,
			Version:    "v1",
			Matches: map[string]interface{}{
				"gene1": 1,
				"gene2": 1,
				"gene3": 1,
			},
		},
		Profile{
			ID:         bsonkit.ObjectID{byte(1)},
			OrganismID: "1280",
			FileID:     "bcd234",
			Public:     false,
			Version:    "v1",
			Matches: map[string]interface{}{
				"gene1": 2,
				"gene2": 2,
				"gene4": 1,
			},
		},
		Profile{
			ID:         bsonkit.ObjectID{byte(2)},
			OrganismID: "1280",
			FileID:     "cde345",
			Public:     false,
			Version:    "v1",
			Matches: map[string]interface{}{
				"gene1": 1,
				"gene2": 2,
				"gene3": 1,
				"gene4": 1,
			},
		},
	}

	fileIDs := []string{"abc123", "bcd234", "cde345"}
	scores := NewScores(fileIDs)
	profiles := NewProfileStore(&scores)
	for _, p := range testProfiles {
		profiles.Add(p)
	}

	if err := scoreAll(scores, profiles); err != nil {
		t.Fatal(err)
	}

	nFileIds := len(scores.fileIDs)
	if nFileIds != len(testProfiles) {
		t.Fatal("Not enough fileIds")
	}
	if len(scores.scores) != nFileIds*(nFileIds-1)/2 {
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

func TestScoreAllStaph(t *testing.T) {
	testFile, err := os.Open("testdata/all_staph.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}

	_, profiles, scores, err := parse(testFile)
	if err != nil {
		t.Fatal(err)
	}

	if err := scoreAll(scores, profiles); err != nil {
		t.Fatal(err)
	}

	nFileIds := len(scores.fileIDs)
	nScores := len(scores.scores)
	if nFileIds != 12056 {
		t.Fatal("Expected some fileIds")
	}
	if nScores != nFileIds*(nFileIds-1)/2 {
		t.Fatal("Expected some scores")
	}
}
