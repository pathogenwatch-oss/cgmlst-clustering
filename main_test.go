package main

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func TestIndexer(t *testing.T) {
	indexer := NewIndexer()
	indexer.Index(Profile{
		ID:         "a",
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
		ID:         "b",
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
			ID:         "a",
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
			ID:         "b",
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
			ID:         "c",
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
	profiles := [...]Profile{
		Profile{
			ID:         "a",
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
			ID:         "b",
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
			ID:         "c",
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
	profilesChan := make(chan Profile)
	fileIdsChan := make(chan string)
	go func() {
		for _, p := range profiles {
			profilesChan <- p
			fileIdsChan <- p.FileID
		}
		close(profilesChan)
		close(fileIdsChan)
	}()

	result := scoreAll(profilesChan, fileIdsChan)
	nFileIds := len(result.FileIDs)
	if nFileIds != len(profiles) {
		t.Fatal("Not enough fileIds")
	}
	if len(result.Scores) != nFileIds*(nFileIds-1)/2 {
		t.Fatal("Not enough scores")
	}
	expectedScores := []int{2, 1, 1}
	for i, score := range expectedScores {
		if score != result.Scores[i] {
			t.Fatalf("Score %d was %d should be %d\n", i, result.Scores[i], score)
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

func BenchmarkScoreAll(b *testing.B) {
	for i := 0; i < b.N; i++ {
		profiles := make(chan Profile)
		fileIDsChan := make(chan string)

		profilesFile, err := os.Open("all_staph.bson")
		if err != nil {
			b.Fatal("Couldn't open test file")
		}
		r := (io.Reader)(profilesFile)
		parseProfiles(&r, profiles, fileIDsChan)

		scores := scoreAll(profiles, fileIDsChan)
		nFileIds := len(scores.FileIDs)
		nScores := len(scores.Scores)
		if nFileIds < 2 {
			b.Fatal("Expected some fileIds")
		}
		if nScores != nFileIds*(nFileIds-1)/2 {
			b.Fatal("Expected some fileIds")
		}
	}
}
