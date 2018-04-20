package main

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
)

func compareClusters(t *testing.T, actual []string, expected []string) {
	if len(actual) != len(expected) {
		t.Fatalf("%v != %v\n", actual, expected)
	}
	for i := 0; i < len(actual); i++ {
		if actual[i] != expected[i] {
			t.Fatalf("%v != %v\n", actual, expected)
		}
	}
}

func TestGet(t *testing.T) {
	// Distances are based on the manhattan distance between the following:
	// ...A.DE
	// F..B...
	// .......
	// ...C...

	distances := []scoreDetails{
		{"a", "b", 1, COMPLETE},
		{"a", "c", 3, COMPLETE}, {"b", "c", 2, COMPLETE},
		{"a", "d", 2, COMPLETE}, {"b", "d", 3, COMPLETE}, {"c", "d", 5, COMPLETE},
		{"a", "e", 3, COMPLETE}, {"b", "e", 4, COMPLETE}, {"c", "e", 6, COMPLETE}, {"d", "e", 1, COMPLETE},
		{"a", "f", 4, COMPLETE}, {"b", "f", 3, COMPLETE}, {"c", "f", 5, COMPLETE}, {"d", "f", 6, COMPLETE}, {"e", "f", 7, COMPLETE},
	}

	scores := scoresStore{scores: distances, fileIDs: []string{"a", "b", "c", "d", "e", "f"}}
	clusters := NewClusters(scores)
	value := clusters.Get(1)
	expected := []string{"a", "a", "c", "d", "d", "f"}
	compareClusters(t, value, expected)

	value = clusters.Get(2)
	expected = []string{"a", "a", "a", "a", "a", "f"}
	compareClusters(t, value, expected)
}

func TestAnotherGet(t *testing.T) {
	// These data look a bit like this
	// i.e. A -> B is 2
	// and  A -> E is 2 + 4 = 6

	// A-2       3-D
	// 	 |       |
	// 	 B-5-G-5-C
	// 	 |       |
	// E-4       4-F

	distances := []scoreDetails{
		{"a", "b", 2, COMPLETE},
		{"a", "c", 12, COMPLETE}, {"b", "c", 10, COMPLETE},
		{"a", "d", 15, COMPLETE}, {"b", "d", 13, COMPLETE}, {"c", "d", 3, COMPLETE},
		{"a", "e", 6, COMPLETE}, {"b", "e", 4, COMPLETE}, {"c", "e", 14, COMPLETE}, {"d", "e", 17, COMPLETE},
		{"a", "f", 16, COMPLETE}, {"b", "f", 14, COMPLETE}, {"c", "f", 4, COMPLETE}, {"d", "f", 7, COMPLETE}, {"e", "f", 18, COMPLETE},
		{"a", "g", 7, COMPLETE}, {"b", "g", 5, COMPLETE}, {"c", "g", 5, COMPLETE}, {"d", "g", 8, COMPLETE}, {"e", "g", 9, COMPLETE}, {"f", "g", 9, COMPLETE},
	}

	scores := scoresStore{scores: distances, fileIDs: []string{"a", "b", "c", "d", "e", "f", "g"}}
	clusters := NewClusters(scores)

	value := clusters.Get(1)
	expected := []string{"a", "b", "c", "d", "e", "f", "g"}
	compareClusters(t, value, expected)

	value = clusters.Get(2)
	expected = []string{"a", "a", "c", "d", "e", "f", "g"}
	compareClusters(t, value, expected)

	value = clusters.Get(3)
	expected = []string{"a", "a", "c", "c", "e", "f", "g"}
	compareClusters(t, value, expected)

	value = clusters.Get(4)
	expected = []string{"a", "a", "c", "c", "a", "c", "g"}
	compareClusters(t, value, expected)

	value = clusters.Get(5)
	expected = []string{"a", "a", "a", "a", "a", "a", "a"}
	compareClusters(t, value, expected)

	value = clusters.Get(6)
	expected = []string{"a", "a", "a", "a", "a", "a", "a"}
	compareClusters(t, value, expected)

	value = clusters.Get(17)
	expected = []string{"a", "a", "a", "a", "a", "a", "a"}
	compareClusters(t, value, expected)

	value = clusters.Get(100)
	expected = []string{"a", "a", "a", "a", "a", "a", "a"}
	compareClusters(t, value, expected)
}

func randomScores(n int, seed int64) scoresStore {
	r := rand.New(rand.NewSource(seed))
	nDistances := (n * (n - 1)) / 2
	distances := make([]scoreDetails, nDistances)
	fileIDs := make([]string, n)
	fileIDs[0] = fmt.Sprintf("file%d", 0)

	idx := 0
	for a := 1; a < n; a++ {
		fileA := fmt.Sprintf("file%d", a)
		fileIDs[a] = fileA
		for b := 0; b < a; b++ {
			fileB := fmt.Sprintf("file%d", b)
			distances[idx] = scoreDetails{fileA, fileB, r.Intn(100 * n), COMPLETE}
			idx++
		}
	}

	return scoresStore{scores: distances, fileIDs: fileIDs}
}

func checkClusters(t *testing.T, scores scoresStore, clusters []string, threshold int) {
	idx := 0
	for a := 1; a < len(scores.fileIDs); a++ {
		for b := 0; b < a; b++ {
			d := scores.scores[idx].value
			if d <= threshold {
				if clusters[a] != clusters[b] {
					t.Fatalf("%d and %d should be in same cluster: distance (%d) <= threshold (%d)", a, b, d, threshold)
				}
			}
			idx++
		}
	}
}

func countClusters(clusters []string) int {
	seen := make(map[string]bool)
	for _, c := range clusters {
		seen[c] = true
	}
	return len(seen)
}

func TestRandomClusters(t *testing.T) {
	for seed := int64(0); seed < 10; seed++ {
		scores := randomScores(1000, seed)
		clusters := NewClusters(scores)

		for _, threshold := range []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000} {
			clusters := clusters.Get(threshold)
			nClusters := countClusters(clusters)
			log.Println(threshold, nClusters)
			checkClusters(t, scores, clusters, threshold)
		}
	}
}

func TestLotsOfRandomClusters(t *testing.T) {
	scores := randomScores(100000, 0)
	clusters := NewClusters(scores)

	log.Println("Made the fake scores")
	thresholds := []int{100, 500, 1000, 2000}

	for _, threshold := range thresholds {
		_ = clusters.Get(threshold)
	}
	log.Println("Done the clustering")
}
