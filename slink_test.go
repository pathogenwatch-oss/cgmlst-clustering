package main

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
)

func compareClusters(t *testing.T, actual []int, expected []int) {
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

	scores := scoresStore{scores: distances, STs: []string{"a", "b", "c", "d", "e", "f"}}
	distanceValues, _ := scores.Distances()
	clusters, err := NewClusters(len(scores.STs), distanceValues)
	if err != nil {
		t.Fatal(err)
	}
	value := clusters.Get(1)
	expected := []int{1, 1, 2, 4, 4, 5}
	compareClusters(t, value, expected)

	value = clusters.Get(2)
	expected = []int{4, 4, 4, 4, 4, 5}
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

	scores := scoresStore{scores: distances, STs: []string{"a", "b", "c", "d", "e", "f", "g"}}
	distanceValues, _ := scores.Distances()
	clusters, err := NewClusters(len(scores.STs), distanceValues)
	if err != nil {
		t.Fatal(err)
	}

	value := clusters.Get(1)
	expected := []int{0, 1, 2, 3, 4, 5, 6}
	compareClusters(t, value, expected)

	value = clusters.Get(2)
	expected = []int{1, 1, 2, 3, 4, 5, 6}
	compareClusters(t, value, expected)

	value = clusters.Get(3)
	expected = []int{1, 1, 3, 3, 4, 5, 6}
	compareClusters(t, value, expected)

	value = clusters.Get(4)
	expected = []int{4, 4, 5, 5, 4, 5, 6}
	compareClusters(t, value, expected)

	value = clusters.Get(5)
	expected = []int{6, 6, 6, 6, 6, 6, 6}
	compareClusters(t, value, expected)

	value = clusters.Get(6)
	expected = []int{6, 6, 6, 6, 6, 6, 6}
	compareClusters(t, value, expected)

	value = clusters.Get(17)
	expected = []int{6, 6, 6, 6, 6, 6, 6}
	compareClusters(t, value, expected)

	value = clusters.Get(100)
	expected = []int{6, 6, 6, 6, 6, 6, 6}
	compareClusters(t, value, expected)
}

func randomScores(n int, seed int64) scoresStore {
	r := rand.New(rand.NewSource(seed))
	nDistances := (n * (n - 1)) / 2
	distances := make([]scoreDetails, nDistances)
	STs := make([]string, n)
	STs[0] = fmt.Sprintf("st%d", 0)

	idx := 0
	for a := 1; a < n; a++ {
		stA := fmt.Sprintf("st%d", a)
		STs[a] = stA
		for b := 0; b < a; b++ {
			stB := fmt.Sprintf("st%d", b)
			distances[idx] = scoreDetails{stA, stB, r.Intn(100 * n), COMPLETE}
			idx++
		}
	}

	return scoresStore{scores: distances, STs: STs}
}

func checkClusters(t *testing.T, scores scoresStore, clusters []int, threshold int) {
	idx := 0
	for a := 1; a < len(scores.STs); a++ {
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

func countClusters(clusters []int) int {
	seen := make(map[int]bool)
	for _, c := range clusters {
		seen[c] = true
	}
	return len(seen)
}

func TestRandomClusters(t *testing.T) {
	for seed := int64(0); seed < 10; seed++ {
		nScores := 1000
		scores := randomScores(nScores, seed)

		distances, err := scores.Distances()
		if err != nil {
			t.Fatal(err)
		}
		clusters, err := NewClusters(nScores, distances)
		if err != nil {
			t.Fatal(err)
		}

		for _, threshold := range []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000} {
			clusters := clusters.Get(threshold)
			nClusters := countClusters(clusters)
			log.Println(threshold, nClusters)
			checkClusters(t, scores, clusters, threshold)
		}
	}
}

func TestLotsOfRandomClusters(t *testing.T) {
	log.Println("TestLotsOfRandomClusters: Start")
	nScores := 10000

	scores := randomScores(nScores, 0)
	distances, err := scores.Distances()
	if err != nil {
		t.Fatal(err)
	}
	log.Println("Made the fake scores")

	clusters, err := NewClusters(nScores, distances)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("Clustered")

	thresholds := []int{100, 500, 1000, 2000}
	for _, threshold := range thresholds {
		_ = clusters.Get(threshold)
		log.Println("Clustered at threshold:", threshold)
	}

}