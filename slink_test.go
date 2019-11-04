package main

import (
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"testing"
)

func compareSlices(t *testing.T, actual []int, expected []int) {
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
		{0, 1, 1, COMPLETE},
		{0, 2, 3, COMPLETE}, {1, 2, 2, COMPLETE},
		{0, 3, 2, COMPLETE}, {1, 3, 3, COMPLETE}, {2, 3, 5, COMPLETE},
		{0, 4, 3, COMPLETE}, {1, 4, 4, COMPLETE}, {2, 4, 6, COMPLETE}, {3, 4, 1, COMPLETE},
		{0, 5, 4, COMPLETE}, {1, 5, 3, COMPLETE}, {2, 5, 5, COMPLETE}, {3, 5, 6, COMPLETE}, {4, 5, 7, COMPLETE},
	}

	scores := scoresStore{scores: distances, STs: []string{"a", "b", "c", "d", "e", "f"}}
	distanceValues, _ := scores.Distances()
	clusters, err := ClusterFromScratch(distanceValues, len(scores.STs))
	if err != nil {
		t.Fatal(err)
	}
	value := clusters.Get(1)
	expected := []int{1, 1, 2, 4, 4, 5}
	compareSlices(t, value, expected)

	value = clusters.Get(2)
	expected = []int{4, 4, 4, 4, 4, 5}
	compareSlices(t, value, expected)
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
		{0, 1, 2, COMPLETE},
		{0, 2, 12, COMPLETE}, {1, 2, 10, COMPLETE},
		{0, 3, 15, COMPLETE}, {1, 3, 13, COMPLETE}, {2, 3, 3, COMPLETE},
		{0, 4, 6, COMPLETE}, {1, 4, 4, COMPLETE}, {2, 4, 14, COMPLETE}, {3, 4, 17, COMPLETE},
		{0, 5, 16, COMPLETE}, {1, 5, 14, COMPLETE}, {2, 5, 4, COMPLETE}, {3, 5, 7, COMPLETE}, {4, 5, 18, COMPLETE},
		{0, 6, 7, COMPLETE}, {1, 6, 5, COMPLETE}, {2, 6, 5, COMPLETE}, {3, 6, 8, COMPLETE}, {4, 6, 9, COMPLETE}, {5, 6, 9, COMPLETE},
	}

	scores := scoresStore{scores: distances, STs: []string{"a", "b", "c", "d", "e", "f", "g"}}
	distanceValues, _ := scores.Distances()
	clusters, err := ClusterFromScratch(distanceValues, len(scores.STs))
	if err != nil {
		t.Fatal(err)
	}

	value := clusters.Get(1)
	expected := []int{0, 1, 2, 3, 4, 5, 6}
	compareSlices(t, value, expected)

	value = clusters.Get(2)
	expected = []int{1, 1, 2, 3, 4, 5, 6}
	compareSlices(t, value, expected)

	value = clusters.Get(3)
	expected = []int{1, 1, 3, 3, 4, 5, 6}
	compareSlices(t, value, expected)

	value = clusters.Get(4)
	expected = []int{4, 4, 5, 5, 4, 5, 6}
	compareSlices(t, value, expected)

	value = clusters.Get(5)
	expected = []int{6, 6, 6, 6, 6, 6, 6}
	compareSlices(t, value, expected)

	value = clusters.Get(6)
	expected = []int{6, 6, 6, 6, 6, 6, 6}
	compareSlices(t, value, expected)

	value = clusters.Get(17)
	expected = []int{6, 6, 6, 6, 6, 6, 6}
	compareSlices(t, value, expected)

	value = clusters.Get(100)
	expected = []int{6, 6, 6, 6, 6, 6, 6}
	compareSlices(t, value, expected)
}

func TestUpdate(t *testing.T) {
	// A-2       3-D
	// 	 |       |
	// 	 B-5-G-5-C
	// 	 |       |
	// E-4       4-F
	distances := []scoreDetails{
		{0, 1, 2, COMPLETE},
		{0, 2, 12, COMPLETE}, {1, 2, 10, COMPLETE},
		{0, 3, 15, COMPLETE}, {1, 3, 13, COMPLETE}, {2, 3, 3, COMPLETE},
		{0, 4, 6, COMPLETE}, {1, 4, 4, COMPLETE}, {2, 4, 14, COMPLETE}, {3, 4, 17, COMPLETE},
		{0, 5, 16, COMPLETE}, {1, 5, 14, COMPLETE}, {2, 5, 4, COMPLETE}, {3, 5, 7, COMPLETE}, {4, 5, 18, COMPLETE},
		{0, 6, 7, COMPLETE}, {1, 6, 5, COMPLETE}, {2, 6, 5, COMPLETE}, {3, 6, 8, COMPLETE}, {4, 6, 9, COMPLETE}, {5, 6, 9, COMPLETE},
	}

	scores := scoresStore{scores: distances, STs: []string{"a", "b", "c", "d", "e", "f", "g"}}
	distanceValues, _ := scores.Distances()

	partial, err := ClusterFromScratch(distanceValues[:3], 3)
	if err != nil {
		t.Fatal(err)
	}

	expectedLambda := []int{2, 10, ALMOST_INF}
	expectedPi := []int{1, 2, 2}
	compareSlices(t, partial.lambda, expectedLambda)
	compareSlices(t, partial.pi, expectedPi)

	complete, err := ClusterFromScratch(distanceValues, len(scores.STs))
	if err != nil {
		t.Fatal(err)
	}

	cache := Cache{
		Pi:     partial.pi,
		Lambda: partial.lambda,
	}
	for i := 0; i < 3; i++ {
		distances[i] = scoreDetails{}
	}
	updated, err := ClusterFromCache(distanceValues, len(scores.STs), &cache)
	if err != nil {
		t.Fatal(err)
	}
	expectedLambda = []int{2, 4, 3, 4, 5, 5, ALMOST_INF}
	expectedPi = []int{1, 4, 3, 5, 6, 6, 6}
	compareSlices(t, updated.lambda, expectedLambda)
	compareSlices(t, updated.pi, expectedPi)

	compareSlices(t, updated.lambda, complete.lambda)
	compareSlices(t, updated.pi, complete.pi)
}

func TestAnotherUpdate(t *testing.T) {

	// .---3--.
	// A-5-B-9-C-2-D
	// |   '---1---|
	// '-----6-----'

	distances := []scoreDetails{
		{0, 1, 5, COMPLETE},
		{0, 2, 3, COMPLETE}, {1, 2, 9, COMPLETE},
		{0, 3, 6, COMPLETE}, {1, 3, 1, COMPLETE}, {2, 3, 2, COMPLETE},
	}

	scores := scoresStore{scores: distances, STs: []string{"a", "b", "c", "d"}}
	distanceValues, _ := scores.Distances()

	partial, err := ClusterFromScratch(distanceValues[:3], 3)
	if err != nil {
		t.Fatal(err)
	}

	expectedLambda := []int{3, 5, ALMOST_INF}
	expectedPi := []int{2, 2, 2}
	compareSlices(t, partial.lambda, expectedLambda)
	compareSlices(t, partial.pi, expectedPi)

	complete, err := ClusterFromScratch(distanceValues, len(scores.STs))
	if err != nil {
		t.Fatal(err)
	}

	cache := Cache{
		Pi:     partial.pi,
		Lambda: partial.lambda,
	}
	for i := 0; i < 3; i++ {
		distances[i] = scoreDetails{}
	}
	updated, err := ClusterFromCache(distanceValues, len(scores.STs), &cache)
	if err != nil {
		t.Fatal(err)
	}
	expectedLambda = []int{3, 1, 2, ALMOST_INF}
	expectedPi = []int{3, 3, 3, 3}
	compareSlices(t, updated.lambda, expectedLambda)
	compareSlices(t, updated.pi, expectedPi)

	compareSlices(t, updated.lambda, complete.lambda)
	compareSlices(t, updated.pi, complete.pi)
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
			distances[idx] = scoreDetails{a, b, r.Intn(100 * n), COMPLETE}
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
		clusters, err := ClusterFromScratch(distances, nScores)
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

	clusters, err := ClusterFromScratch(distances, nScores)
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

func TestFormat(t *testing.T) {
	clusters := Clusters{make([]int, 5), make([]int, 5), 5}
	distances := []int{5, 1, 9, 6, 1, 2, 1, 2, 0, 7}
	output := clusters.Format(5, distances, []CgmlstSt{"a", "b", "c", "d", "e"})
	expectedEdges := map[int][][2]int{
		0: {{2, 4}},
		1: {{0, 2}, {1, 3}, {0, 4}},
		2: {{2, 3}, {1, 4}},
		3: {},
		4: {},
		5: {{0, 1}},
	}

	count := 0
	seen := make([]bool, 6)
	for o := range output {
		for distance := range o.Edges {
			if !reflect.DeepEqual(expectedEdges[distance], o.Edges[distance]) {
				t.Fatal(distance, o.Edges)
			}
			if seen[distance] {
				t.Fatal(distance)
			}
			seen[distance] = true
			count++
		}
	}
	if count != 6 {
		t.Fatal(count)
	}

}
