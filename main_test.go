package main

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestSubset(t *testing.T) {
	testFile, err := os.Open("testdata/TestRequestIsSubset.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	progress := ProgressSinkHole()
	index, scores, maxThreshold, _, canReuseCache, err := parse(testFile, progress)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(scores.STs, []CgmlstSt{"a", "b"}) {
		t.Fatal("STs")
	}
	if maxThreshold != 4 {
		t.Fatal("maxThreshold")
	}
	if canReuseCache {
		t.Fatal("Cannot reuse cache")
	}

	distances, err := scores.Distances()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(distances, []int{1}) {
		t.Fatal(distances)
	}

	scoreComplete, errChan := scoreAll(&scores, index, progress)

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatal(err)
		}
	case <-scoreComplete:
	}

	distances, err = scores.Distances()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(distances, []int{1}) {
		t.Fatal(distances)
	}

	clusters, err := NewClusters(len(scores.STs), distances)
	if err != nil {
		t.Fatal(err)
	}
	output := clusters.Format(maxThreshold, distances, scores.STs)

	for dist := 0; dist <= 4; dist++ {
		doc := <-output
		if _, found := doc.Edges[dist]; !found {
			t.Fatal(dist)
		}
	}
	doc := <-output
	if len(doc.Sts) != 2 {
		t.Fatal("Expected 2 STs")
	}
}

func TestHigherThreshold(t *testing.T) {
	testFile, err := os.Open("testdata/TestRequestHasHigherThreshold.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	progress := ProgressSinkHole()
	index, scores, maxThreshold, _, canReuseCache, err := parse(testFile, progress)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(scores.STs, []CgmlstSt{"a", "b", "d"}) {
		t.Fatal("STs")
	}
	if maxThreshold != 5 {
		t.Fatal("maxThreshold")
	}
	if canReuseCache {
		t.Fatal("Cannot reuse cache")
	}

	distances, err := scores.Distances()
	if err == nil {
		t.Fatal("Not got all the distances yet")
	}

	scoreComplete, errChan := scoreAll(&scores, index, progress)

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatal(err)
		}
	case <-scoreComplete:
	}

	distances, err = scores.Distances()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(distances, []int{1, 1, 0}) {
		t.Fatal(distances)
	}

	clusters, err := NewClusters(len(scores.STs), distances)
	if err != nil {
		t.Fatal(err)
	}
	output := clusters.Format(maxThreshold, distances, scores.STs)

	for dist := 0; dist <= 5; dist++ {
		doc := <-output
		if _, found := doc.Edges[dist]; !found {
			t.Fatal(dist)
		}
	}
	doc := <-output
	if len(doc.Sts) != 3 {
		t.Fatalf("%+v\n", doc)
	}
}

type MockWriter struct {
	maxPercent float32
	t          *testing.T
	silent     bool
}

func (w MockWriter) Write(data []byte) (n int, err error) {
	if !w.silent {
		fmt.Printf("%s", data)
	}
	return len(data), nil
}

func TestAll(t *testing.T) {
	testFile, err := os.Open("testdata/FakeProfiles.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	w := MockWriter{
		maxPercent: 0,
		t:          t,
		silent:     true,
	}
	_main(testFile, w)
}

func TestSmallDatasetWithoutCache(t *testing.T) {
	testFile, err := os.Open("testdata/SmallDatasetWithoutCache.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	w := MockWriter{
		maxPercent: 0,
		t:          t,
		silent:     true,
	}
	STs, clusters, distances := _main(testFile, w)
	if !reflect.DeepEqual(STs, []CgmlstSt{"A", "B", "C", "D", "E"}) {
		t.Fatal("Wrong STs")
	}
	if !reflect.DeepEqual(distances, []int{1, 5, 4, 3, 2, 1, 5, 5, 5, 4}) {
		t.Fatal("Wrong distances")
	}
	if !reflect.DeepEqual(clusters.pi, []int{1, 3, 3, 4, 4}) {
		t.Fatal("Wrong pi")
	}
	if !reflect.DeepEqual(clusters.lambda, []int{1, 2, 1, 4, 2147483647}) {
		t.Fatal("Wrong lambda")
	}
}

func TestSmallDatasetWithCache(t *testing.T) {
	testFile, err := os.Open("testdata/SmallDatasetWithCache.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	w := MockWriter{
		maxPercent: 0,
		t:          t,
		silent:     true,
	}
	STs, clusters, distances := _main(testFile, w)
	if !reflect.DeepEqual(STs, []CgmlstSt{"A", "B", "C", "D", "E"}) {
		t.Fatal("Wrong STs")
	}
	if !reflect.DeepEqual(distances, []int{1, 5, 4, 3, 2, 1, 5, 5, 5, 4}) {
		t.Fatal("Wrong distances")
	}
	if !reflect.DeepEqual(clusters.pi, []int{1, 3, 3, 4, 4}) {
		t.Fatal("Wrong pi")
	}
	if !reflect.DeepEqual(clusters.lambda, []int{1, 2, 1, 4, 2147483647}) {
		t.Fatal("Wrong lambda")
	}
}

func TestSmallDatasetWithReorderedCache(t *testing.T) {
	testFile, err := os.Open("testdata/SmallDatasetWithReorderedCache.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	w := MockWriter{
		maxPercent: 0,
		t:          t,
		silent:     true,
	}
	STs, clusters, distances := _main(testFile, w)
	if !reflect.DeepEqual(STs, []CgmlstSt{"A", "B", "D", "C", "E"}) {
		t.Fatal("Wrong STs")
	}
	if !reflect.DeepEqual(distances, []int{1, 3, 2, 5, 4, 1, 5, 5, 4, 5}) {
		t.Fatal("Wrong distances")
	}
	if !reflect.DeepEqual(clusters.pi, []int{1, 2, 3, 4, 4}) {
		t.Fatal("Wrong pi")
	}
	if !reflect.DeepEqual(clusters.lambda, []int{1, 2, 1, 4, 2147483647}) {
		t.Fatal("Wrong lambda")
	}
}

func TestSmallDatasetWithUnusedCache(t *testing.T) {
	testFile, err := os.Open("testdata/SmallDatasetWithUnusedCache.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	w := MockWriter{
		maxPercent: 0,
		t:          t,
		silent:     true,
	}
	STs, clusters, distances := _main(testFile, w)
	if !reflect.DeepEqual(STs, []CgmlstSt{"A", "C", "D", "E"}) {
		t.Fatal("Wrong STs")
	}
	if !reflect.DeepEqual(distances, []int{5, 3, 1, 5, 5, 4}) {
		t.Fatal("Wrong distances")
	}
	if !reflect.DeepEqual(clusters.pi, []int{2, 2, 4, 4}) {
		t.Fatal("Wrong pi")
	}
	if !reflect.DeepEqual(clusters.lambda, []int{3, 1, 4, 2147483647}) {
		t.Fatal("Wrong lambda")
	}
}
