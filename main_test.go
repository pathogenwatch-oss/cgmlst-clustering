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
}

func (w MockWriter) Write(data []byte) (n int, err error) {
	fmt.Printf("%s", data)
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
	}
	_main(testFile, w)
}
