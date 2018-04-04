package main

import (
	"os"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func TestCustomParse(t *testing.T) {
	testFile, err := os.Open("testdata/genomes.bson")
	if err != nil {
		t.Fatal("Couldn't load test data")
	}
	errc := make(chan error)
	c := readBsonDocs(testFile, errc)
	doc := <-c
	var p ProfileDoc
	if err := Unmarshal(doc, &p); err != nil {
		t.Fatal(err)
	}
	if p.FileID != "1b5be653a771dd81b5d9c290a6ac1c17f8999c97" {
		t.Fatal("Wrong fileID")
	}
	if p.OrganismID != "1280" {
		t.Fatal("Wrong organismId")
	}
	// t.Fatal(len(p.Analysis.CgMlst.Matches))
	if len(p.Analysis.CgMlst.Matches) != 2226 {
		t.Fatalf("Wrong number of matches %d", len(p.Analysis.CgMlst.Matches))
	}
}

func BenchmarkCustomParse(b *testing.B) {
	testFile, err := os.Open("testdata/genomes.bson")
	if err != nil {
		b.Fatal("Couldn't load test data")
	}
	errc := make(chan error)
	c := readBsonDocs(testFile, errc)
	doc := <-c
	var p ProfileDoc
	if err := Unmarshal(doc, &p); err != nil {
		b.Fatal(err)
	}
	if p.FileID != "1b5be653a771dd81b5d9c290a6ac1c17f8999c97" {
		b.Fatal("Wrong fileId")
	}
	if p.OrganismID != "1280" {
		b.Fatal("Wrong organismId")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p1 ProfileDoc
		if err := Unmarshal(doc, &p1); err != nil {
			b.Fatal(err)
		}
	}
}
