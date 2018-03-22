package main

import (
	"testing"
)

func BenchmarkScoreAll(b *testing.B) {
	profiles, err := loadFile("../private_staph.2.bson")
	if err != nil {
		b.Fatal("Couldn't open test file")
	}
	scores := scoreAll(profiles)
	nFileIds := len(scores.FileIDs)
	nScores := len(scores.Scores)
	if nFileIds < 2 {
		b.Fatal("Expected some fileIds")
	}
	if nScores != nFileIds*(nFileIds-1)/2 {
		b.Fatal("Expected some fileIds")
	}
}
