package main

import (
	"io"
	"log"
	"os"
)

func main() {
	r := (io.Reader)(os.Stdin)
	fileIDs, profiles, scores, err := parse(r)
	if err != nil {
		panic(err)
	}

	if err := scoreAll(scores, profiles); err != nil {
		panic(err)
	}
	log.Printf("fileIDs: %d; Scores: %d\n", len(fileIDs), len(scores.scores))
}
