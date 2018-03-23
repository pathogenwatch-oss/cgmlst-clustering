package main

import (
	"os"
	"sync"
	"testing"
)

func BenchmarkConcurrentArrayUpdate(b *testing.B) {
	array := make([]int, b.N)
	indexes := make(chan int)
	go func() {
		for i := 0; i < b.N; i++ {
			indexes <- i
		}
		close(indexes)
	}()
	var wg sync.WaitGroup
	for worker := 0; worker < 100; worker++ {
		wg.Add(1)
		go func() {
			for {
				if i, more := <-indexes; !more {
					wg.Done()
					return
				} else {
					array[i] = i * i
				}
			}
		}()
	}
	wg.Wait()
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
	profiles, err := os.Open("../all_staph.bson")
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
