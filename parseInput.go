package main

import (
	"github.com/goccy/go-json"
	"io"
	"sync"
)

const (
	PENDING    int = 0
	COMPLETE   int = 1
	FROM_CACHE int = 2
)

type CgmlstSt = string
type M = map[string]interface{}

type Request struct {
	STs       []CgmlstSt
	Threshold int
}

type Cache struct {
	sync.RWMutex
	Pi        []int
	Lambda    []int
	Sts       []string
	Threshold int
	Edges     map[int][][2]int
	nEdges    int
}

func NewCache() *Cache {
	c := Cache{Edges: make(map[int][][2]int), nEdges: 0}
	return &c
}

type Profile struct {
	ST         CgmlstSt
	Matches    M
	schemeSize int32
}

func parse(r io.Reader, progress chan ProgressEvent) (request Request, cache Cache, index *Indexer, err error) {
	decoder := json.NewDecoder(r)
	if requestErr := decoder.Decode(&request); requestErr != nil {
		err = requestErr
		return
	}

	progress <- ProgressEvent{PROFILES_EXPECTED, len(request.STs)}

	if cacheErr := decoder.Decode(&cache); cacheErr != nil {
		err = cacheErr
		return
	}

	index = NewIndexer(request.STs)

	for {
		var profile Profile
		if profileErr := decoder.Decode(&profile); profileErr != nil {
			if profileErr == io.EOF {
				break
			}
			err = profileErr
			return
		}
		duplicate, profileErr := index.Index(&profile)
		if profileErr == nil && !duplicate {
			//nProfiles++
			progress <- ProgressEvent{PROFILE_PARSED, 1}
		}
	}
	return
}
