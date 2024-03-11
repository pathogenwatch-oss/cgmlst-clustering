package main

import (
	"github.com/goccy/go-json"
	"io"
	"sync"
)

const (
	PENDING  int = 0
	COMPLETE int = 1
)

type CgmlstSt = string

type Request struct {
	STs       []CgmlstSt
	Threshold int
}

type Cache struct {
	Edges     map[int][][2]int
	Pi        []int
	Lambda    []int
	Sts       []string
	Threshold int
	nEdges    int
	sync.RWMutex
}

func NewCache() *Cache {
	c := Cache{Edges: make(map[int][][2]int), nEdges: 0}
	return &c
}

type Profile struct {
	ST         CgmlstSt
	Matches    []string
	schemeSize uint32
}

func indexProfile(profile *Profile, index *Indexer, progress chan ProgressEvent) {
	duplicate, profileErr := index.Index(profile)
	if profileErr == nil && !duplicate {
		progress <- ProgressEvent{PROFILE_PARSED, 1}
	}
}

func parse(r io.Reader, progress chan ProgressEvent) (request Request, cache Cache, index *IndexMap, err error) {
	err = nil
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

	var indexer = NewIndexer(request.STs)

	for {
		var profile Profile
		if profileErr := decoder.Decode(&profile); profileErr != nil {
			if profileErr == io.EOF {
				break
			}
			err = profileErr
			return
		}
		indexProfile(&profile, indexer, progress)
	}
	index = indexer.index

	return
}
