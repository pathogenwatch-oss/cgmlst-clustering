package main

import (
	"errors"
	"fmt"
	"sync"
)

type Index struct {
	Genes   *BitArray
	Alleles *BitArray
	Ready   bool
}

type AlleleKey struct {
	Allele interface{}
	Gene   int
}

type Tokeniser struct {
	lookup    map[AlleleKey]uint64
	nextValue chan uint64
	lastValue uint64
	sync.Mutex
}

func NewTokeniser() *Tokeniser {
	t := Tokeniser{
		nextValue: make(chan uint64),
		lookup:    make(map[AlleleKey]uint64),
	}
	go func() {
		var i uint64
		for i = 0; ; i++ {
			t.nextValue <- i
		}
	}()
	return &t
}

func (t *Tokeniser) Get(key AlleleKey) uint64 {
	t.Lock()
	defer t.Unlock()
	if value, ok := t.lookup[key]; ok {
		return value
	}
	value := <-t.nextValue
	t.lookup[key] = value
	t.lastValue = value
	return value
}

type IndexMap struct {
	lookup     map[CgmlstSt]int
	indices    []Index
	schemeSize int32
}

type Indexer struct {
	geneTokens   *Tokeniser
	alleleTokens *Tokeniser
	index        *IndexMap
	sync.Mutex
}

func NewIndexer(STs []CgmlstSt) (i *Indexer) {
	nSts := len(STs)
	lookup := make(map[CgmlstSt]int)
	for idx, st := range STs {
		lookup[st] = idx
	}
	return &Indexer{
		geneTokens:   NewTokeniser(),
		alleleTokens: NewTokeniser(),
		index: &IndexMap{
			indices:    make([]Index, nSts),
			lookup:     lookup,
			schemeSize: ALMOST_INF,
		},
	}
}

// Index returns true if already indexed
func (i *Indexer) Index(profile *Profile) (bool, error) {
	var (
		offset int
		ok     bool
		index  *Index
	)

	defer i.Unlock()
	i.Lock()
	if offset, ok = i.index.lookup[profile.ST]; !ok {
		return false, errors.New("Missing ST during indexing")
	}
	index = &i.index.indices[offset]
	if index.Ready {
		return true, nil
	}

	index.Genes = NewBitArray(2500)
	if i.alleleTokens.lastValue < 2500 {
		index.Alleles = NewBitArray(2500)
	} else {
		index.Alleles = NewBitArray(i.alleleTokens.lastValue)
	}
	var bit uint64
	for gene, allele := range profile.Matches {
		bit = i.alleleTokens.Get(AlleleKey{
			allele,
			gene,
		})
		index.Alleles.SetBit(bit)
		bit := i.geneTokens.Get(AlleleKey{
			nil,
			gene,
		})
		index.Genes.SetBit(bit)
	}
	index.Ready = true
	if profile.schemeSize < i.index.schemeSize {
		i.index.schemeSize = profile.schemeSize
	}
	return false, nil
}

func (i *IndexMap) Complete() error {
	for st, idx := range i.lookup {
		if !i.indices[idx].Ready {
			return fmt.Errorf("Didn't see a profile for ST '%s'", st)
		}
	}
	return nil
}
