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
	Gene   string
	Allele interface{}
}

type Tokeniser struct {
	sync.Mutex
	lookup    map[AlleleKey]uint64
	nextValue chan uint64
	lastValue uint64
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

type Indexer struct {
	sync.Mutex
	geneTokens   *Tokeniser
	alleleTokens *Tokeniser
	indices      []Index
	lookup       map[CgmlstSt]int
	schemeSize   int32
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
		indices:      make([]Index, nSts),
		lookup:       lookup,
		schemeSize:   ALMOST_INF,
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
	if offset, ok = i.lookup[profile.ST]; !ok {
		return false, errors.New("Missing ST during indexing")
	}
	index = &i.indices[offset]
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
			gene,
			allele,
		})
		index.Alleles.SetBit(bit)
		bit := i.geneTokens.Get(AlleleKey{
			gene,
			nil,
		})
		index.Genes.SetBit(bit)
	}
	index.Ready = true
	if profile.schemeSize < i.schemeSize {
		i.schemeSize = profile.schemeSize
	}
	return false, nil
}

func (i *Indexer) Complete() error {
	for st, idx := range i.lookup {
		if !i.indices[idx].Ready {
			return fmt.Errorf("Didn't see a profile for ST '%s'", st)
		}
	}
	return nil
}
