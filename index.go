package main

import (
	"errors"
	"fmt"
	"github.com/RoaringBitmap/gocroaring"
)

type Index struct {
	Genes   *BitArray
	Alleles *gocroaring.Bitmap
	Ready   bool
}

type AlleleKey struct {
	Allele interface{}
	Gene   int
}

type Tokeniser struct {
	lookup    map[AlleleKey]uint32
	nextValue chan uint32
	lastValue uint32
}

func NewTokeniser() *Tokeniser {
	t := Tokeniser{
		nextValue: make(chan uint32),
		lookup:    make(map[AlleleKey]uint32),
	}
	go func() {
		var i uint32
		for i = 0; ; i++ {
			t.nextValue <- i
		}
	}()
	return &t
}

func (t *Tokeniser) Get(key AlleleKey) uint32 {
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
	schemeSize uint32
}

type Indexer struct {
	geneTokens   *Tokeniser
	alleleTokens *Tokeniser
	index        *IndexMap
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

	if offset, ok = i.index.lookup[profile.ST]; !ok {
		return false, errors.New("Missing ST during indexing")
	}
	index = &i.index.indices[offset]
	if index.Ready {
		return true, nil
	}
	index.Genes = NewBitArray(2500)
	index.Alleles = gocroaring.New()

	var bit uint32
	for gene, allele := range profile.Matches {
		if allele == "" {
			continue
		}
		bit = i.alleleTokens.Get(AlleleKey{
			allele,
			gene,
		})
		index.Alleles.Add(bit)
		bit := i.geneTokens.Get(AlleleKey{
			nil,
			gene,
		})
		index.Genes.SetBit(uint64(bit))
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
			return fmt.Errorf("didn't see a profile for ST '%s'", st)
		}
	}
	return nil
}
