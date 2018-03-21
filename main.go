package main

import (
	"fmt"
	"io"
	"math/bits"
	"os"

	"github.com/golang-collections/go-datastructures/bitarray"
	"github.com/pkg/bson"
)

type BitArray bitarray.BitArray

type CrapHashKey struct {
	Gene   string
	Allele interface{}
}

type Profile struct {
	ID         string
	OrganismID string
	FileID     string
	Public     bool
	Version    string
	Matches    map[string]interface{}
}

type Index struct {
	Genes   BitArray
	Alleles BitArray
}

func count(ba BitArray) int {
	iter := ba.Blocks()
	count := 0
	for {
		if ok := iter.Next(); ok {
			_, block := iter.Value()
			count = count + bits.OnesCount64(uint64(block))
		} else {
			break
		}
	}
	return count
}

var crapHashTable = make(map[interface{}]uint64)
var maxCrapHash uint64

func crapHash(input interface{}) uint64 {
	if hash, ok := crapHashTable[input]; ok {
		return hash
	}
	crapHashTable[input] = maxCrapHash
	maxCrapHash = maxCrapHash + 1
	return maxCrapHash - 1
}

var indexCache = make(map[string]Index)

func index(profile Profile) Index {
	if index, ok := indexCache[profile.FileID]; ok {
		return index
	}
	genesBa := bitarray.NewSparseBitArray()
	allelesBa := bitarray.NewSparseBitArray()
	for gene, allele := range profile.Matches {
		alleleHash := crapHash(CrapHashKey{
			Gene:   gene,
			Allele: allele,
		})
		allelesBa.SetBit(alleleHash)
		geneHash := crapHash(CrapHashKey{
			Gene:   gene,
			Allele: nil,
		})
		genesBa.SetBit(geneHash)
	}
	index := Index{
		Genes:   genesBa,
		Alleles: allelesBa,
	}
	indexCache[profile.FileID] = index
	return index
}

func compare(indexA Index, indexB Index) int {
	geneCount := count(indexA.Genes.And(indexB.Genes))
	alleleCount := count(indexA.Alleles.And(indexB.Alleles))
	return geneCount - alleleCount
}

func parseProfile(doc map[string]interface{}) Profile {
	return Profile{
		//ID:         fmt.Sprintf("%x", doc["_id"].(bson.ObjectId)),
		ID:         doc["_id"].(string),
		FileID:     doc["fileId"].(string),
		OrganismID: doc["organismId"].(string),
		Public:     doc["public"].(bool),
		Version:    doc["version"].(string),
		Matches:    doc["matches"].(map[string]interface{}),
	}
}

func main() {
	dec := bson.NewDecoder(os.Stdin)
	profiles := make([]Profile, 0)
	fileIds := make(map[string]bool)
	scores := make([]int, 0)

	for {
		d := make(map[string]interface{})

		if err := dec.Decode(&d); err != nil {
			if err != io.EOF {
				panic(err)
			}
			fmt.Println(len(scores))
			fmt.Println(len(indexCache))
			return
		}

		p := parseProfile(d)
		i := index(p)
		for _, otherP := range profiles {
			i2 := index(otherP)
			scores = append(scores, compare(i, i2))
		}

		profiles = append(profiles, p)
		fileIds[p.FileID] = true
		if len(fileIds)%100 == 0 {
			fmt.Println(len(fileIds))
		}
	}
}
