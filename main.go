package main

import (
	"bytes"
	"io"
	"log"
	"math/bits"
	"os"
	"sync"

	"github.com/golang-collections/go-datastructures/bitarray"
	"github.com/pkg/bson"
)

type BitArray bitarray.BitArray

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

type AlleleKey struct {
	Gene   string
	Allele interface{}
}

type Tokeniser struct {
	lookup   map[AlleleKey]uint64
	maxValue uint64
	mux      sync.Mutex
}

func (t *Tokeniser) Get(key AlleleKey) uint64 {
	t.mux.Lock()
	defer t.mux.Unlock()
	if hash, ok := t.lookup[key]; ok {
		return hash
	}
	t.lookup[key] = t.maxValue
	t.maxValue = t.maxValue + 1
	return t.maxValue - 1
}

type Indexer struct {
	tokens *Tokeniser
	lookup map[string]Index
	mux    sync.Mutex
}

func (i *Indexer) Index(profile Profile) Index {
	i.mux.Lock()
	defer i.mux.Unlock()
	if index, ok := i.lookup[profile.FileID]; ok {
		return index
	}
	genesBa := bitarray.NewSparseBitArray()
	allelesBa := bitarray.NewSparseBitArray()
	for gene, allele := range profile.Matches {
		alleleHash := i.tokens.Get(AlleleKey{
			Gene:   gene,
			Allele: allele,
		})
		if err := allelesBa.SetBit(alleleHash); err != nil {
			panic(err)
		}
		geneHash := i.tokens.Get(AlleleKey{
			Gene:   gene,
			Allele: nil,
		})
		if err := genesBa.SetBit(geneHash); err != nil {
			panic(err)
		}
	}
	index := Index{
		Genes:   genesBa,
		Alleles: allelesBa,
	}
	i.lookup[profile.FileID] = index
	return index
}

type Comparer struct {
	lookup map[string]Index
}

func (c *Comparer) compare(fileIDA string, fileIDB string) int {
	indexA, okA := c.lookup[fileIDA]
	indexB, okB := c.lookup[fileIDB]
	if !okA || !okB {
		panic("Missing index")
	}
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

type Job struct {
	FileIDA    string
	FileIDB    string
	ScoreIndex int
}

type Score struct {
	Value int
	Index int
}

func scoreProfiles(jobs chan Job, output chan Score, comparer Comparer, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		j, more := <-jobs
		if !more {
			return
		}
		score := comparer.compare(j.FileIDA, j.FileIDB)
		if j.ScoreIndex%100000 == 0 {
			log.Println(j.ScoreIndex)
		}
		output <- Score{
			Value: score,
			Index: j.ScoreIndex,
		}
	}
}

func indexProfiles(profiles chan Profile, indexer *Indexer, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		p, more := <-profiles
		if !more {
			return
		}
		indexer.Index(p)
	}
}

type scoresResult struct {
	FileIDs []string
	Scores  []int
}

func scoreAll(r io.Reader) scoresResult {
	numWorkers := 4
	dec := bson.NewDecoder(r)
	fileIds := make(map[string]bool)

	profiles := make(chan Profile)
	tokeniser := Tokeniser{
		lookup: make(map[AlleleKey]uint64),
	}
	indexer := Indexer{
		tokens: &tokeniser,
		lookup: make(map[string]Index),
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 1; i <= numWorkers; i++ {
		go indexProfiles(profiles, &indexer, &wg)
	}

	for {
		d := make(map[string]interface{})

		if err := dec.Decode(&d); err != nil {
			if err != io.EOF {
				panic(err)
			}
			close(profiles)
			break
		}

		p := parseProfile(d)
		if _, ok := fileIds[p.FileID]; !ok {
			profiles <- p
		}

		fileIds[p.FileID] = true
		if len(fileIds)%100 == 0 {
			log.Println(len(fileIds))
		}
	}
	wg.Wait()

	jobs := make(chan Job)
	scores := make(chan Score)
	matrix := make([]int, (len(fileIds)*(len(fileIds)-1))/2)
	scoresRemaining := len(matrix)

	wg.Add(numWorkers + 1)
	for i := 1; i <= numWorkers; i++ {
		go scoreProfiles(jobs, scores, Comparer{lookup: indexer.lookup}, &wg)
	}

	go func() {
		defer wg.Done()
		for scoresRemaining > 0 {
			s := <-scores
			matrix[s.Index] = s.Value
			scoresRemaining--
		}
		close(scores)
	}()

	fileIDList := make([]string, len(fileIds))
	i := 0
	for fileID := range fileIds {
		fileIDList[i] = fileID
		i++
	}
	scoreIndex := 0
	for i, fileIDA := range fileIDList[:len(fileIDList)-1] {
		for _, fileIDB := range fileIDList[i+1:] {
			jobs <- Job{
				FileIDA:    fileIDA,
				FileIDB:    fileIDB,
				ScoreIndex: scoreIndex,
			}
			scoreIndex++
		}
	}
	close(jobs)
	wg.Wait()
	return scoresResult{
		fileIDList,
		matrix,
	}
}

func loadFile(p string) (io.Reader, error) {
	file, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	filesize := fileinfo.Size()
	buffer := make([]byte, filesize)

	_, err = file.Read(buffer)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(buffer), nil
}

func main() {
	scores := scoreAll(os.Stdin)
	log.Printf("fileIDs: %d; Scores: %d\n", len(scores.FileIDs), len(scores.Scores))
}
