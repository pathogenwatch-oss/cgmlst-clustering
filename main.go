package main

import (
	"io"
	"log"
	"os"
	"sync"

	"github.com/pkg/bson"
)

type Profile struct {
	ID         string
	OrganismID string
	FileID     string
	Public     bool
	Version    string
	Matches    map[string]interface{}
}

type Index struct {
	Genes   *BitArray
	Alleles *BitArray
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
	lookup       map[string]Index
}

func NewIndexer() *Indexer {
	i := Indexer{
		geneTokens:   NewTokeniser(),
		alleleTokens: NewTokeniser(),
		lookup:       make(map[string]Index),
	}
	return &i
}

func (i *Indexer) Index(profile Profile) Index {
	defer i.Unlock()
	i.Lock()
	if index, ok := i.lookup[profile.FileID]; ok {
		return index
	}
	genesBa := NewBitArray(2500)
	var allelesBa *BitArray
	if i.alleleTokens.lastValue < 2500 {
		allelesBa = NewBitArray(2500)
	} else {
		allelesBa = NewBitArray(i.alleleTokens.lastValue)
	}
	for gene, allele := range profile.Matches {
		alleleHash := i.alleleTokens.Get(AlleleKey{
			gene,
			allele,
		})
		allelesBa.SetBit(alleleHash)
		geneHash := i.geneTokens.Get(AlleleKey{
			gene,
			nil,
		})
		genesBa.SetBit(geneHash)
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
	geneCount := CompareBits(indexA.Genes, indexB.Genes)
	alleleCount := CompareBits(indexA.Alleles, indexB.Alleles)
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

type dataBlob map[string]interface{}

func parseProfiles(r *io.Reader, profiles chan Profile, fileIDs chan string) {
	numWorkers := 4
	dec := bson.NewDecoder(*r)
	var wg sync.WaitGroup

	fileIDsSet := make(map[string]bool)
	var fileIDsLock sync.Mutex

	dataBlobs := make(chan dataBlob)
	go func() {
		for {
			d := make(dataBlob)
			if err := dec.Decode(&d); err == nil {
				dataBlobs <- d
			} else if err == io.EOF {
				close(dataBlobs)
				return
			} else {
				panic(err)
			}
		}
	}()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			for {
				if d, more := <-dataBlobs; more {
					p := parseProfile(d)
					fileIDsLock.Lock()
					if _, ok := fileIDsSet[p.FileID]; !ok {
						profiles <- p
						fileIDs <- p.FileID
						fileIDsSet[p.FileID] = true
					}
					fileIDsLock.Unlock()
				} else {
					wg.Done()
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(profiles)
		close(fileIDs)
	}()
}

func scoreAll(r io.Reader) scoresResult {
	numWorkers := 4

	profiles := make(chan Profile)
	fileIDsChan := make(chan string)
	fileIDs := []string{}
	indexer := NewIndexer()

	var wg sync.WaitGroup

	parseProfiles(&r, profiles, fileIDsChan)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go indexProfiles(profiles, indexer, &wg)
	}

	wg.Add(1)
	go func() {
		for i := 0; ; i++ {
			if f, more := <-fileIDsChan; more {
				fileIDs = append(fileIDs, f)
			} else {
				wg.Done()
				return
			}
		}
	}()

	wg.Wait()

	jobs := make(chan Job)
	scores := make(chan Score)
	matrix := make([]int, (len(fileIDs)*(len(fileIDs)-1))/2)
	scoresRemaining := len(matrix)

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go scoreProfiles(jobs, scores, Comparer{lookup: indexer.lookup}, &wg)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for scoresRemaining > 0 {
			s := <-scores
			matrix[s.Index] = s.Value
			scoresRemaining--
		}
		close(scores)
	}()

	scoreIndex := 0
	for i, fileIDA := range fileIDs[:len(fileIDs)-1] {
		for _, fileIDB := range fileIDs[i+1:] {
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
		fileIDs,
		matrix,
	}
}

func main() {
	scores := scoreAll(os.Stdin)
	log.Printf("fileIDs: %d; Scores: %d\n", len(scores.FileIDs), len(scores.Scores))
}
