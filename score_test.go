package main

import (
	"github.com/RoaringBitmap/gocroaring"
	"reflect"
	"testing"
)

func TestComparer_compare(t *testing.T) {

	indices := make([]BitProfiles, 4)
	allPresent := NewBitArray(10)
	for i := 0; i < 10; i++ {
		allPresent.SetBit(uint64(i))
	}
	oneGap := NewBitArray(10)
	for i := 0; i < 10; i++ {
		if i == 5 {
			continue
		}
		oneGap.SetBit(uint64(i))
	}
	indices[0] = BitProfiles{
		Genes:   allPresent,
		Alleles: gocroaring.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
		Ready:   true,
	}
	indices[1] = BitProfiles{
		Genes:   allPresent,
		Alleles: gocroaring.New(0, 2, 3, 4, 5, 6, 7, 8, 9, 10),
		Ready:   true,
	}
	indices[2] = BitProfiles{
		Genes:   oneGap,
		Alleles: gocroaring.New(0, 1, 2, 3, 4, 6, 7, 8, 9),
		Ready:   true,
	}
	indices[3] = BitProfiles{
		Genes:   oneGap,
		Alleles: gocroaring.New(0, 2, 3, 4, 7, 8, 9, 10, 11),
		Ready:   false,
	}

	profileMap := ProfilesMap{
		indices:    indices,
		schemeSize: 10,
		lookup: map[CgmlstSt]int{
			"a": 0,
			"b": 1,
			"c": 2,
			"d": 3,
		},
	}
	c := &Comparer{
		profilesMap:      profileMap,
		minMatchingGenes: 8,
	}
	type args struct {
		stA int
		stB int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Identity",
			args: args{
				stA: 0,
				stB: 0,
			},
			want: 0,
		},
		{
			name: "1 difference",
			args: args{
				stA: 0,
				stB: 1,
			},
			want: 1,
		},
		{
			name: "1 gap",
			args: args{
				stA: 0,
				stB: 2,
			},
			want: 0,
		},
		{
			name: "2 differences 1 gap",
			args: args{
				stA: 0,
				stB: 3,
			},
			want: 2,
		},
		{
			name: "2 difference 2x1 gap",
			args: args{
				stA: 2,
				stB: 3,
			},
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.compare(tt.args.stA, tt.args.stB); got != tt.want {
				t.Errorf("compare() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSortSts(t *testing.T) {
	request := Request{
		STs:       []CgmlstSt{"a", "b", "c"},
		Threshold: 1,
	}
	cache := Cache{
		Sts:       []CgmlstSt{},
		Edges:     map[int][][2]int{0: {}, 1: {}},
		Threshold: 1,
		nEdges:    2,
	}
	index := ProfilesMap{
		lookup:  map[string]int{"a": 0, "b": 1, "c": 2},
		indices: []BitProfiles{{Ready: true}, {Ready: true}, {Ready: true}},
	}
	canReuseCache, STs, cacheToScoresMap := sortSts(request.STs, &cache, &index)

	expected := []CgmlstSt{"a", "b", "c"}
	if !reflect.DeepEqual(STs, expected) {
		t.Fatalf("Expected %v, got %v\n", expected, STs)
	}
	if canReuseCache {
		t.Fatal("Wrong")
	}
	if len(cacheToScoresMap) != 0 {
		t.Fatal("Wrong")
	}

	cache.Sts = []CgmlstSt{"b", "a"}
	cache.Pi = []int{0, 0}
	cache.Lambda = []int{0, 0}
	canReuseCache, STs, cacheToScoresMap = sortSts(request.STs, &cache, &index)
	expected = []CgmlstSt{"b", "a", "c"}
	if !reflect.DeepEqual(STs, expected) {
		t.Fatalf("Expected %v, got %v\n", expected, STs)
	}
	if !canReuseCache {
		t.Fatal("Wrong")
	}
	if !reflect.DeepEqual(cacheToScoresMap, []int{0, 1}) {
		t.Fatalf("Didn't expect %v\n", cacheToScoresMap)
	}

	cache.Sts = []CgmlstSt{"b", "d", "a"}
	cache.Pi = []int{0, 0, 0}
	cache.Lambda = []int{0, 0, 0}
	canReuseCache, STs, cacheToScoresMap = sortSts(request.STs, &cache, &index)
	expected = []CgmlstSt{"b", "a", "c"}
	if !reflect.DeepEqual(STs, expected) {
		t.Fatalf("Expected %v, got %v\n", expected, STs)
	}
	if canReuseCache {
		t.Fatal("Wrong")
	}
	if !reflect.DeepEqual(cacheToScoresMap, []int{0, -1, 1}) {
		t.Fatalf("Didn't expect %v\n", cacheToScoresMap)
	}

	cache.Sts = []CgmlstSt{"b", "a", "b"}
	cache.Pi = []int{0, 0, 0}
	cache.Lambda = []int{0, 0, 0}
	request.STs = []CgmlstSt{"c", "a", "b", "c"}
	canReuseCache, STs, cacheToScoresMap = sortSts(request.STs, &cache, &index)
	expected = []CgmlstSt{"b", "a", "c"}
	if !reflect.DeepEqual(STs, expected) {
		t.Fatalf("Expected %v, got %v\n", expected, STs)
	}
	if canReuseCache {
		t.Fatal("Wrong")
	}
	if !reflect.DeepEqual(cacheToScoresMap, []int{0, 1, 0}) {
		t.Fatalf("Didn't expect %v\n", cacheToScoresMap)
	}
}

//func TestParseCacheScores(t *testing.T) {
//	testFile, err := os.Open("testdata/TestParseCache.bson")
//	if err != nil {
//		t.Fatal("Couldn't load test data")
//	}
//
//	docs := bsonkit.GetDocuments(testFile)
//	docs.Next()
//	if docs.Err != nil {
//		t.Fatal(docs.Err)
//	}
//	doc := docs.Doc
//
//	cache := NewCache()
//	if err = cache.Update(doc, 5); err != nil {
//		t.Fatal(err)
//	}
//
//	request := Request{
//		STs: []CgmlstSt{"a", "b", "d", "e"},
//	}
//	index := Indexer{
//		lookup:  map[string]int{"a": 0, "b": 1, "d": 2, "e": 3},
//		profilesMap: []BitProfiles{BitProfiles{Ready: true}, BitProfiles{Ready: true}, BitProfiles{Ready: true}, BitProfiles{Ready: true}},
//	}
//
//	scores, err := NewScores(request, cache, &index)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	expectedValues := []int{5, ALMOST_INF, 1, -1, -1, -1}
//	for i, v := range expectedValues {
//		if scores.scores[i].value != v {
//			t.Fatal(i, v, scores.scores[i].value)
//		}
//	}
//
//	expectedStatuses := []int{FROM_CACHE, FROM_CACHE, FROM_CACHE, PENDING, PENDING, PENDING}
//	for i, v := range expectedStatuses {
//		if scores.scores[i].status != v {
//			t.Fatal(i, v, scores.scores[i].status)
//		}
//	}
//}
//
//func BenchmarkScores(b *testing.B) {
//	request := Request{
//		STs: make([]CgmlstSt, 1000),
//	}
//	cache := Cache{
//		Sts: []CgmlstSt{},
//	}
//	index := Indexer{
//		lookup:  make(map[CgmlstSt]int),
//		profilesMap: make([]BitProfiles, 1000),
//	}
//	for i := 0; i < 1000; i++ {
//		st := fmt.Sprintf("st%d", i)
//		request.STs[i] = st
//		index.lookup[st] = i
//		index.profilesMap[i].Ready = true
//	}
//
//	b.ResetTimer()
//	for iter := 0; iter < b.N; iter++ {
//		scores, err := NewScores(request, &cache, &index)
//		if err != nil {
//			b.Fatal(err)
//		}
//		for a := 1; a < len(request.STs); a++ {
//			for b := 0; b < a; b++ {
//				scores.Set(a, b, 0, PENDING)
//			}
//		}
//	}
//}
//

//
//func TestGetIndex(t *testing.T) {
//	request := Request{
//		STs: []CgmlstSt{"st1", "st2", "st3", "st4"},
//	}
//	cache := Cache{
//		Sts: []CgmlstSt{},
//	}
//	index := Indexer{
//		profilesMap: make([]BitProfiles, len(request.STs)),
//		lookup:  make(map[CgmlstSt]int),
//	}
//	for i, st := range request.STs {
//		index.lookup[st] = i
//		index.profilesMap[i].Ready = true
//	}
//
//	scores, err := NewScores(request, &cache, &index)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	testCases := []struct {
//		a, b int
//	}{
//		{1, 0},
//		{2, 0},
//		{2, 1},
//		{3, 0},
//		{3, 1},
//		{3, 2},
//	}
//
//	for i, tc := range testCases {
//		if v, err := scores.getIndex(tc.a, tc.b); err != nil {
//			t.Fatal(err)
//		} else if i != v {
//			t.Fatalf("Expected %d, got %d\n", i, v)
//		}
//		if v, err := scores.getIndex(tc.b, tc.a); err != nil {
//			t.Fatal(err)
//		} else if i != v {
//			t.Fatalf("Expected %d, got %d\n", i, v)
//		}
//	}
//}
//
//func TestGetScore(t *testing.T) {
//	request := Request{
//		STs: []CgmlstSt{"st1", "st2", "st3", "st4"},
//	}
//	cache := Cache{
//		Sts: []CgmlstSt{},
//	}
//	index := Indexer{
//		profilesMap: make([]BitProfiles, len(request.STs)),
//		lookup:  make(map[CgmlstSt]int),
//	}
//	for i, st := range request.STs {
//		index.lookup[st] = i
//		index.profilesMap[i].Ready = true
//	}
//
//	scores, err := NewScores(request, &cache, &index)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	testCases := []struct {
//		a, b int
//	}{
//		{1, 0},
//		{2, 0},
//		{2, 1},
//		{3, 0},
//		{3, 1},
//		{3, 2},
//	}
//
//	for _, tc := range testCases {
//		if v, err := scores.Get(tc.a, tc.b); err != nil {
//			t.Fatal(err)
//		} else if v.stA != tc.a || v.stB != tc.b {
//			t.Fatalf("Expected %v, got %v\n", tc, v)
//		}
//		if v, err := scores.Get(tc.b, tc.a); err != nil {
//			t.Fatal(err)
//		} else if v.stA != tc.a || v.stB != tc.b {
//			t.Fatalf("Expected %v, got %v\n", tc, v)
//		}
//	}
//}
//
//func TestComparer(t *testing.T) {
//	profilesMap := [...]Profile{
//		Profile{
//			ST: "abc123",
//			Matches: map[string]interface{}{
//				"gene1": 1,
//				"gene2": 1,
//				"gene3": 1,
//			},
//		},
//		Profile{
//			ST: "bcd234",
//			Matches: map[string]interface{}{
//				"gene1": 2,
//				"gene2": 2,
//				"gene4": 1,
//			},
//		},
//		Profile{
//			ST: "cde345",
//			Matches: map[string]interface{}{
//				"gene1": 1,
//				"gene2": 2,
//				"gene3": 1,
//				"gene4": 1,
//			},
//		},
//	}
//
//	indexer := NewIndexer([]string{"abc123", "bcd234", "cde345"})
//	for i, p := range profilesMap {
//		indexer.BitProfiles(&p)
//		for j := 0; j < 10000; j++ {
//			indexer.alleleTokens.Get(AlleleKey{
//				fmt.Sprintf("fake-%d", i),
//				j,
//			})
//		}
//	}
//
//	if nBlocks := len(indexer.profilesMap[indexer.lookup["bcd234"]].Alleles.blocks); nBlocks != 157 {
//		t.Fatalf("Expected 157 blocks, got %d\n", nBlocks)
//	}
//	if nBlocks := len(indexer.profilesMap[indexer.lookup["cde345"]].Alleles.blocks); nBlocks != 313 {
//		t.Fatalf("Expected 313 blocks, got %d\n", nBlocks)
//	}
//
//	comparer := Comparer{indexer.profilesMap, 3}
//	if value := comparer.compare(0, 1); value != ALMOST_INF {
//		t.Fatalf("Expected %d, got %d\n", ALMOST_INF, value)
//	}
//	if value := comparer.compare(0, 2); value != 1 {
//		t.Fatalf("Expected 1, got %d\n", value)
//	}
//	if value := comparer.compare(1, 2); value != 1 {
//		t.Fatalf("Expected 1, got %d\n", value)
//	}
//}
//
//func TestScoreAll(t *testing.T) {
//	testProfiles := [...]Profile{
//		Profile{
//			ST: "abc123",
//			Matches: map[string]interface{}{
//				"gene1": 1,
//				"gene2": 1,
//				"gene3": 1,
//			},
//		},
//		Profile{
//			ST: "bcd234",
//			Matches: map[string]interface{}{
//				"gene1": 2,
//				"gene2": 2,
//				"gene4": 1,
//			},
//		},
//		Profile{
//			ST: "cde345",
//			Matches: map[string]interface{}{
//				"gene1": 1,
//				"gene2": 2,
//				"gene3": 1,
//				"gene4": 1,
//			},
//		},
//	}
//
//	request := Request{
//		STs: []CgmlstSt{"abc123", "bcd234", "cde345"},
//	}
//	cache := Cache{
//		Sts: []CgmlstSt{},
//	}
//
//	indexer := NewIndexer(request.STs)
//	for _, p := range testProfiles {
//		indexer.BitProfiles(&p)
//	}
//
//	scores, err := NewScores(request, &cache, indexer)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	scoreComplete, errChan := scores.RunScoring(indexer, ProgressSinkHole())
//	select {
//	case err := <-errChan:
//		if err != nil {
//			t.Fatal(err)
//		}
//	case <-scoreComplete:
//	}
//
//	nSTs := len(scores.STs)
//	if nSTs != len(testProfiles) {
//		t.Fatal("Not enough STs")
//	}
//	if len(scores.scores) != nSTs*(nSTs-1)/2 {
//		t.Fatal("Not enough scores")
//	}
//	expectedScores := []int{2, 1, 1}
//	for i, score := range expectedScores {
//		if score != scores.scores[i].value {
//			t.Fatalf("Score %d was %v should be %d\n", i, scores.scores[i], score)
//		}
//	}
//}
//
//func TestScoreAllFakeData(t *testing.T) {
//	testFile, err := os.Open("testdata/FakeProfiles.bson")
//	if err != nil {
//		t.Fatal("Couldn't load test data")
//	}
//
//	request, cache, index, err := parse(testFile, ProgressSinkHole())
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	scores, err := NewScores(request, cache, index)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	scoreComplete, errChan := scores.RunScoring(index, ProgressSinkHole())
//	select {
//	case err := <-errChan:
//		if err != nil {
//			t.Fatal(err)
//		}
//	case <-scoreComplete:
//	}
//
//	nSTs := len(scores.STs)
//	nScores := len(scores.scores)
//	if nSTs != 7000 {
//		t.Fatal("Expected some STs")
//	}
//	if nScores != nSTs*(nSTs-1)/2 {
//		t.Fatal("Expected some scores")
//	}
//
//	for _, s := range scores.scores {
//		if s.status == PENDING {
//			t.Fatalf("Expected all scores to be complete: %v", s)
//		}
//	}
//
//	distances, err := scores.Distances()
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	clusters, err := ClusterFromScratch(distances, len(scores.STs))
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	thresholds := []int{10, 50, 100, 200}
//	for _, threshold := range thresholds {
//		c := clusters.Get(threshold)
//		log.Println(threshold, countClusters(c))
//	}
//}

func TestNewScores(t *testing.T) {

	cache := Cache{
		Sts:       []CgmlstSt{"1", "2", "5", "6"},
		Lambda:    make([]int, 4),
		Pi:        make([]int, 4),
		Threshold: 5,
		Edges: map[int][][2]int{
			0: {},
			1: {},
			2: {},
			3: {},
			4: {{0, 1}},
			5: {{0, 2}, {1, 2}},
		},
	}
	genes := NewBitArray(6)
	var i uint64
	for i = 0; i < 6; i++ {
		genes.SetBit(i)
	}

	profiles := ProfilesMap{
		lookup: map[string]int{"1": 0, "2": 1, "3": 2, "4": 3, "5": 4, "6": 5},
		indices: []BitProfiles{
			{Genes: genes, Alleles: gocroaring.New(1, 2, 3, 4, 5, 6), Ready: true},
			{Genes: genes, Alleles: gocroaring.New(7, 8, 9, 10, 5, 6), Ready: true},
			{Genes: genes, Alleles: gocroaring.New(12, 13, 13, 15, 16, 6), Ready: true},
			{Genes: genes, Alleles: gocroaring.New(1, 8, 9, 10, 11, 6), Ready: true},
			{Genes: genes, Alleles: gocroaring.New(7, 8, 9, 10, 5, 6), Ready: true},
			{Genes: genes, Alleles: gocroaring.New(100, 200, 300, 400, 500, 600), Ready: true},
		},
		schemeSize: 6,
	}

	request := Request{STs: []CgmlstSt{"1", "2", "3", "4", "5", "6"}, Threshold: 5}

	type args struct {
		request  Request
		cache    *Cache
		profiles *ProfilesMap
	}

	tests := []struct {
		name    string
		args    args
		wantS   ScoresStore
		wantErr bool
	}{
		{"TestCache",
			args{request, &cache, &profiles},
			ScoresStore{STs: []CgmlstSt{"1", "2", "5", "6", "3", "4"}, scores: []int{4, 5, 5, 2147483647, 2147483647, 2147483647, -1, -1, -1, -1, -1, -1, -1, -1, -1}, todo: 9, canReuseCache: true},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotS, err := NewScores(tt.args.request, tt.args.cache, tt.args.profiles)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewScores() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotS, tt.wantS) {
				t.Errorf("NewScores() gotS = %v, want %v", gotS, tt.wantS)
			}
		})
	}
}
