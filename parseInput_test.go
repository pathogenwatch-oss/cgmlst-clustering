package main

//func TestParseRequestDoc(t *testing.T) {
//	testFile, err := os.Open("testdata/TestParseRequestDoc.bson")
//	if err != nil {
//		t.Fatal("Couldn't load test data")
//	}
//	docs := bsonkit.GetDocuments(testFile)
//
//	docs.Next()
//	if docs.Err != nil {
//		t.Fatal(docs.Err)
//	}
//	doc := docs.Doc
//
//	if request, err := parseRequestDoc(doc); err != nil {
//		t.Fatal(err)
//	} else if len(request.STs) != 3 {
//		t.Fatal("Expected 3 STs got", request.STs)
//	} else if request.Threshold != 50 {
//		t.Fatalf("Expected %v got %v\n", 50, request.Threshold)
//	} else {
//		expected := []string{"abc", "def", "ghi"}
//		if !reflect.DeepEqual(request.STs, expected) {
//			t.Fatalf("Expected %v got %v\n", expected, request.STs)
//		}
//	}
//
//	// This has a duplicate ST
//	docs.Next()
//	if docs.Err != nil {
//		t.Fatal(docs.Err)
//	}
//	doc = docs.Doc
//
//	if request, err := parseRequestDoc(doc); err != nil {
//		t.Fatal(err)
//	} else if len(request.STs) != 3 {
//		t.Fatal("Expected 3 STs")
//	} else if request.Threshold != 50 {
//		t.Fatalf("Expected %v got %v\n", 50, request.Threshold)
//	} else {
//		expected := []string{"abc", "abc", "ghi"}
//		if !reflect.DeepEqual(request.STs, expected) {
//			t.Fatalf("Expected %v got %v\n", expected, request.STs)
//		}
//	}
//
//	// This doesn't have a ST
//	docs.Next()
//	if docs.Err != nil {
//		t.Fatal(docs.Err)
//	}
//	doc = docs.Doc
//
//	if _, err := parseRequestDoc(doc); err == nil {
//		t.Fatal("This doesn't have a ST. Should have thrown an error")
//	}
//
//	// Doesn't have a maxThresholds key
//	docs.Next()
//	if docs.Err != nil {
//		t.Fatal(docs.Err)
//	}
//	doc = docs.Doc
//
//	if _, err := parseRequestDoc(doc); err == nil {
//		t.Fatal("Doesn't have a thresholds key. Should have thrown an error")
//	}
//
//	if docs.Next() {
//		t.Fatal("Unexpected extra document")
//	}
//}
//
//func TestParseCache(t *testing.T) {
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
//	expected := []int{2, 3, 3, 3}
//	if !reflect.DeepEqual(cache.Pi, expected) {
//		t.Fatalf("Expected %v, got %v", expected, cache.Pi)
//	}
//
//	expected = []int{1, 1, 2, ALMOST_INF}
//	if !reflect.DeepEqual(cache.Lambda, expected) {
//		t.Fatalf("Expected %v, got %v", expected, cache.Lambda)
//	}
//
//	expectedStrings := []string{"a", "b", "c", "d"}
//	if !reflect.DeepEqual(cache.Sts, expectedStrings) {
//		t.Fatalf("Expected %v, got %v", expectedStrings, cache.Sts)
//	}
//
//	if cache.Threshold != 5 {
//		t.Fatalf("Expected 5, got %v", cache.Threshold)
//	}
//
//	if docs.Next() {
//		t.Fatal("Unexpected document")
//	}
//}
//
//func TestParseProfile(t *testing.T) {
//	testFile, err := os.Open("testdata/TestUpdateProfiles.bson")
//	if err != nil {
//		t.Fatal("Couldn't load test data")
//	}
//
//	index := NewIndexer([]string{"def", "abc"})
//
//	docs := bsonkit.GetDocuments(testFile)
//	docs.Next()
//	if docs.Err != nil {
//		t.Fatal(docs.Err)
//	}
//
//	if _, err := parseAndIndex(docs.Doc, index); err != nil {
//		t.Fatal(err)
//	}
//
//	i := index.indices[index.lookup["abc"]]
//	if !i.Ready {
//		t.Fatal("Profile not in index")
//	}
//}
//
//func ProgressSinkHole() chan ProgressEvent {
//	hole := make(chan ProgressEvent)
//	go func() {
//		for range hole {
//		}
//	}()
//	return hole
//}
//
//func TestParse(t *testing.T) {
//	testFile, err := os.Open("testdata/TestParse.bson")
//	if err != nil {
//		t.Fatal("Couldn't load test data")
//	}
//	request, cache, index, err := parse(testFile, ProgressSinkHole())
//	if err != nil {
//		t.Fatal(err)
//	}
//	if err = index.Complete(); err == nil {
//		missing := make([]CgmlstSt, 0, len(request.STs))
//		for st, idx := range index.lookup {
//			if !index.indices[idx].Ready {
//				missing = append(missing, st)
//			}
//		}
//		t.Fatalf("Didn't supply all the required profiles\nMissing: %v\nRequested: %v\n", missing, request.STs)
//	}
//
//	var scores scoresStore
//	if scores, err = NewScores(request, cache, index); err != nil {
//		t.Fatal(err)
//	}
//
//	if scores.canReuseCache != true {
//		t.Fatal("Expected true")
//	}
//	if len(scores.STs) != 5 {
//		t.Fatalf("Expected 5 STs, got %v", scores.STs)
//	}
//	expected := []CgmlstSt{"a", "b", "c", "d", "e"}
//	if !reflect.DeepEqual(expected, scores.STs) {
//		t.Fatalf("Got %v\n", scores.STs)
//	}
//	nProfiles := 0
//	for _, i := range index.indices {
//		if i.Ready {
//			nProfiles++
//		}
//	}
//	if nProfiles != 2 {
//		t.Fatalf("Expected 2 profiles, got %v\n", nProfiles)
//	}
//	if len(scores.scores) != 10 {
//		t.Fatal("Expected 10 scores")
//	}
//	if request.Threshold != 5 {
//		t.Fatalf("Expected 50, got %v\n", request.Threshold)
//	}
//	if scores.scores[0].value != 5 {
//		t.Fatalf("Got %v", scores.scores[0])
//	}
//	if scores.scores[3].value != ALMOST_INF {
//		t.Fatalf("Got %v", scores.scores[3])
//	}
//	if scores.scores[6].status != PENDING {
//		t.Fatalf("Got %v", scores.scores[6])
//	}
//}
//
//func TestParseNoCache(t *testing.T) {
//	testFile, err := os.Open("testdata/TestParseNoCache.bson")
//	if err != nil {
//		t.Fatal("Couldn't load test data")
//	}
//	request, cache, index, err := parse(testFile, ProgressSinkHole())
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	var scores scoresStore
//	if scores, err = NewScores(request, cache, index); err != nil {
//		t.Fatal(err)
//	}
//	if scores.canReuseCache == true {
//		t.Fatal("Expected false")
//	}
//	if len(scores.STs) != 5 {
//		t.Fatalf("Expected 5 STs, got %v", scores.STs)
//	}
//	expected := []CgmlstSt{"a", "e", "b", "c", "d"}
//	if !reflect.DeepEqual(expected, scores.STs) {
//		t.Fatalf("Got %v\n", scores.STs)
//	}
//	nProfiles := 0
//	for _, i := range index.indices {
//		if i.Ready {
//			nProfiles++
//		}
//	}
//	if nProfiles != 2 {
//		t.Fatalf("Expected 2 profiles, got %v\n", nProfiles)
//	}
//	if len(scores.scores) != 10 {
//		t.Fatal("Expected 10 scores")
//	}
//	if request.Threshold != 5 {
//		t.Fatalf("Expected 50, got %v\n", request.Threshold)
//	}
//	for _, score := range scores.scores {
//		if score.status != PENDING {
//			t.Fatalf("Got %v", score)
//		}
//	}
//}
//
//func TestParsePartialCache(t *testing.T) {
//	testFile, err := os.Open("testdata/TestParsePartialCache.bson")
//	if err != nil {
//		t.Fatal("Couldn't load test data")
//	}
//	request, cache, index, err := parse(testFile, ProgressSinkHole())
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	var scores scoresStore
//	if scores, err = NewScores(request, cache, index); err != nil {
//		t.Fatal(err)
//	}
//
//	if scores.canReuseCache != false {
//		t.Fatal("Expected false")
//	}
//}
//
//func TestParseDuplicates(t *testing.T) {
//	testFile, err := os.Open("testdata/TestDuplicatePi.bson")
//	if err != nil {
//		t.Fatal("Couldn't load test data")
//	}
//	cache := NewCache()
//	docs := bsonkit.GetDocuments(testFile)
//
//	docs.Next()
//	doc := docs.Doc
//	if err := cache.Update(doc, 50); err != nil {
//		t.Fatal(err)
//	}
//
//	docs.Next()
//	doc = docs.Doc
//	if err = cache.Update(doc, 50); err != nil {
//		t.Fatal(err)
//	}
//
//	docs.Next()
//	doc = docs.Doc
//	if err = cache.Update(doc, 50); err == nil {
//		t.Fatal("Expected a duplicate pi error")
//	}
//}
//
//func TestParseThresholds(t *testing.T) {
//	testFile, err := os.Open("testdata/TestParse.bson")
//	if err != nil {
//		t.Fatal("Couldn't load test data")
//	}
//	cache := NewCache()
//	docs := bsonkit.GetDocuments(testFile)
//
//	docs.Next()
//	docs.Next()
//	doc := docs.Doc
//	if err := cache.Update(doc, 5); err != nil {
//		t.Fatal(err)
//	}
//
//	if cache.Threshold != 5 {
//		t.Fatal("Expected 5")
//	}
//}
//
//func TestAllParse(t *testing.T) {
//	var nSTs, expected int
//	testFile, err := os.Open("testdata/FakeProfiles.bson")
//	if err != nil {
//		t.Fatal("Couldn't load test data")
//	}
//	request, cache, index, err := parse(testFile, ProgressSinkHole())
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	var scores scoresStore
//	if scores, err = NewScores(request, cache, index); err != nil {
//		t.Fatal(err)
//	}
//	if !scores.canReuseCache {
//		t.Fatal("Expected true")
//	}
//	if nSTs, expected = len(scores.STs), 7000; nSTs != expected {
//		t.Fatalf("Expected %d STs, got %d\n", expected, nSTs)
//	}
//
//	nProfiles := 0
//	for _, i := range index.indices {
//		if i.Ready {
//			nProfiles++
//		}
//	}
//	if actual, expected := nProfiles, nSTs; actual != expected {
//		t.Fatalf("Expected %d profiles, got %d\n", expected, actual)
//	}
//	if actual, expected := len(scores.scores), nSTs*(nSTs-1)/2; actual != expected {
//		t.Fatalf("Expected %d scores, got %d\n", expected, actual)
//	}
//	if request.Threshold != 50 {
//		t.Fatalf("Expected 50, got %v\n", request.Threshold)
//	}
//}
//
//func TestRead(t *testing.T) {
//	testFile, err := os.Open("testdata/FakeProfiles.bson")
//	if err != nil {
//		t.Fatal("Couldn't load test data")
//	}
//
//	docs := bsonkit.GetDocuments(testFile)
//	docs.Next()
//
//	doc := docs.Doc
//	if !doc.Next() {
//		t.Fatal("Expected a key")
//	} else if doc.Err != nil {
//		t.Fatal(doc.Err)
//	}
//
//	profiles := 0
//	for docs.Next() {
//		doc = docs.Doc
//		for doc.Next() {
//			if string(doc.Key()) == "results" {
//				profiles++
//				break
//			}
//		}
//		if doc.Err != nil {
//			t.Fatal(doc.Err)
//		}
//	}
//	if docs.Err != nil {
//		t.Fatal(doc.Err)
//	}
//
//	if profiles != 7000 {
//		t.Fatalf("Expected 10000 profiles, got %d\n", profiles)
//	}
//}
