package main

//func TestIndexer(t *testing.T) {
//	STs := []string{"abc123", "bcd234"}
//	indexer := NewIndexer(STs)
//	indexer.BitProfiles(&Profile{
//		ST: "abc123",
//		Matches: map[string]interface{}{
//			"gene1": 1,
//			"gene2": 1,
//			"gene3": 1,
//		},
//	})
//	index := indexer.profilesMap[indexer.lookup["abc123"]]
//	if value := index.Genes.blocks[0]; value != 7 {
//		t.Fatalf("Got %d, expected 7\n", value)
//	}
//
//	indexer.BitProfiles(&Profile{
//		ST: "bcd234",
//		Matches: map[string]interface{}{
//			"gene1": 2,
//			"gene2": 2,
//			"gene4": 1,
//		},
//	})
//
//	valueOfGene3 := (1 << indexer.geneTokens.Get(AlleleKey{
//		"gene3",
//		nil,
//	}))
//	valueOfGene4 := (1 << indexer.geneTokens.Get(AlleleKey{
//		"gene4",
//		nil,
//	}))
//	expectedValue := 7 - valueOfGene3 + valueOfGene4
//
//	index = indexer.profilesMap[indexer.lookup["bcd234"]]
//	if value := index.Genes.blocks[0]; value != uint64(expectedValue) {
//		t.Fatalf("Got %d, expected %d\n", value, expectedValue)
//	}
//	if value := index.Alleles.blocks[0]; value != 56 {
//		t.Fatalf("Got %d, expected 56\n", value)
//	}
//}
//
//func TestTokeniser(t *testing.T) {
//	tokens := NewTokeniser()
//	if token := tokens.Get(AlleleKey{"foo", 1}); token != 0 {
//		t.Fatal("Wanted 0")
//	}
//	if token := tokens.Get(AlleleKey{"foo", 1}); token != 0 {
//		t.Fatal("Wanted 0")
//	}
//	if token := tokens.Get(AlleleKey{"bar", 1}); token != 1 {
//		t.Fatal("Wanted 1")
//	}
//	if token := tokens.Get(AlleleKey{"foo", 1}); token != 0 {
//		t.Fatal("Wanted 0")
//	}
//	if token := tokens.Get(AlleleKey{"foo", "1"}); token != 2 {
//		t.Fatal("Wanted 2")
//	}
//}
