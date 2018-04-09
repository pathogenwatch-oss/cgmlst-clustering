package main

import (
	"bytes"
	"io"

	"gitlab.com/cgps/bsonkit"
)

type ObjectID [12]byte

func trimlast(s []byte) []byte { return s[:len(s)-1] }

func unmarshalMatch(data []byte) (string, interface{}) {
	iter := reader{bson: data[4 : len(data)-1]}
	m := Match{}
	for iter.Next() {
		typ, ename, element := iter.Element()
		key := string(trimlast(ename))
		switch key {
		case "gene":
			m.Gene = string(trimlast(element))
		case "id":
			switch typ {
			case 0x02:
				m.ID = string(trimlast(element))
			case 0x10:
				m.ID = int64(element[0]) | int64(element[1])<<8 | int64(element[2])<<16 | int64(element[3])<<24
			case 0x12:
				m.ID = int64(element[0]) | int64(element[1])<<8 | int64(element[2])<<16 | int64(element[3])<<24 | int64(element[4])<<32 | int64(element[5]<<40) | int64(element[6]<<48) | int64(element[7]<<56)
			}
		}
	}
	return "", m
}

func unmarshalMatches(data []byte, p *Profile) error {
	iter := bsonkit.Reader(data)
	for {
		element, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		gene, id := unmarshalMatch(element.Bytes)
		p.Matches[gene] = id
	}
	return iter.Err()
}

func unmarshalCgMlst(data []byte, p *Profile) error {
	iter := bsonkit.Reader(data)
	for {
		element, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		switch element.Key() {
		case "__v":
			p.Version = element.Value()
		case "matches":
			unmarshalMatches(element.Bytes, p)
		}
	}
	return iter.Err()
}

func unmarshalAnalysis(data []byte, p *Profile) error {
	iter := bsonkit.Reader(data)
	for {
		element, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		switch element.Key() {
		case "cgmlst":
			unmarshalCgMlst(element, p)
		}
	}
	return iter.Err()
}

// func Unmarshal(data []byte, profile *ProfileDoc) error {
// 	iter := reader{bson: data[4 : len(data)-1]}
// 	for iter.Next() {
// 		_, ename, element := iter.Element()
// 		key := string(trimlast(ename))

// 		switch key {
// 		case "_id":
// 			var oid ObjectID
// 			copy(oid[:], element)
// 			profile.ID = oid
// 		case "fileId":
// 			profile.FileID = string(trimlast(element))
// 		case "organismId":
// 			profile.OrganismID = string(trimlast(element))
// 		case "public":
// 			profile.Public = element[0] == 1
// 		case "analysis":
// 			unmarshalAnalysis(element, profile)
// 		}
// 	}
// 	return iter.Err()
// }

func parseProfile(data []byte) Profile {
	profile := Profile{}
	iter := bsonkit.Reader(data)
	for {
		element, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		switch element.Key() {
		case "_id":
			profile.ID = element.Value()
		case "fileId":
			profile.FileID = element.Value()
		case "organismId":
			profile.OrganismID = element.Value()
		case "public":
			profile.Public = element.Value()
		case "analysis":
			unmarshalAnalysis(element.Bytes, profile)
		}
	}
	return profile
}

func parseGenomes(data []byte) GenomesDoc {

}

func parse(r io.Reader) {
	for iter := bsonkit.GetDocuments(r); iter.Next(); {
		if bytes.Contains(iter.bytes, []byte("genomes")) {
			genomes := parseGenomes(iter.bytes)
			// do something with the genomes
		}
		if bytes.Contains(iter.bytes, []byte("cgmlst")) {
			profile := parseProfile(iter.bytes)
			// do something with the profile
		}
		if bytes.Contains(iter.bytes, []byte("scores")) {
			scores := parseScores(iter.bytes)
			// do something with the scores
		}
	}
}
