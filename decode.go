package main

import (
	"errors"
	"io"

	"gitlab.com/cgps/bsonkit"
)

func parseMatch(data []byte) (gene string, id interface{}, err error) {
	var ok bool
	elements := bsonkit.Parse(data)
	for elements.Next() {
		element := elements.Element()
		switch element.Key() {
		case "gene":
			if gene, ok = element.Value().(string); !ok {
				err = errors.New("Bad value for gene")
				return
			}
		case "id":
			id = element.Value()
		}
	}
	if elements.Err != nil {
		err = elements.Err
	}
	return
}

func parseMatches(data []byte, p *Profile) error {
	elements := bsonkit.Parse(data)
	for elements.Next() {
		element := elements.Element()
		gene, id, err := parseMatch(element.Bytes)
		if err != nil {
			return err
		}
		p.Matches[gene] = id
	}
	if elements.Err != nil {
		return elements.Err
	}
	return nil
}

func parseCgMlst(data []byte, p *Profile) (err error) {
	var ok bool
	elements := bsonkit.Parse(data)
	for elements.Next() {
		element := elements.Element()
		switch element.Key() {
		case "__v":
			if p.Version, ok = element.Value().(string); !ok {
				err = errors.New("Bad value for __v")
			}
		case "matches":
			err = parseMatches(element.Bytes, p)
		}
		if err != nil {
			return
		}
	}
	if elements.Err != nil {
		return elements.Err
	}
	if p.Version == "" {
		return errors.New("version not found")
	}
	if len(p.Matches) == 0 {
		return errors.New("No matches parsed")
	}
	return
}

func parseAnalysis(data []byte, p *Profile) (err error) {
	elements := bsonkit.Parse(data)
	for elements.Next() {
		element := elements.Element()
		switch element.Key() {
		case "cgmlst":
			err = parseCgMlst(element.Bytes, p)
			return
		}
	}
	if elements.Err != nil {
		return elements.Err
	}
	return errors.New("Could not find cgmlst in analysis")
}

func parseProfile(data []byte) (profile Profile, err error) {
	var ok bool
	elements := bsonkit.Parse(data)
	for elements.Next() {
		element := elements.Element()
		switch element.Key() {
		case "_id":
			if profile.ID, ok = element.Value().(ObjectID); !ok {
				err = errors.New("Bad value for _id")
			}
		case "fileId":
			if profile.FileID, ok = element.Value().(string); !ok {
				err = errors.New("Bad value for fileId")
			}
		case "organismId":
			if profile.OrganismID, ok = element.Value().(string); !ok {
				err = errors.New("Bad value for organismId")
			}
		case "public":
			if profile.Public, ok = element.Value().(bool); !ok {
				err = errors.New("Bad value for public")
			}
		case "analysis":
			err = parseAnalysis(element.Bytes, &profile)
		}
		if err != nil {
			return
		}
	}
	if elements.Err != nil {
		err = elements.Err
	}
	return
}

func parse(r io.Reader) (fileIDs []string, profiles map[string]Profile, scores scoresStore, err error) {
	docs := bsonkit.GetDocuments(r)
	docs.Next()

	if docs.Err != nil {
		err = docs.Err
		return
	}

	firstDoc := bsonkit.Parse(docs.Bytes)
	for firstDoc.Next() {
		element := firstDoc.Element()
		switch element.Key() {
		case "genomes":
			fileIds, err = parseGenomes(element.Bytes)
			if err != nil {
				return
			}
			break
		}
	}

	if firstDoc.Err != nil {
		err = firstDoc.Err
		return
	}

	if len(fileIDs) == 0 {
		err = errors.New("No fileIds found in first doc")
		return
	}

	for docs.Next() {
		doc := bsonkit.Parse(docs.Bytes)
		for doc.Next() {
			element := doc.Element()
			switch element.Key() {
			case "analysis":
				_, err = parseProfile(element.Bytes)
			case "scores":
				_, err = parseScores(element.Bytes)
			}
			if err != nil {
				return
			}
		}
		if doc.Err != nil {
			err = doc.Err
			return
		}
	}
	if docs.Err != nil {
		err = docs.Err
		return
	}
	return
}
