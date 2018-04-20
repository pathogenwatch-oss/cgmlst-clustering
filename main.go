package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"

	"gitlab.com/cgps/bsonkit"
)

var thresholds = map[string][]int{
	"1280": {5, 50, 200, 500},
}

type ClusterDetails struct {
	Genome  bsonkit.ObjectID `json:"genome"`
	Cluster bsonkit.ObjectID `json:"cluster"`
}

type ClusterOutput struct {
	Taxid     string           `json:"taxid"`
	Threshold int              `json:"threshold"`
	Genomes   []ClusterDetails `json:"genomes"`
}

func isSmaller(a, b bsonkit.ObjectID) bool {
	return bytes.Compare(a[:], b[:]) < 0
}

func mapGenomeToCluster(threshold int, c Clusters, IDs []GenomeID) []ClusterDetails {
	// For each genome we want to return the name of the cluster
	// clusters are named after their first member, this is the sequence
	// with the smallest genomeID.

	type fileID = string
	type genomeID = bsonkit.ObjectID
	type clusterID = int
	minIDs := make(map[fileID]genomeID)

	// Each fileID could be shared with more than one genomeID.  We find the
	// smallest (i.e. earliest) genomeID for each fileID
	for _, id := range IDs {
		if current, found := minIDs[id.fileID]; !found || isSmaller(id.id, current) {
			minIDs[id.fileID] = id.id
		}
	}

	// clusterIDs are arbitrary integers identifying the cluster of each fileID
	// They're sorted the same as fileIDs below.
	clusterIDs := c.Get(threshold)
	fileIDs := c.fileIDs

	minCluster := make(map[clusterID]genomeID)
	var (
		fID fileID
		gID genomeID
	)

	// This is used to store a map between each fileID and the cluster it is found
	// in.  The clusterIDs are essentially meaningless integers.
	fileIDToClusterID := make(map[fileID]clusterID)

	for i, cID := range clusterIDs {
		fID = fileIDs[i]
		fileIDToClusterID[fID] = cID

		// gID is the smallest genomeID for a given fileID
		gID = minIDs[fID] // ASSUMPTION: this will always be found otherwise where did we get the fileID?
		if current, found := minCluster[cID]; !found || isSmaller(gID, current) {
			minCluster[cID] = gID
		}
	}

	// Now we have a mapping between fileIDs and clusterIDs and the smallest
	// genomeID for each clusterID, we can iterate over the genomes and say
	// which is the smallest genomeID they share a cluster with.
	clusters := make([]ClusterDetails, len(IDs))
	for i, id := range IDs {
		cID := fileIDToClusterID[id.fileID]
		cName := minCluster[cID]
		clusters[i] = ClusterDetails{id.id, cName}
	}

	return clusters
}

func main() {
	r := (io.Reader)(os.Stdin)
	fileIDs, IDs, profiles, scores, err := parse(r)
	if err != nil {
		panic(err)
	}

	if err := scoreAll(scores, profiles); err != nil {
		panic(err)
	}

	enc := json.NewEncoder(os.Stdout)
	for c := range buildCacheOutputs(scores) {
		enc.Encode(c)
	}

	// TODO: HARDCODED
	taxid := "1280"
	clusters := NewClusters(scores)
	for _, threshold := range thresholds[taxid] {
		details := ClusterOutput{
			Taxid:     taxid,
			Threshold: threshold,
			Genomes:   mapGenomeToCluster(threshold, clusters, IDs),
		}
		enc.Encode(details)
	}

	log.Printf("fileIDs: %d; Scores: %d\n", len(fileIDs), len(scores.scores))
}
