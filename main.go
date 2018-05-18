package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"

	"gitlab.com/cgps/bsonkit"
)

type ClusterDetails struct {
	Genome  bsonkit.ObjectID `json:"genome"`
	Cluster bsonkit.ObjectID `json:"cluster"`
}

type ClusterOutput struct {
	Threshold int              `json:"threshold"`
	Genomes   []ClusterDetails `json:"genomes"`
}

func isSmaller(a, b bsonkit.ObjectID) bool {
	return bytes.Compare(a[:], b[:]) < 0
}

func mapGenomeToCluster(threshold int, c Clusters, STs []CgmlstSt, IDs []GenomeSTPair) []ClusterDetails {
	// TODO add a test for this

	// For each genome we want to return the name of their cluster
	// clusters are named after their first member, this is the sequence
	// with the smallest genomeID.
	minIDs := make(map[CgmlstSt]GenomeID)

	// Each CgMLST sequence type (ST) could be shared with more than one genomeID.  We find the
	// smallest (i.e. earliest) genomeID for each ST
	for _, id := range IDs {
		if current, found := minIDs[id.ST]; !found || isSmaller(id.ID, current) {
			minIDs[id.ST] = id.ID
		}
	}

	// clusterIDs are arbitrary integers identifying the cluster of each ST
	// They're sorted the same as STs below.
	clusterIDs := c.Get(threshold)

	minCluster := make(map[clusterID]GenomeID)
	var (
		ST  CgmlstSt
		gID GenomeID
	)

	// This is used to store a map between each ST and the cluster it is found
	// in.  The clusterIDs are essentially random integers.
	StToClusterID := make(map[CgmlstSt]clusterID)

	for i, cID := range clusterIDs {
		ST = STs[i]
		StToClusterID[ST] = cID

		// gID is the smallest genomeID for a given ST
		gID = minIDs[ST] // ASSUMPTION: this will always be found otherwise where did we get the ST?
		if current, found := minCluster[cID]; !found || isSmaller(gID, current) {
			minCluster[cID] = gID
		}
	}

	// Now we have a mapping between STs and clusterIDs and the smallest
	// genomeID for each clusterID, we can iterate over the genomes and say
	// which is the smallest genomeID they share a cluster with.
	clusters := make([]ClusterDetails, len(IDs))
	for i, id := range IDs {
		cID := StToClusterID[id.ST]
		cName := minCluster[cID]
		clusters[i] = ClusterDetails{id.ID, cName}
	}

	return clusters
}

func main() {
	r := (io.Reader)(os.Stdin)
	STs, IDs, profiles, scores, thresholds, err := parse(r)
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

	distances, err := scores.Distances()
	if err != nil {
		panic(err)
	}

	clusters, err := NewClusters(len(scores.STs), distances)
	if err != nil {
		panic(err)
	}
	for _, threshold := range thresholds {
		details := ClusterOutput{
			Threshold: threshold,
			Genomes:   mapGenomeToCluster(threshold, clusters, STs, IDs),
		}
		enc.Encode(details)
	}

	log.Printf("STs: %d; Scores: %d\n", len(STs), len(scores.scores))
}
