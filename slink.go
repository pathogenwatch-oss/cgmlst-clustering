package main

import (
	"errors"
	"math"
)

const ALMOST_INF = math.MaxInt32

type clusterID = int

type Clusters struct {
	pi     []int
	lambda []int
	nItems int
}

type ClusterOutput struct {
	Edges     map[int][][2]int `json:"edges"`
	Pi        []int            `json:"pi"`
	Lambda    []int            `json:"lambda"`
	Sts       []string         `json:"STs"`
	Threshold int              `json:"threshold"`
}

func ClusterFromScratch(distances []int, nItems int) (c Clusters, err error) {
	return ClusterFromCache(distances, nItems, NewCache())
}

func ClusterFromCache(distances []int, nItems int, cache *Cache) (c Clusters, err error) {
	if len(distances) != (nItems*(nItems-1))/2 {
		err = errors.New("Wrong number of distances given")
		return
	}

	// If items are {a, b, c, d, e} then the distances between them should be arranged:
	// distances := {
	// 	(a => b),
	// 	(a => c), (b => c),
	// 	(a => d), (b => d), (c => d),
	// 	(a => e), (b => e), (c => e), (d => e),
	// }

	c.pi = make([]int, nItems)
	c.lambda = make([]int, nItems)
	c.nItems = nItems

	copy(c.pi, cache.Pi)
	copy(c.lambda, cache.Lambda)
	nCacheItems := len(cache.Pi)

	// Uses the SLINK algorithm for single linkage clustering
	// http://www.cs.gsu.edu/~wkim/index_files/papers/sibson.pdf
	// R. Sibson (1973)
	// The Computer Journal. British Computer Society.

	// This has some good diagrams with background
	// https://github.com/battuzz/slink/blob/master/doc/Presentation.pdf

	// lambda[i] is the distance at which `i` would be clustered with something bigger than it
	// pi[i] is the biggest object in the cluster it joins

	M := make([]int, nItems)
	mStart := nCacheItems * (nCacheItems - 1) / 2
	mEnd := mStart

	for n := nCacheItems; n < nItems; n++ {
		// We build up pi and lambda by adding each datum in increasing size

		// If the sequences are {a, b, c, d}
		// the distances are:
		// {(a,b), (a, c), (b, c), (a, d), (b, d), (c, d)}

		// Here we set M to be each of the distances of things < n to n
		// i.e. {(0, n), (1, n) ... (n-2, n-1)}
		mStart, mEnd = mEnd, mEnd+n
		copy(M, distances[mStart:mEnd])

		// The new node starts by pointing to itself and assumes no bigger nodes exist
		c.pi[n] = n
		c.lambda[n] = ALMOST_INF

		for i := 0; i < n; i++ {
			if M[i] <= c.lambda[i] { // There's a new bigger thing closer than we thought
				if M[c.pi[i]] > c.lambda[i] {
					M[c.pi[i]] = c.lambda[i] // The thing i was pointing could be considered closer to n
				}
				c.pi[i] = n        // The next biggest thing is now n
				c.lambda[i] = M[i] // And update the distance to it
			} else {
				if M[c.pi[i]] > M[i] {
					M[c.pi[i]] = M[i] // The thing i was pointing could be considered closer to n
				}
			}
		}
		for i := 0; i < n; i++ {
			// If the biggest thing i's cluster is further from it's next cluster
			// than i is from the cluster
			if c.lambda[i] >= c.lambda[c.pi[i]] {
				c.pi[i] = n // make n the biggest thing in i's next cluster
			}
		}
	}
	return
}

func (c Clusters) Format(threshold int, distances []int, sts []CgmlstSt) (output chan ClusterOutput) {
	output = make(chan ClusterOutput, 5)
	go func() {
		defer close(output)
		for t := 0; t <= threshold; t++ {
			edges := map[int][][2]int{}
			atThreshold := make([][2]int, 0, 100)
			idx := 0
			for i := 1; i < c.nItems; i++ {
				for j := 0; j < i; j++ {
					if distances[idx] == t {
						atThreshold = append(atThreshold, [2]int{j, i})
					}
					idx++
				}
			}
			edges[t] = atThreshold
			output <- ClusterOutput{edges, []int{}, []int{}, []CgmlstSt{}, threshold}
		}
		output <- ClusterOutput{map[int][][2]int{}, c.pi, c.lambda, sts, threshold}
	}()

	return output
}

// This isn't used except for testing
func (c Clusters) Get(threshold int) []int {
	clusterIDs := make([]int, c.nItems)
	for i := len(clusterIDs) - 1; i >= 0; i-- {
		if c.lambda[i] > threshold {
			clusterIDs[i] = i
		} else {
			clusterIDs[i] = clusterIDs[c.pi[i]]
		}
	}
	return clusterIDs
}
