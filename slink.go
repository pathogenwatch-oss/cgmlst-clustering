package main

import (
	"math"
)

const ALMOST_INF = math.MaxInt32

type Clusters struct {
	pi      []int
	lambda  []int
	fileIDs []string
}

func NewClusters(scores scoresStore) (c Clusters) {
	distances := scores.scores
	c.fileIDs = scores.fileIDs
	c.pi = make([]int, len(c.fileIDs))
	c.lambda = make([]int, len(c.fileIDs))

	// Uses the SLINK algorithm for single linkage clustering
	// http://www.cs.gsu.edu/~wkim/index_files/papers/sibson.pdf
	// R. Sibson (1973)
	// The Computer Journal. British Computer Society.

	// This has some good diagrams with background
	// https://github.com/battuzz/slink/blob/master/doc/Presentation.pdf

	// lambda[i] is the distance at which `i` would be clustered with something bigger than it
	// pi[i] is the biggest object in the cluster it joins

	M := make([]scoreDetails, len(c.fileIDs))
	mStart := 0
	mEnd := 0

	for n := 0; n < len(c.fileIDs); n++ {
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
			if M[i].value <= c.lambda[i] { // There's a new bigger thing closer than we thought
				if M[c.pi[i]].value > c.lambda[i] {
					M[c.pi[i]].value = c.lambda[i] // The thing i was pointing could be considered closer to n
				}
				c.pi[i] = n              // The next biggest thing is now n
				c.lambda[i] = M[i].value // And update the distance to it
			} else {
				if M[c.pi[i]].value > M[i].value {
					M[c.pi[i]].value = M[i].value // The thing i was pointing could be considered closer to n
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

func (c Clusters) Get(threshold int) []int {
	out := make([]int, len(c.fileIDs))
	for i := len(out) - 1; i >= 0; i-- {
		if c.lambda[i] > threshold {
			out[i] = i
		} else {
			out[i] = out[c.pi[i]]
		}
	}
	return out
}
