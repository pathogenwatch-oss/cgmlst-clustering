package main

import "math/bits"

type BitArray struct {
	blocks  []uint64
	nBlocks uint64
}

func NewBitArray(max uint64) *BitArray {
	nBlocks := (max / 64) + 1
	b := BitArray{
		make([]uint64, nBlocks),
		nBlocks,
	}
	return &b
}

func (b *BitArray) SetBit(i uint64) {
	block := i / 64
	bit := i % 64
	if block >= b.nBlocks {
		b.blocks = append(b.blocks, block-b.nBlocks+1)
	}
	b.blocks[block] &= i << bit
}

func CompareBits(b1 *BitArray, b2 *BitArray) int {
	var nBlocks uint64
	if b1.nBlocks < b2.nBlocks {
		nBlocks = b1.nBlocks
	} else {
		nBlocks = b2.nBlocks
	}

	count := 0
	var i uint64
	for i = 0; i < nBlocks; i++ {
		if b1.blocks[i] == 0 {
			continue
		} else if b1.blocks[i] == 0 {
			continue
		}
		combined := b1.blocks[i] & b1.blocks[i]
		if combined == 0 {
			continue
		}
		count += bits.OnesCount64(combined)
	}
	return count
}
