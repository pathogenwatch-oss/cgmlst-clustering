package main

import (
	"math/bits"
)

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
		newBlocks := make([]uint64, block+1)
		copy(newBlocks, b.blocks)
		b.blocks = newBlocks
		b.nBlocks = block + 1
	}
	b.blocks[block] |= (1 << bit)
}

func CompareBits(b1 *BitArray, b2 *BitArray) int {
	var smaller, bigger []uint64
	if b1.nBlocks < b2.nBlocks {
		smaller = b1.blocks
		bigger = b2.blocks[:b1.nBlocks]
	} else {
		smaller = b2.blocks
		bigger = b1.blocks[:b2.nBlocks]
	}

	count := 0
	for i, block := range smaller {
		if block == 0 {
			continue
		} else if bigger[i] == 0 {
			continue
		}
		combined := (block & bigger[i])
		if combined != 0 {
			count += bits.OnesCount64(combined)
		}
	}
	return count
}
