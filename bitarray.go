package main

import (
	"math/bits"
)

type BitArray struct {
	blocks  []uint64
	nBlocks uint64
}

type FrozenBlock struct {
	index int
	block uint64
}

type FrozenBitArray struct {
	blocks []FrozenBlock
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

func (b *BitArray) Freeze() FrozenBitArray {
	f := FrozenBitArray{}
	for i, block := range b.blocks {
		if block == 0 {
			continue
		}
		f.blocks = append(f.blocks, FrozenBlock{i, block})
	}
	return f
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
		} else if b2.blocks[i] == 0 {
			continue
		}
		combined := b1.blocks[i] & b2.blocks[i]
		if combined == 0 {
			continue
		}
		count += bits.OnesCount64(combined)
	}
	return count
}

func CompareFrozenBits(f1 FrozenBitArray, f2 FrozenBitArray) int {
	var count int

	n1 := len(f1.blocks)
	n2 := len(f2.blocks)
	var i1, i2 int
	for skip1, skip2 := 0, 0; skip1 < n1 && skip2 < n2; {
		i1, i2 = f1.blocks[skip1].index, f2.blocks[skip2].index
		if i1 < i2 {
			skip1++
			continue
		} else if i1 > i2 {
			skip2++
			continue
		}
		count += bits.OnesCount64(f1.blocks[skip1].block & f2.blocks[skip2].block)
		skip1++
		skip2++
	}

	return count
}
