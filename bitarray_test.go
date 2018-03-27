package main

import (
	"math/rand"
	"testing"
)

func TestCompare(t *testing.T) {
	b1 := NewBitArray(10)
	b1.SetBit(1)

	b2 := NewBitArray(20)
	b2.SetBit(1)
	b2.SetBit(11)

	if score := CompareBits(b1, b2); score != 1 {
		t.Fatalf("Got %d expected 1\n", score)
	}
}

func TestBlocks(t *testing.T) {
	b1 := NewBitArray(10)
	if nBlocks := len(b1.blocks); nBlocks != 1 {
		t.Fatalf("Got %d blocks, expected 1\n", nBlocks)
	}
	if value := b1.blocks[0]; value != 0 {
		t.Fatalf("Got %d value, expected 0\n", value)
	}

	b1.SetBit(7)
	if nBlocks := len(b1.blocks); nBlocks != 1 {
		t.Fatalf("Got %d blocks, expected 1\n", nBlocks)
	}
	var expected uint64 = 1 << 7
	if value := b1.blocks[0]; value != expected {
		t.Fatalf("Got %d value, expected %d\n", value, expected)
	}

	b1.SetBit(63)
	if nBlocks := len(b1.blocks); nBlocks != 1 {
		t.Fatalf("Got %d blocks, expected 1\n", nBlocks)
	}
	expected |= 1 << 63
	if value := b1.blocks[0]; value != expected {
		t.Fatalf("Got %d value, expected %d\n", value, expected)
	}

	b1.SetBit(64)
	if nBlocks := len(b1.blocks); nBlocks != 2 {
		t.Fatalf("Got %d blocks, expected 2\n", nBlocks)
	}
	if value := b1.blocks[0]; value != expected {
		t.Fatalf("Got %d value, expected %d\n", value, expected)
	}
	if value := b1.blocks[1]; value != 1 {
		t.Fatalf("Got %d value, expected 1\n", value)
	}

	b1.SetBit(1000)
	if nBlocks := len(b1.blocks); nBlocks != 16 {
		t.Fatalf("Got %d blocks, expected 16\n", nBlocks)
	}
}

func BenchmarkBitSet(b *testing.B) {
	bits := make([]uint64, 2000)
	rand.Seed(0)
	for i := range bits {
		bits[i] = rand.Uint64() % 150000
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ba := NewBitArray(140000)
		for _, bit := range bits {
			ba.SetBit(bit)
		}
	}
}

func BenchmarkCompareBits(b *testing.B) {
	rand.Seed(0)
	b1 := NewBitArray(140000)
	for i := 0; i < 2000; i++ {
		b1.SetBit(rand.Uint64() % 150000)
	}
	b2 := NewBitArray(140000)
	for i := 0; i < 2000; i++ {
		b2.SetBit(rand.Uint64() % 150000)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompareBits(b1, b2)
	}
}
