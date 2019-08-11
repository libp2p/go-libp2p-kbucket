package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"strings"

	mh "github.com/multiformats/go-multihash"
)

const bits = 16
const target = 1 << bits
const idLen = 32 + 2

func main() {
	pkg := os.Getenv("GOPACKAGE")
	file := os.Getenv("GOFILE")
	targetFile := strings.TrimSuffix(file, ".go") + "_prefixmap.go"

	ids := new([target]uint64)
	found := new([target]bool)
	count := int32(0)

	out := make([]byte, 32)
	inp := [idLen]byte{mh.SHA2_256, 32}
	hasher := sha256.New()

	for i := uint64(0); count < target; i++ {
		binary.BigEndian.PutUint64(inp[2:], i)

		hasher.Write(inp[:])
		out = hasher.Sum(out[:0])
		hasher.Reset()

		prefix := binary.BigEndian.Uint32(out) >> (32 - bits)
		if !found[prefix] {
			found[prefix] = true
			ids[prefix] = i
			count++
		}
	}

	f, err := os.Create(targetFile)
	if err != nil {
		panic(err)
	}

	printf := func(s string, args ...interface{}) {
		_, err := fmt.Fprintf(f, s, args...)
		if err != nil {
			panic(err)
		}
	}

	printf("package %s\n\n", pkg)
	printf("// Code generated by generate/generate_map.go DO NOT EDIT\n")
	printf("var keyPrefixMap = [...]uint64{")
	for i, j := range ids[:] {
		if i%16 == 0 {
			printf("\n\t")
		} else {
			printf(" ")
		}
		printf("%d,", j)
	}
	printf("\n}")
	f.Close()
}