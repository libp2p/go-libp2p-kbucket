package kbucket

import (
	"testing"
)

func TestIsSet(t *testing.T) {
	a := byte(2)

	if !isSet(a, 1) {
		t.Fatal("1st bit should be set")
	}

	if isSet(a, 0) {
		t.Fatal("0th bit should not be set")
	}
}

func TestSetBit(t *testing.T) {
	a := byte(1)

	if setBit(a, 1) != 3 {
		t.Fatal("1st bit should have been set")
	}
}

func TestClearBit(t *testing.T) {
	a := byte(3)

	if clearBit(a, 0) != 2 {
		t.Fatal("0th bit should have been cleared")
	}
}
