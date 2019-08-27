package main

import (
	"strings"
	"testing"
)

func TestProcessHeader(t *testing.T) {
	config := NewConfig()
	headerRow := []string{"a", "b", "c"}

	t.Run("invalid IDType", func(t *testing.T) {
		config.IDField = "a"
		config.IDType = "nope"
		_, _, _, err := processHeader(config, nil, headerRow)
		if err == nil || !strings.Contains(err.Error(), "unknown IDType") {
			t.Fatalf("unknown IDType gave: %v", err)
		}
	})
}
