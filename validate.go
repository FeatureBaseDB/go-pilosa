package pilosa

import (
	"regexp"
)

const (
	maxIndexName = 64
	maxFrameName = 64
	maxLabel     = 64
)

var indexNameRegex = regexp.MustCompile("^[a-z0-9_-]+$")
var frameNameRegex = regexp.MustCompile("^[a-z0-9][.a-z0-9_-]*$")
var labelRegex = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9_]*$")

// ValidIndexName returns true if the given index name is valid, otherwise false
func ValidIndexName(name string) bool {
	return len(name) <= maxIndexName && indexNameRegex.Match([]byte(name))
}

// ValidFrameName returns true if the given frame name is valid, otherwise false
func ValidFrameName(name string) bool {
	return len(name) <= maxFrameName && frameNameRegex.Match([]byte(name))
}

// ValidLabel returns true if the given label is valid, otherwise false
func ValidLabel(label string) bool {
	return len(label) <= maxLabel && labelRegex.Match([]byte(label))
}

func validateIndexName(name string) error {
	if ValidIndexName(name) {
		return nil
	}
	return ErrorInvalidIndexName
}

func validateFrameName(name string) error {
	if ValidFrameName(name) {
		return nil
	}
	return ErrorInvalidFrameName
}

func validateLabel(label string) error {
	if ValidLabel(label) {
		return nil
	}
	return ErrorInvalidLabel
}
