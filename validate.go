package pilosa

import (
	"regexp"
)

const (
	maxDatabaseName = 64
	maxFrameName    = 64
	maxLabel        = 64
)

var databaseNameRegex = regexp.MustCompile("^[a-z0-9_-]+$")
var frameNameRegex = regexp.MustCompile("^[a-z0-9][.a-z0-9_-]*$")
var labelRegex = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9_]*$")

// ValidDatabaseName returns true if the given database name is valid, otherwise false
func ValidDatabaseName(name string) bool {
	return len(name) <= maxDatabaseName && databaseNameRegex.Match([]byte(name))
}

// ValidFrameName returns true if the given frame name is valid, otherwise false
func ValidFrameName(name string) bool {
	return len(name) <= maxFrameName && frameNameRegex.Match([]byte(name))
}

// ValidLabel returns true if the given label is valid, otherwise false
func ValidLabel(label string) bool {
	return len(label) <= maxLabel && labelRegex.Match([]byte(label))
}

func validateDatabaseName(name string) error {
	if ValidDatabaseName(name) {
		return nil
	}
	return ErrorInvalidDatabaseName
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
