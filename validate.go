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

func validateDatabaseName(name string) error {
	if len(name) > maxDatabaseName || !databaseNameRegex.Match([]byte(name)) {
		return ErrorInvalidDatabaseName
	}
	return nil
}

func validateFrameName(name string) error {
	if len(name) > maxFrameName || !frameNameRegex.Match([]byte(name)) {
		return ErrorInvalidFrameName
	}
	return nil
}

func validateLabel(label string) error {
	if len(label) > maxLabel || !labelRegex.Match([]byte(label)) {
		return ErrorInvalidLabel
	}
	return nil
}
