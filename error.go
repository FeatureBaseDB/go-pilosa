package pilosa

import (
	"fmt"
)

type PilosaError struct {
	Message string
}

// NewPilosaError creates a Pilosa error
func NewPilosaError(message string) *PilosaError {
	return &PilosaError{Message: message}
}

func (e PilosaError) Error() string {
	return fmt.Sprintf("PilosaError: %s", e.Message)
}

var (
	ErrorEmptyCluster        = NewPilosaError("No usable addresses in the cluster")
	ErrorDatabaseExists      = NewPilosaError("Database exists")
	ErrorFrameExists         = NewPilosaError("Frame exists")
	ErrorInvalidDatabaseName = NewPilosaError("Invalid database name")
	ErrorInvalidFrameName    = NewPilosaError("Invalid frame name")
	ErrorInvalidLabel        = NewPilosaError("Invalid label")
)
