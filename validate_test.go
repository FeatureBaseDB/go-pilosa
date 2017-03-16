package pilosa

import "testing"

func TestValidateDatabaseName(t *testing.T) {
	names := []string{
		"a", "ab", "ab1", "1", "_", "-", "b-c", "d_e",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	for _, name := range names {
		if validateDatabaseName(name) != nil {
			t.Fatalf("Should be valid database name: %s", name)
		}
	}
}

func TestValidateDatabaseNameInvalid(t *testing.T) {
	names := []string{
		"", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
	}
	for _, name := range names {
		if validateDatabaseName(name) == nil {
			t.Fatalf("Should be invalid database name: %s", name)
		}
	}
}

func TestValidateFrameName(t *testing.T) {
	names := []string{
		"a", "ab", "ab1", "b-c", "d_e", "d.e", "1",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	for _, name := range names {
		if validateFrameName(name) != nil {
			t.Fatalf("Should be valid frame name: %s", name)
		}
	}
}

func TestValidateFrameNameInvalid(t *testing.T) {
	names := []string{
		"", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce", "_", "-", ".data",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
	}
	for _, name := range names {
		if validateFrameName(name) == nil {
			t.Fatalf("Should be invalid frame name: %s", name)
		}
	}

}

func TestValidateLabel(t *testing.T) {
	labels := []string{
		"a", "ab", "ab1", "d_e", "A", "Bc", "B1", "aB",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	for _, label := range labels {
		if validateLabel(label) != nil {
			t.Fatalf("Should be valid label: %s", label)
		}
	}
}

func TestValidateLabelInvalid(t *testing.T) {
	labels := []string{
		"", "1", "_", "-", "b-c", "'", "^", "/", "\\", "*", "a:b", "valid?no", "yüce",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
	}
	for _, label := range labels {
		if validateLabel(label) == nil {
			t.Fatalf("Should be invalid label: %s", label)
		}
	}
}
