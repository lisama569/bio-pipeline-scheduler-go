package pipeline

import (
	"errors"
	"os"
)

func readFile(p string) ([]byte, error)   { return os.ReadFile(p) }
func writeFile(p string, b []byte) error  { return os.WriteFile(p, b, 0o644) }
func errorsAs(err error, target any) bool { return errors.As(err, target) }
