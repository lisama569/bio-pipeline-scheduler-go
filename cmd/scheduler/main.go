package main

import (
	"fmt"
	"os"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: scheduler <run|validate> <pipeline.json> [--state DIR]")
}

func main() {
	usage()
}
