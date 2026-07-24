package main

import (
	"os"

	"github.com/shaneburrell/quiksync/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
