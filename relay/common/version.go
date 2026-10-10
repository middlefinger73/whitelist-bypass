package common

import (
	"fmt"
	"os"
)

const Version = "0.3.28-ackstats"

func MaybePrintVersion() {
	for _, arg := range os.Args[1:] {
		if arg == "-version" || arg == "--version" {
			fmt.Println(Version)
			os.Exit(0)
		}
	}
}
