package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	generator "github.com/julmust/kafka-queue-filler/pkg"
)

func main() {
	no_messages := flag.Int("messages", 10, "Number of messages to generate")
	log_output := flag.Bool("logging", false, "If true, enables logging. Requires -logpath to be set")
	log_path := flag.String("logpath", "", "Sets the output path for log messages")
	flag.Parse()

	if *log_output {
		if *log_path == "" {
			fmt.Fprint(os.Stderr, "Error: logging enabled but no logpath set\n")
			os.Exit(1)
		}

		lf, _ := os.OpenFile(*log_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		log.SetOutput(lf)
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
		defer lf.Close()

		log.Print("Logger initialized...")
	} else {
		log.SetOutput(io.Discard)
	}

	generator.Run(*no_messages)
}
