// Package generator utilizes goroutines to generate messages
// for that is then sent to a Kafka queue
package generator

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var artists = []string{"a", "b", "c"}
var songs = []string{"1", "2", "3"}

type Message struct {
	Artist string
	Song   string
}

// Generates no_runs number of messages and pushes them
// to a Kafka queue
func Run(no_runs int) int {
	var wg sync.WaitGroup
	var started int

	// Set up Kafka connection
	p, err := ckafka.NewProducer(
		&ckafka.ConfigMap{
			"bootstrap.servers": "localhost",
		},
	)
	if err != nil {
		fmt.Fprint(os.Stderr, "Error: logging enabled but no logpath set\n")
		os.Exit(1)
	}
	defer p.Close()

	// Populate Kafka queue
	start := time.Now()
	for i := 0; i < no_runs; i++ {
		wg.Add(1)

		started += 1
		go generate(&wg, p)
	}
	wg.Wait()
	end := time.Now()
	log.Printf("Done. Sent %d messages in %v. Exiting", no_runs, end.Sub(start))

	return started
}

// Generates a message containing song information and
// passes it to the function responsible to write to Kafka
func generate(wg *sync.WaitGroup, p *ckafka.Producer) []byte {
	defer wg.Done()
	m := Message{artists[rand.Intn(len(artists))], songs[rand.Intn(len(songs))]}

	j, err := json.Marshal(m)
	if err != nil {
		log.Println(err)
	}

	log.Printf("Generated messsage: %v\n", string(j))

	if err := writeToKafka(j, p); err != nil {
		log.Print(err)
	}

	return j
}

func writeToKafka(json_obj []byte, p *ckafka.Producer) error {
	topic := "events"
	err := p.Produce(&ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic},
		Value:          json_obj,
	}, nil)
	if err != nil {
		return errors.New("writeToKafka: Failed to push message to Kafka.")
	}

	return nil
}
