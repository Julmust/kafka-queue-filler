// Package generator utilizes goroutines to generate messages
// for that is then sent to a Kafka queue
package generator

import (
	"encoding/json"
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
		fmt.Fprint(os.Stderr, "Error: Could not create kafka producer\n")
		os.Exit(1)
	}
	defer p.Close()

	// Populate Kafka queue
	start := time.Now()
	gen_vals := make(chan []byte, no_runs)
	for i := 0; i < no_runs/2; i++ {
		writeToKafka(p, gen_vals)
	}

	for j := 0; j < no_runs; j++ {
		wg.Add(1)

		started += 1
		go generate(&wg, gen_vals)
	}

	wg.Wait()
	end := time.Now()
	log.Printf("Done. Sent %d messages in %v. Exiting", no_runs, end.Sub(start))

	return started
}

// Generates a message containing song information and
// passes it to the function responsible to write to Kafka
func generate(wg *sync.WaitGroup, gen_vals chan []byte) {
	defer wg.Done()
	m := Message{artists[rand.Intn(len(artists))], songs[rand.Intn(len(songs))]}

	j, err := json.Marshal(m)
	if err != nil {
		log.Println(err)
	}

	log.Printf("Generated messsage: %v\n", string(j))

	gen_vals <- j
}

// Writes a byte encoded json message to the Kakfa topic
func writeToKafka(p *ckafka.Producer, gen_vals chan []byte) {
	topic := "events"
	for val := range gen_vals {
		err := p.Produce(&ckafka.Message{
			TopicPartition: ckafka.TopicPartition{Topic: &topic},
			Value:          val,
		}, nil)
		if err != nil {
			log.Println("writeToKafka: Failed to push message to Kafka.")
		}
	}
}
