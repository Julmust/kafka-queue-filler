// Package generator utilizes goroutines to generate messages
// for that is then sent to a Kafka queue
package generator

import (
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"
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

	// Populate Kafka queue
	start := time.Now()
	for i := 0; i < no_runs; i++ {
		wg.Add(1)

		started += 1
		go Generate(&wg)
	}
	wg.Wait()
	end := time.Now()
	log.Printf("Done. Sent %d messages in %v. Exiting", no_runs, end.Sub(start))

	return started
}

// Generates a message containing song information and
// passes it to the function responsible to write to Kafka
func Generate(wg *sync.WaitGroup) []byte {
	defer wg.Done()
	m := Message{artists[rand.Intn(len(artists))], songs[rand.Intn(len(songs))]}

	j, err := json.Marshal(m)
	if err != nil {
		log.Println(err)
	}

	log.Printf("Generated messsage: %v\n", string(j))

	if err := writeToKafka(j); err != nil {
		log.Print(err)
	}

	return j
}

func writeToKafka(json_obj []byte) error {
	return errors.New("writeToKafka: Failed to push message to Kafka.")
}
