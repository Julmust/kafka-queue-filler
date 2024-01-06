package generator

import (
	"encoding/json"
	"log"
	"os"
	"reflect"
	"sync"
	"testing"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/kafka"
)

func TestRun(t *testing.T) {
	res := Run(10)

	if res != 10 {
		t.Errorf("Run: expected 10, received %d", res)
	}
}

func TestGeneration(t *testing.T) {
	var wg sync.WaitGroup

	// Channel size required to prevent locking of the channel for the test
	gen_vals := make(chan []byte, 1)

	wg.Add(1)

	generate(&wg, gen_vals)
	wg.Wait()

	var msg Message
	res := <-gen_vals
	close(gen_vals)

	if err := json.Unmarshal(res, &msg); err != nil {
		panic(err)
	}

	if reflect.TypeOf(msg.Artist).String() != "string" {
		t.Errorf("msg.Arist: Not string value")
	}

	if reflect.TypeOf(msg.Song).String() != "string" {
		t.Errorf("msg.Song: Not string value")
	}

}

func TestWriteToKafka(t *testing.T) {
	// Create mock message
	msg := Message{"testArtist", "testSong"}
	json_byte_msg, err := json.Marshal(msg)
	if err != nil {
		log.Println(err)
	}

	// Create mock Kafka instance
	container, err := gnomock.Start(
		kafka.Preset(kafka.WithTopics("events")),
		gnomock.WithDebugMode(),
		gnomock.WithLogWriter(os.Stdout),
		gnomock.WithContainerName("kafka"),
	)
	if err != nil {
		panic(err)
	}

	// Create producer and define topic to send message to
	p, err := ckafka.NewProducer(
		&ckafka.ConfigMap{
			"bootstrap.servers": container.Address(kafka.BrokerPort),
		},
	)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// Create channel with size 1 to prevent channel from locking
	gen_vals := make(chan []byte, 1)
	gen_vals <- json_byte_msg
	close(gen_vals)

	// Test writing to topic
	writeToKafka(p, gen_vals)
}
