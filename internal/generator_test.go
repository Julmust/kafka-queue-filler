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

var kafkaContainer *gnomock.Container

func TestMain(m *testing.M) {
	// Set up mock Kafka container
	container, err := gnomock.Start(
		kafka.Preset(kafka.WithTopics("events")),
		gnomock.WithDebugMode(),
		gnomock.WithLogWriter(os.Stdout),
		gnomock.WithContainerName("kafka"),
	)
	if err != nil {
		panic(err)
	}
	kafkaContainer = container

	eV := m.Run()

	_ = gnomock.Stop(container)

	os.Exit(eV)
}

func TestRun(t *testing.T) {
	res := Run(10)

	if res != 10 {
		t.Errorf("Run: expected 10, received %d", res)
	}
}

func TestGeneration(t *testing.T) {
	var wg sync.WaitGroup

	wg.Add(1)

	p, err := ckafka.NewProducer(
		&ckafka.ConfigMap{
			"bootstrap.servers": kafkaContainer.Address(kafka.BrokerPort),
		},
	)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	res := generate(&wg, p)
	var msg Message

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
	msg := Message{"testArtist", "testSong"}
	json_byte_msg, err := json.Marshal(msg)
	if err != nil {
		log.Println(err)
	}

	topic := "events"
	p, err := ckafka.NewProducer(
		&ckafka.ConfigMap{
			"bootstrap.servers": kafkaContainer.Address(kafka.BrokerPort),
		},
	)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	p.Produce(&ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic},
		Value:          json_byte_msg,
	}, nil)

	p.Flush(5 * 1000)

	err = writeToKafka(json_byte_msg, p)
	if err != nil {
		t.Error(err)
	}
}
