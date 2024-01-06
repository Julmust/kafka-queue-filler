package generator

import (
	"encoding/json"
	"log"
	"reflect"
	"sync"
	"testing"
)

func TestRun(t *testing.T) {
	res := Run(10)

	if res != 10 {
		t.Errorf("Run: expected 10, received %d", res)
	}
}

func TestGeneration(t *testing.T) {
	var wg sync.WaitGroup

	wg.Add(1)
	res := generate(&wg)
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

	err = writeToKafka(json_byte_msg)
	if err != nil {
		t.Error(err)
	}
}
