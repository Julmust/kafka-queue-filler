package tests

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	generator "github.com/julmust/kafka-queue-filler/internal"
)

func TestRun(t *testing.T) {
	res := generator.Run(10)

	if res != 10 {
		t.Errorf("Run: expected 10, received %d", res)
	}
}

func TestGeneration(t *testing.T) {
	var wg sync.WaitGroup

	wg.Add(1)
	res := generator.Generate(&wg)
	var msg generator.Message

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
