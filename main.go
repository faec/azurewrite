// A little test utility that writes some moderately-interesting json data
// to a an Azure Events Hub
//
// Example usage: azurewrite -connection <connection string>

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	eventhubs "github.com/Azure/azure-event-hubs-go"
)

// readData reads from a github repo issues list and returns the resulting
// raw data.
func readData() []byte {
	req, err := http.NewRequest(
		"GET", "https://api.github.com/repos/elastic/beats/issues", nil)
	if err != nil {
		log.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		return bodyBytes
	}
	log.Fatal(fmt.Sprintf(
		"Couldn't read github: response code %v\n", resp.StatusCode))
	return nil
}

// Issue represents a github issue
type Issue struct {
	//URL   url.URL `json:"url"`
	ID    int    `json:"id"`
	Title string `json:"title"`
	State string `json:"state"`
	Body  string `json:"body"`
}

// Issues is an array of github issues
type Issues []Issue

// sendData interprets the given bytes as json representation of github issues
// (Issues) and sends (a few test fields of) the individual issues to the
// Events Hub with the given connection string
func sendData(bytes []byte, connection string) {
	var issues Issues
	err := json.Unmarshal(bytes, &issues)
	if err != nil {
		log.Fatal(err)
	}

	hub, err := eventhubs.NewHubFromConnectionString(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer hub.Close(ctx)
	defer cancel()
	if err != nil {
		log.Fatalf("failed to get hub %v\n", err)
	}
	ctx = context.Background()

	sent := 0
	for _, issue := range issues {
		blob, err := json.Marshal(issue)
		if err != nil {
			log.Print(err)
			continue
		}
		err = hub.Send(ctx, eventhubs.NewEventFromString(string(blob)))

		if err != nil {
			log.Print(err)
			continue
		}
		sent++
	}
	fmt.Printf("%v / %v messages sent\n", sent, len(issues))
}

func main() {
	connection := flag.String("connection", "", "Event Hubs connection string")

	flag.Parse()
	if *connection == "" {
		log.Fatal("Connection string must be provided.\n" +
			"Usage: azurewrite -connection <connection string>")
	}
	bytes := readData()
	sendData(bytes, *connection)
}
