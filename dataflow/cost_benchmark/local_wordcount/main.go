package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/storage"
)

var PATH = "/Users/maksimgaiduk/repos/mgaiduk/dataflow/cost_benchmark/gen_random_data/simulated_data/part0"
var (
	input  = flag.String("input", "simulated_data/part0", "File(s) to read. gs://mgaiduk/ is appended by default")
	output = flag.String("output", "output.txt", "Files to write to")
)

func main() {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	rc, err := client.Bucket("mgaiduk").Object(*input).NewReader(ctx)
	if err != nil {
		panic(err)
	}
	outputFile, err := os.Create(*output)
	defer outputFile.Close()
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(rc)
	counts := make(map[string]int64)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		values := strings.Split(scanner.Text(), ",")
		text := values[1]
		for _, word := range strings.Split(text, " ") {
			counts[word] += 1
		}
	}
	w := bufio.NewWriter(outputFile)
	for word, count := range counts {
		w.Write([]byte(fmt.Sprintf("%v,%v\n", count, word)))
	}
}
