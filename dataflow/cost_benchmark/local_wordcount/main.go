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
	input  = flag.String("input", "simulated_data/part", "Input file prefix. gs://mgaiduk/ is appended by default")
	count  = flag.Int("count", 1, "how many file parts to read.")
	output = flag.String("output", "output.txt", "Files to write to")
	mode   = flag.String("mode", "simple", "simple or multithreaded")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	bucket := client.Bucket("mgaiduk")
	counts := make(map[string]int64)
	if *mode == "simple" {
		for i := 0; i < *count; i += 1 {
			gcsPath := fmt.Sprintf("%v%v", *input, i)
			rc, err := bucket.Object(gcsPath).NewReader(ctx)
			if err != nil {
				panic(err)
			}
			scanner := bufio.NewScanner(rc)
			// optionally, resize scanner's capacity for lines over 64K, see next example
			for scanner.Scan() {
				values := strings.Split(scanner.Text(), ",")
				text := values[1]
				for _, word := range strings.Split(text, " ") {
					counts[word] += 1
				}
			}
		}
		outputFile, err := os.Create(*output)
		if err != nil {
			panic(err)
		}
		defer outputFile.Close()
		w := bufio.NewWriter(outputFile)
		for word, count := range counts {
			w.Write([]byte(fmt.Sprintf("%v,%v\n", count, word)))
		}
	}
}
