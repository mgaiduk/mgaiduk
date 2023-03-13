// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// large_wordcount is an example that demonstrates a more complex version
// of a wordcount pipeline. It uses a SplittableDoFn for reading the
// text files, then uses a map side input to build sorted shards.
//
// This example, large_wordcount, is the fourth in a series of five
// successively more detailed 'word count' examples. You may first want to
// take a look at minimal_wordcount and wordcount.
// Then look at debugging_worcount for some testing and validation concepts.
// After you've looked at this example, follow up with the windowed_wordcount
// pipeline, for introduction of additional concepts.
//
// Basic concepts, also in the minimal_wordcount and wordcount examples:
// Reading text files; counting a PCollection; executing a Pipeline both locally
// and using a selected runner; defining DoFns.
//
// New Concepts:
//
//  1. Using a SplittableDoFn transform to read the IOs.
//  2. Using a Map Side Input to access values for specific keys.
//  3. Testing your Pipeline via passert and metrics, using Go testing tools.
//
// This example will not be enumerating concepts, but will document them as they
// appear. There may be repetition from previous examples.
//
// To change the runner, specify:
//
//	--runner=YOUR_SELECTED_RUNNER
//
// The input file defaults to a public data set containing the text of King
// Lear, by William Shakespeare. You can override it and choose your own input
// with --input.
package main

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"

	// The imports here are for the side effect of runner registration.
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dot"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
)

var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/*.txt", "File(s) to read.")
	output = flag.String("output", "", "Output file (required). Use @* or @N (eg. @5) to indicate dynamic, or fixed number of shards. No shard indicator means a single file.")
)

// Concept: DoFn and Type Registration
// All DoFns and user types used as PCollection elements must be registered with beam.

func init() {
	register.Function2x0(extractFn)
	register.Function2x1(formatFn)
	register.Iter1[*string]()
}

// The below transforms are identical to the wordcount versions. If this was
// production code, common transforms would be placed in a separate package
// and shared directly rather than being copied.

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

// extractFn is a DoFn that emits the words in a given line.
func extractFn(line string, emit func(string)) {
	values := strings.Split(line, ",")
	text := values[1]
	for _, word := range strings.Split(text, " ") {
		emit(word)
	}
}

// formatFn is a DoFn that formats a word and its count as a string.
func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}

// CountWords is a composite transform that counts the words of an PCollection
// of lines. It expects a PCollection of type string and returns a PCollection
// of type KV<string,int>.
func CountWords(s beam.Scope, lines beam.PCollection) beam.PCollection {
	s = s.Scope("CountWords")
	col := beam.ParDo(s, extractFn, lines)
	return stats.Count(s, col)
}

// pipeline builds and executes the pipeline, returning a PCollection of strings
// representing the output files.
func Pipeline(s beam.Scope, input, output string) {
	// Since this is the whole pipeline, we don't use a subscope here.
	lines := textio.Read(s, input)
	counted := CountWords(s, lines)
	formatted := beam.ParDo(s, func(val string, cnt int) string {
		return fmt.Sprintf("%v,%v", cnt, val)
	}, counted)
	textio.Write(s, output, formatted)
	return
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	if *output == "" {
		log.Exit(ctx, "No output provided")
	}

	p := beam.NewPipeline()
	s := p.Root()
	Pipeline(s, *input, *output)

	if _, err := beamx.RunWithMetrics(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
