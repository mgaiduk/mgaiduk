package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	input  = flag.String("input", "gs://mgaiduk-us-central1/live_xgb_dataset2/json/part*.json", "Input with events for counters. Avro format")
	output = flag.String("output", "gs://mgaiduk-us-central1/live_xgb_dataset3/csv_gzip/part*", "Output. Will contain target events from input + joined features")
)

type Event struct {
	LivestreamId string `csv:"livestreamId"`
	HostId       string `csv:"hostId"`
	MemberId     string `csv:"memberId"`
	event_time   int64  `csv:"event_time"`
	event_name   string `csv:"event_name"`
	duration_ms  int64  `csv:"duration_ms"`
}

type DecayCounter struct {
	Value         float64
	DecayInterval time.Duration
	LastUpdate    time.Time
	FieldName     string
	EventName     string
}

func NewDecayCounter(decayInterval time.Duration, fieldName, eventName string) DecayCounter {
	return DecayCounter{
		Value:         0.0,
		DecayInterval: decayInterval,
		LastUpdate:    time.Unix(0, 0),
		FieldName:     fieldName,
		EventName:     eventName,
	}
}

func (c *DecayCounter) Decay(eventTime time.Time) {
	timeDelta := eventTime.Sub(c.LastUpdate)
	ratio := timeDelta.Seconds() / c.DecayInterval.Seconds()
	c.Value *= math.Exp(-ratio)
	c.LastUpdate = eventTime
}

func (c *DecayCounter) ConsumeEvent(event map[string]any) {
	event_name := event["event_name"].(string)
	if event_name != c.EventName {
		return
	}
	event_ts := event["event_time"].(int64)
	eventTime := time.Unix(event_ts/1_000, 0)
	if c.LastUpdate.After(eventTime) {
		panic(fmt.Errorf("Unsorted timestamps detected!"))
	}
	c.Decay(eventTime)
	value := event[c.FieldName].(int64)
	c.Value += float64(value)
}

func MakeCounters() []DecayCounter {
	result := make([]DecayCounter, 0, 1)
	result = append(result, NewDecayCounter(time.Second, "duration_ms", "view_end"))
	return result
}

type reduceFn struct {
	Gap time.Duration
}

func (r reduceFn) ProcessElement(key string, input func(*map[string]any) bool, emit func(map[string]any)) {
	var event map[string]any
	events := make([]map[string]any, 0)
	for input(&event) {
		// apply gap to simulate runtime data processing delays
		event_ts := event["event_time"].(int64)
		event_name := event["event_name"].(string)
		if event_name != "timespent_target" {
			event_ts += int64(r.Gap.Milliseconds())
		}
		event["event_time"] = event_ts
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i]["event_time"].(int64) < events[j]["event_time"].(int64)
	})
	counters := MakeCounters()
	for _, event := range events {
		event_name := event["event_name"].(string)
		if event_name == "timespent_target" {
			joined_features := event["joined_features"].([]float64)
			for i := range counters {
				joined_features = append(joined_features, counters[i].Value)
			}
			event["joined_features"] = joined_features
			emit(event)
		} else {
			for i := range counters {
				counters[i].ConsumeEvent(event)
			}
		}
	}
}

func ParseFn(event string, emit func(map[string]any)) {
	if f {
		fmt.Printf("%v\n", event)
		f = false
	}
	var result map[string]any
	json.Unmarshal([]byte(event), &result)
	for _, field := range []string{"event_time", "duration_ms", "like_counter", "gift_cheers_value", "gift_quantity"} {
		str := result[field].(string)
		converted, err := strconv.ParseInt(str, 0, 64)
		if err != nil {
			panic(err)
		}
		result[field] = converted
	}
	result["joined_features"] = make([]float64, 0)
	_, ok := result["livestreamId"].(string)
	if ok {
		emit(result)
	}
}

var f = true

func main() {
	flag.Parse()
	beam.Init()
	if *output == "" {
		log.Fatal("No output provided")
	}
	p := beam.NewPipeline()
	s := p.Root()
	// typeOf string will return json data
	features := textio.Read(s, *input)
	parsed := beam.ParDo(s, ParseFn, features)
	// reduce by key
	key := "hostId"
	grouped := beam.GroupByKey(s, beam.ParDo(s, func(event map[string]any) (string, map[string]any) {
		k := event[key].(string)
		return k, event
	}, parsed))
	reduced := beam.ParDo(s, &reduceFn{Gap: time.Hour}, grouped)
	formatted := beam.ParDo(s, func(event map[string]any) string {
		js, err := json.Marshal(event)
		if err != nil {
			panic(err)
		}
		return string(js)
	}, reduced)
	textio.Write(s, *output, formatted)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
