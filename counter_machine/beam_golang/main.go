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

	"encoding/gob"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	input  = flag.String("input", "gs://mgaiduk-us-central1/live_xgb_dataset2/json/part*.json", "Input with events for counters. Avro format")
	output = flag.String("output", "gs://mgaiduk-us-central1/live_xgb_dataset3/csv_gzip/part*", "Output. Will contain target events from input + joined features")
)

type TNode = map[string]any

func ToString(node TNode) string {
	for _, field := range []string{"event_time", "duration_ms", "like_counter", "gift_cheers_value", "gift_quantity"} {
		number := node[field].(int64)
		converted := fmt.Sprintf("%v", number)
		node[field] = converted
	}
	bytes, err := json.Marshal(node)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func NewTNode(s string) TNode {
	var node TNode
	err := json.Unmarshal([]byte(s), &node)
	for _, field := range []string{"event_time", "duration_ms", "like_counter", "gift_cheers_value", "gift_quantity"} {
		str := node[field].(string)
		converted, err := strconv.ParseInt(str, 0, 64)
		if err != nil {
			panic(err)
		}
		node[field] = converted
	}
	if err != nil {
		panic(err)
	}
	return node
}

func init() {
	register.DoFn3x0[string, func(*string) bool, func(string)](&reduceFn{})
	gob.Register(map[string]interface{}{})
}

type DecayCounter struct {
	Value         float64
	DecayInterval time.Duration
	LastUpdate    time.Time
	EventName     string
	Name          string
	GetValue      func(TNode) float64
}

func NewDecayCounter(decayInterval time.Duration, eventName, name string, getValue func(TNode) float64) DecayCounter {
	return DecayCounter{
		Value:         0.0,
		DecayInterval: decayInterval,
		LastUpdate:    time.Unix(0, 0),
		EventName:     eventName,
		Name:          name,
		GetValue:      getValue,
	}
}

func (c *DecayCounter) Decay(eventTime time.Time) {
	timeDelta := eventTime.Sub(c.LastUpdate)
	ratio := timeDelta.Seconds() / c.DecayInterval.Seconds()
	c.Value *= math.Exp(-ratio)
	c.LastUpdate = eventTime
}

func (c *DecayCounter) ConsumeEvent(event TNode) {
	event_name := event["event_name"].(string)
	if event_name != c.EventName {
		return
	}
	event_ts := event["event_time"].(int64)
	eventTime := time.Unix(event_ts/1_000, 0)
	if c.LastUpdate.After(eventTime) {
		panic(fmt.Errorf("unsorted timestamps detected"))
	}
	c.Decay(eventTime)
	value := c.GetValue(event)
	c.Value += float64(value)
}

func MakeCounters() []DecayCounter {
	result := make([]DecayCounter, 0, 1)
	for _, decay := range []time.Duration{time.Hour * 24 * 30} {
		// timespent
		result = append(result, NewDecayCounter(decay, "view_end", fmt.Sprintf("timespent_decay_%v", decay), func(event TNode) float64 {
			return float64(event["duration_ms"].(int64))
		}))
		// lives count
		result = append(result, NewDecayCounter(decay, "view_end", fmt.Sprintf("lives_count_%v", decay), func(event TNode) float64 {
			duration_ms := float64(event["duration_ms"].(int64))
			if duration_ms > 1500*60 { // 1.5 mins
				return 1.0
			} else {
				return 0.0
			}
		}))
		// likes
		result = append(result, NewDecayCounter(decay, "like", fmt.Sprintf("like_count_%v", decay), func(event TNode) float64 {
			return float64(event["like_counter"].(int64))
		}))
		// gifting
		result = append(result, NewDecayCounter(decay, "gift", fmt.Sprintf("gift_count_%v", decay), func(event TNode) float64 {
			return float64(event["gift_quantity"].(int64))
		}))
		result = append(result, NewDecayCounter(decay, "gift", fmt.Sprintf("gift_value_%v", decay), func(event TNode) float64 {
			return float64(event["gift_cheers_value"].(int64) * event["gift_quantity"].(int64))
		}))
		// shares
		result = append(result, NewDecayCounter(decay, "share", fmt.Sprintf("shares_%v", decay), func(event TNode) float64 {
			return 1.0
		}))
		// comments
		result = append(result, NewDecayCounter(decay, "comment", fmt.Sprintf("comments_%v", decay), func(event TNode) float64 {
			return 1.0
		}))
	}

	return result
}

type reduceFn struct {
	Gap    time.Duration
	Keys   []string
	IsLast bool
}

var trainStartTime = 1679961600 // Tue 28 Mar 2023 01:00:00 BST

func (r *reduceFn) ProcessElement(key string, input func(*string) bool, emit func(string)) {
	feature_prefix := "feature_"
	for _, k := range r.Keys {
		feature_prefix += k
		feature_prefix += "_"
	}
	feature_prefix += fmt.Sprintf("%v_", r.Gap)
	var eventStr string
	events := make([]TNode, 0)
	for input(&eventStr) {
		event := NewTNode(eventStr)
		event_ts := event["event_time"].(int64)
		event_name := event["event_name"].(string)
		if event_name == "timespent_target" {
			if event_ts < int64(trainStartTime) {
				continue
			}
		} else {
			// these events are needed for future iterations of ParDo, but not for the final results
			emit(eventStr)
			// apply gap to simulate runtime data processing delays
			event_ts += int64(r.Gap.Seconds())
		}
		event["event_time"] = event_ts
		// nasty bug was here with passing initial map which is a pointer!
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i]["event_time"].(int64) < events[j]["event_time"].(int64)
	})
	counters := MakeCounters()
	for _, event := range events {
		event_name := event["event_name"].(string)
		if event_name == "timespent_target" {
			for i := range counters {
				feature_name := feature_prefix + counters[i].Name
				event[feature_name] = counters[i].Value
			}
			out := ToString(event)
			emit(out)
		} else {
			for i := range counters {
				counters[i].ConsumeEvent(event)
			}
		}
	}
}

func ParseFn(event string, emit func(string)) {
	// some debug print
	if f {
		fmt.Printf("%v\n", event)
		f = false
	}
	var result TNode
	json.Unmarshal([]byte(event), &result)
	_, ok := result["livestreamId"].(string)
	if ok {
		bytes, err := json.Marshal(result)
		if err != nil {
			panic(err)
		}
		emit(string(bytes))
	}
}

func FilterFn(eventStr string, emit func(string)) {
	event := NewTNode(eventStr)
	event_name := event["event_name"].(string)
	if event_name == "timespent_target" {
		// only need events used for training in the result
		emit(eventStr)
	}
}

var f = true

func add_features(s beam.Scope, col beam.PCollection, keys []string, gap time.Duration) beam.PCollection {
	grouped := beam.GroupByKey(s, beam.ParDo(s, func(eventStr string) (string, string) {
		event := NewTNode(eventStr)
		groupingKey := ""
		for _, k := range keys {
			groupingKey += event[k].(string)
			groupingKey += "£" // sentinel
		}
		return groupingKey, ToString(event)
	}, col))
	reduced := beam.ParDo(s, &reduceFn{Gap: gap, Keys: keys}, grouped)
	return reduced
}

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
	for _, keys := range [][]string{
		{"hostId"},
		{"memberId"},
		{"livestreamId"},
		{"hostId", "memberId"},
	} {
		for _, gap := range []time.Duration{
			time.Minute,
			time.Hour,
			time.Hour * 24} {
			parsed = add_features(s, parsed, keys, gap)
		}
	}
	filtered := beam.ParDo(s, FilterFn, parsed)
	textio.Write(s, *output, filtered)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
