package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")
	output = flag.String("output", "", "Output file (required).")
)

func init() {
	register.Function2x0(ExtractCgRanks)
	register.Emitter1[BqLine]()
}

type BqLine struct {
	Count           int    `json:"cnt,string"`
	CgRanks         string `json:"cg_ranks"`
	IsLiked         string `json:"isLiked"`
	IsShared        string `json:"isShared"`
	IsProfileOpened string `json:"isProfileOpened"`
	IsDownloaded    string `json:"isDownloaded"`
	IsFollowed      string `json:"isFollowed"`
	Vplay98         bool   `json:"vplay98"`
}

type ParsedLine struct {
	Count           int
	Pos             int
	CgRank          string
	IsLiked         bool
	IsShared        bool
	IsProfileOpened bool
	IsDownloaded    bool
	IsFollowed      bool
	Vplay98         bool
}

func ExtractCgRanks(line string, emit func(string, ParsedLine)) {
	var b BqLine
	err := json.Unmarshal([]byte(line), &b)
	if err != nil {
		fmt.Printf("Failed to unmarshal: %v\n", line)
		panic(err)
	}
	var m map[string]string
	err = json.Unmarshal([]byte(b.CgRanks), &m)
	if err != nil {
		fmt.Printf("Failed to unmarshal cg_ranks %v\n", b.CgRanks)
		panic(err)
	}
	isLiked, err := strconv.ParseBool(b.IsLiked)
	if err != nil {
		fmt.Printf("Failed to unmarshal isLiked %v\n", b.IsLiked)
		panic(err)
	}
	isShared, err := strconv.ParseBool(b.IsShared)
	if err != nil {
		fmt.Printf("Failed to unmarshal isShared %v\n", b.IsShared)
		panic(err)
	}
	isProfileOpened, err := strconv.ParseBool(b.IsProfileOpened)
	if err != nil {
		fmt.Printf("Failed to unmarshal isProfileOpened %v\n", b.IsProfileOpened)
		panic(err)
	}
	isDownloaded, err := strconv.ParseBool(b.IsDownloaded)
	if err != nil {
		fmt.Printf("Failed to unmarshal isDownloaded %v\n", b.IsDownloaded)
		panic(err)
	}
	isFollowed, err := strconv.ParseBool(b.IsFollowed)
	if err != nil {
		fmt.Printf("Failed to unmarshal isFollowed %v\n", b.IsFollowed)
		panic(err)
	}
	for k, v := range m {
		pos, err := strconv.Atoi(v)
		if err != nil {
			fmt.Printf("Failed to convert position to int %v\n", v)
			panic(err)
		}
		cgRank := simplifyCgRank(k)
		p := ParsedLine{
			Count:           b.Count,
			Pos:             pos,
			CgRank:          cgRank,
			IsLiked:         isLiked,
			IsShared:        isShared,
			IsProfileOpened: isProfileOpened,
			IsDownloaded:    isDownloaded,
			IsFollowed:      isFollowed,
			Vplay98:         b.Vplay98,
		}
		// to reduce by all these fields
		key := fmt.Sprintf("%s-%v-%v-%v-%v-%v-%v-%v", cgRank, pos, isLiked, isShared, isProfileOpened, isDownloaded, isFollowed, p.Vplay98)
		emit(key, p)
	}
}

func simplifyCgRank(cgRank string) string {
	// sharecone-weighted-Hindi-interaction_2kth_3day-[1]
	for _, shareconePrefix := range []string{"sharecone-weighted-", "sharecone-weightedlag-"} {
		if strings.HasPrefix(cgRank, shareconePrefix) {
			cgRank = strings.TrimPrefix(cgRank, shareconePrefix)
			_, cgRank, _ = strings.Cut(cgRank, "-")
			return fmt.Sprintf("%s%s", shareconePrefix, cgRank)
		}
	}
	if strings.Contains(cgRank, "latestMinView") {
		return "latestMinView"
	}
	return cgRank
}

func ReduceFn(key string, iter func(*ParsedLine) bool) ParsedLine {
	var p ParsedLine
	cnt := 0
	for iter(&p) {
		cnt += p.Count
	}
	p.Count = cnt
	return p
}

func main() {
	flag.Parse()
	beam.Init()

	if *output == "" {
		log.Fatal("No output provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	parsed := beam.ParDo(s, ExtractCgRanks, lines)
	grouped := beam.GroupByKey(s, parsed)
	reduced := beam.ParDo(s, ReduceFn, grouped)
	formatted := beam.ParDo(s, func(p ParsedLine) string {
		return fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v", p.Count, p.Pos, p.CgRank, p.IsLiked, p.IsShared, p.IsProfileOpened, p.IsDownloaded, p.IsFollowed, p.Vplay98)
	}, reduced)
	textio.Write(s, *output, formatted)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
