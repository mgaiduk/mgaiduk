package main

import (
	"context"
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

func init() {
	mapreduce.Register(&WordCount{})
}

type WordSplit struct{}

type WordCount struct{}

type InputRow struct {
	Idx  string `yson:"idx"`
	Text string `yson:"text"`
}

type SplittedRow struct {
	Word string `yson:"word"`
}

type CountRow struct {
	Word  string `yson:"word"`
	Count int    `yson:"count"`
}

func (*WordSplit) InputTypes() []interface{} {
	return []interface{}{&InputRow{}}
}

func (*WordSplit) OutputTypes() []interface{} {
	return []interface{}{&SplittedRow{}}
}

func (*WordCount) InputTypes() []interface{} {
	return []interface{}{&SplittedRow{}}
}

func (*WordCount) OutputTypes() []interface{} {
	return []interface{}{&CountRow{}}
}

func (*WordSplit) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	for in.Next() {
		var row InputRow
		in.MustScan(&row)
		for _, word := range strings.Split(row.Text, " ") {
			outRow := SplittedRow{
				Word: word,
			}
			out[0].MustWrite(&outRow)
		}
	}

	return nil
}

func (*WordCount) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	return mapreduce.GroupKeys(in, func(in mapreduce.Reader) error {
		counts := make(map[string]int64)
		for in.Next() {
			var row SplittedRow
			in.MustScan(&row)
			counts[row.Word] += 1
		}
		for word, cnt := range counts {
			var result CountRow
			result.Word = word
			result.Count = cnt
			out[0].MustWrite(&result)
		}
		return nil
	})
}

func Example() error {
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:             "localhost:8000",
		ReadTokenFromFile: true,
	})
	if err != nil {
		return err
	}

	mr := mapreduce.New(yc)
	inputTable := ypath.Path("//home/tmp/simulated_data")
	outputTable := ypath.Path("//home/tmp/simulated_data_output")

	_, err = yt.CreateTable(context.Background(), yc, sortedTable)
	if err != nil {
		return err
	}

	_, err = yt.CreateTable(context.Background(), yc, outputTable, yt.WithInferredSchema(&CountRow{}))
	if err != nil {
		return err
	}

	op, err = mr.MapReduce(&WordSplit{}, &WordCount{}, spec.Reduce().
		ReduceByColumns("word").
		AddInput(inputTable).
		AddOutput(outputTable))
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %s\n", yt.WebUIOperationURL("localhost:8000", op.ID()))
	err = op.Wait()
	if err != nil {
		return err
	}

	fmt.Printf("Output table: %s\n", yt.WebUITableURL("localhost:8000", outputTable))
	return nil
}

func main() {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}

	if err := Example(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
}