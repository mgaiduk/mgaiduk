package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var FILES_COUNT = 100
var LINES_COUNT = 500000
var WORDS_COUNT = 100

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {
	for i := 0; i < FILES_COUNT; i += 1 {
		f, err := os.Create(fmt.Sprintf("simulated_data/part%v", i))
		if err != nil {
			panic(err)
		}
		defer f.Close()
		// f.Write([]byte("idx,text\n"))
		for j := 0; j < LINES_COUNT; j += 1 {
			f.Write([]byte(fmt.Sprintf("%v,", j)))
			for k := 0; k < WORDS_COUNT; k += 1 {
				wordLen := 1 + rand.Intn(3)
				word := randSeq(wordLen)
				f.Write([]byte(word))
				f.Write([]byte(" "))
			}
			f.Write([]byte("\n"))
		}
	}
}
