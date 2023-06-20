package main

import (
	"fmt"
	"runtime"

	"github.com/applejohnny/go-xgboost"
)

func main() {
	// 1. Read model
	model, err := xgboost.NewPredictor("model.json", runtime.NumCPU(), 0, 102, -1)
	if err != nil {
		panic(err)
	}

	// 2. Do predictions!
	fvals := make([]float32, 0)
	for i := 0; i < 88; i += 1 {
		fvals = append(fvals, float32(i))
	}
	p, _ := model.Predict(xgboost.FloatSliceVector(fvals))
	fmt.Printf("Prediction for %v: %f\n", fvals, p)
}
