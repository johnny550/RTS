package main

import (
	"rts-aws/pkg/producer"
)

func main() {
	manager := producer.New()
	logger := manager.Logger
	err := manager.DownloadData()
	if err != nil {
		logger.Sugar().Fatalf("exiting with %v\n", err)
	}
	emptyData := [][]string{}
	err = manager.AddTimeInfo(0, emptyData)
	if err != nil {
		logger.Sugar().Fatalf("exiting with %v\n", err)
	}
	for {
		err = manager.StreamData("")
		if err != nil {
			logger.Sugar().Fatalf("%v\n", err)
		}
	}
}
