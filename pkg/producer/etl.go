package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"rts-aws/pkg/configuration"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func (m *ClientManager) AddTimeInfo(targetRecordIndex int, records [][]string) (err error) {
	filename := configuration.Env.TargetFile
	if len(records) == 0 {
		records, err = ReadFile(filename)
		if err != nil {
			return
		}
	}
	// modifiedData contains time info for each record
	modifiedData := PrepareData(targetRecordIndex, records)
	// re-use the original file
	targetFile, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error creating new CSV file:", err)
		return
	}
	defer targetFile.Close()
	// write the data
	return WriteData(targetFile, modifiedData)
}

func (m *ClientManager) StreamData(sourceFile string) (err error) {
	if sourceFile == "" {
		sourceFile = configuration.Env.TargetFile
	}
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return fmt.Errorf("unable to load SDK config %v", err)
	}
	kClient := kinesis.NewFromConfig(cfg)
	data, err := ReadFile(sourceFile)
	if err != nil {
		return
	}
	// 1 shard in stream, so 1 partition key is enough
	pk := fmt.Sprint(len(data)) + "pk"
	var wg sync.WaitGroup

	ticker := time.NewTicker(time.Second / 10) //every 100th ms
	errChan := make(chan error)
	for i := 0; i <= 10; i++ {
		wg.Add(1)
		go m.Stream(kClient, data, pk, &wg, errChan)
		<-ticker.C
	}
	go func() {
		wg.Wait()
		close(errChan)
	}()
	return <-errChan
}

func (m *ClientManager) Stream(kinesisClient *kinesis.Client, data [][]string, pk string, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()
	logger := m.Logger
	for i, d := range data {
		// skip the header row
		if i == 0 {
			continue
		}
		// structure the data
		structuredData := ChangeDataStructure(d)
		rec, _ := json.Marshal(structuredData)
		putRecordOutput, err := kinesisClient.PutRecord(context.TODO(), &kinesis.PutRecordInput{
			Data:         rec,
			StreamName:   &configuration.StreamName,
			PartitionKey: &pk,
		})
		if err != nil {
			logger.Sugar().Errorf("failed to put record in K stream %v\n", err)
			// if one fails, continue
			continue
		} else {
			logger.Sugar().Infof("Successfully added record to sequence number %v\n", &putRecordOutput.SequenceNumber)
		}

		if err := m.AddTimeInfo(i, data); err != nil {
			errChan <- err
		}
	}
}
