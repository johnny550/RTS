package tests

import (
	"context"
	"fmt"
	"os"
	"rts-aws/pkg/configuration"
	"rts-aws/pkg/producer"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type ETLManagerTest struct {
	suite.Suite
}

const (
	testFile       = "./test-data/jonloyens-intermediate-data-world.csv"
	fishingFile    = "./test-data/fishing.csv"
	downloadedFile = "../../recommended-fishing-rivers-and-streams-1.csv"
)

func (suite *ETLManagerTest) SetupSuite() {
	manager := producer.New()
	assert.Equal(suite.T(), manager.DwapiClient.Token, configuration.Env.AuthToken)
	user, err := manager.DwapiClient.User.Self()
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), user.DisplayName)
}

func (suite *ETLManagerTest) TestCSVFileReader() {
	rec, err := producer.ReadFile(testFile)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), rec)
	assert.Equal(suite.T(), 2, len(rec))
}

func (suite *ETLManagerTest) TestAppendIfNotExists() {
	t := suite.T()
	testSlice := []string{"apple", "banana", "luke", "skywalker"}
	tests := []struct {
		name      string
		value     string
		assertion assert.ComparisonAssertionFunc
	}{
		{"Additional_element",
			"kiwi",
			assert.Subset,
		}, {
			"No_additional_element",
			"banana",
			assert.ElementsMatch,
		}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rst := producer.AppendIfNotExists(testSlice, test.value)
			test.assertion(t, rst, testSlice)
		})
	}

}

func (suite *ETLManagerTest) TestDataStructureMapping() {
	ts := time.Now().String()
	testData := []string{"Arnold Brook", "Brook Trout - Brown Trout*", "None", "", "Clinton", "", "", "", "-73.541982496", "44.593677719", "(44.593677719, -73.541982496)", ts}
	fishingInfo := producer.ChangeDataStructure(testData)
	assert.IsType(suite.T(), &producer.FishingInfo{}, fishingInfo)
	assert.Equal(suite.T(), testData[0], fishingInfo.Waterbody)
	assert.Equal(suite.T(), ts, fishingInfo.Timestamp)
}

func (suite *ETLManagerTest) TestDownloadingData() {
	manager := producer.New()
	err := manager.DownloadData()
	assert.Nil(suite.T(), err)
	assert.FileExists(suite.T(), downloadedFile)
}

func (suite *ETLManagerTest) TestPeparingdata() {
	header := []string{"Waterbody Name", "Fish Species Present at Waterbody", "Comments", "Special Regulations on Waterbody", "County", "Types of Public Access", "Public Fishing Access Owner", "Waterbody Information", "Longitude", "Latitude", "Location"}
	el1 := []string{"Arnold Brook", "Brook Trout - Brown Trout*", "None", "", "Clinton", "", "", "", "-73.541982496", "44.593677719", "(44.593677719, -73.541982496)"}
	el2 := []string{"cryder Blue", "Sea bass, Red sea beam", "None", "", "Clinton", "", "", "", "-73.541982496", "44.593677719", "(44.593677719, -73.541982496)"}

	testData := [][]string{}
	testData = append(testData, header, el1, el2)
	modifiedData := producer.PrepareData(0, testData)
	assert.Equal(suite.T(), 3, len(modifiedData))
	for i, data := range modifiedData {
		if i == 0 {
			assert.Contains(suite.T(), data[11], "Timestamp")
			continue
		}
		assert.Contains(suite.T(), data[11], fmt.Sprintf("%v", time.Now().Year()))
	}
}

func (suite *ETLManagerTest) TestWritingData() {
	testfile, err := os.Create("Test_file.csv")
	assert.Nil(suite.T(), err)
	header := []string{"Waterbody Name", "Fish Species Present at Waterbody", "Comments", "Special Regulations on Waterbody", "County", "Types of Public Access", "Public Fishing Access Owner", "Waterbody Information", "Longitude", "Latitude", "Location"}
	el1 := []string{"Arnold Brook", "Brook Trout - Brown Trout*", "None", "", "Clinton", "", "", "", "-73.541982496", "44.593677719", "(44.593677719, -73.541982496)"}
	el2 := []string{"cryder Blue", "Sea bass, Red sea beam", "None", "", "Clinton", "", "", "", "-73.541982496", "44.593677719", "(44.593677719, -73.541982496)"}

	type execFunction func()
	testSteps := []struct {
		name string
		fn   execFunction
	}{
		{"Write file", func() {
			testData := [][]string{header, el1, el2}
			err = producer.WriteData(testfile, testData)
			assert.Nil(suite.T(), err)
		}},
		{"Read file", func() {
			data, err := producer.ReadFile(testFile)
			assert.Nil(suite.T(), err)
			assert.NotNil(suite.T(), data)
		}},
		{
			"Delete file", func() {
				os.Remove(testfile.Name())
			}},
	}
	t := suite.T()
	for _, s := range testSteps {
		t.Run(s.name, func(t *testing.T) {
			s.fn()
		})
	}
}

func (suite *ETLManagerTest) TestReadDataExpectErr() {
	data, err := producer.ReadFile("no-such-file.txt")
	assert.NotNil(suite.T(), err)
	assert.Nil(suite.T(), data)
}

func (suite *ETLManagerTest) TestUpdatingTs() {
	manager := producer.New()
	tsStr := "2023-12-17 15:57:45"
	elts, err := time.Parse("2006-01-02 15:04:05", tsStr)
	assert.Nil(suite.T(), err)

	header := []string{"Waterbody Name", "Fish Species Present at Waterbody", "Comments", "Special Regulations on Waterbody", "County", "Types of Public Access", "Public Fishing Access Owner", "Waterbody Information", "Longitude", "Latitude", "Location", "Timestamp"}
	el1 := []string{"Arnold Brook", "Brook Trout - Brown Trout*", "None", "", "Clinton", "", "", "", "-73.541982496", "44.593677719", "(44.593677719, -73.541982496)", tsStr}
	testData := [][]string{header, el1}
	err = manager.AddTimeInfo(1, testData)
	assert.Nil(suite.T(), err)

	el1NewTs, err := time.Parse("2006-01-02 15:04:05", el1[11])
	assert.Nil(suite.T(), err)
	assert.Greater(suite.T(), el1NewTs, elts)
}

func (suite *ETLManagerTest) TestStream() {
	manager := producer.New()
	cfg, err := config.LoadDefaultConfig(context.TODO())
	assert.Nil(suite.T(), err)
	kClient := kinesis.NewFromConfig(cfg)
	header := []string{"Waterbody Name", "Fish Species Present at Waterbody", "Comments", "Special Regulations on Waterbody", "County", "Types of Public Access", "Public Fishing Access Owner", "Waterbody Information", "Longitude", "Latitude", "Location", "Timestamp"}
	el1 := []string{"Arnold Brook", "Brook Trout - Brown Trout*", "None", "", "Clinton", "", "", "", "-73.541982496", "44.593677719", "(44.593677719, -73.541982496)", "2023-12-17 15:57:45"}
	data := [][]string{header, el1}
	pk := "my-partition-key"
	wg := sync.WaitGroup{}
	wg.Add(1)
	errChan := make(chan error)
	go manager.Stream(kClient, data, pk, &wg, errChan)
	go func() {
		wg.Wait()
		close(errChan)
	}()
	assert.Nil(suite.T(), <-errChan)

}

func (suite *ETLManagerTest) TestStreamData() {
	iteratorType := "TRIM_HORIZON"
	manager := producer.New()
	err := manager.StreamData(fishingFile)
	assert.Nil(suite.T(), err)

	// SET the kinesis client
	cfg, err := config.LoadDefaultConfig(context.TODO())
	assert.Nil(suite.T(), err)
	kClient := kinesis.NewFromConfig(cfg)

	// GET the stream ARN
	streamInfo, err := kClient.DescribeStream(context.TODO(), &kinesis.DescribeStreamInput{
		StreamName: &configuration.StreamName,
	})
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), streamInfo)
	streamARN := &streamInfo.StreamDescription.StreamARN
	shardId := &streamInfo.StreamDescription.Shards[0].ShardId
	assert.NotNil(suite.T(), streamARN)

	// Get the shard iterator
	shardIteratorIn := kinesis.GetShardIteratorInput{
		ShardId:           *shardId,
		ShardIteratorType: types.ShardIteratorType(iteratorType),
		StreamName:        &configuration.StreamName,
	}
	shardIterator, err := kClient.GetShardIterator(context.TODO(), &shardIteratorIn)
	assert.Nil(suite.T(), err)

	// Get the records
	recordIn := kinesis.GetRecordsInput{
		ShardIterator: shardIterator.ShardIterator,
		StreamARN:     *streamARN,
	}
	out, err := kClient.GetRecords(context.TODO(), &recordIn)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), out)
}

func TestETLManager(t *testing.T) {
	s := new(ETLManagerTest)
	suite.Run(t, s)
}
