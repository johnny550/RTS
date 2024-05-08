package producer

import (
	"encoding/csv"
	"os"
	"rts-aws/pkg/configuration"
	"time"
)

type FishingInfo struct {
	Waterbody     string
	FishSpecies   string
	Comments      string
	WaterBodyReg  string
	County        string
	Access        string
	Owner         string
	WaterbodyInfo string
	Longitude     string
	Latitude      string
	Location      string
	Timestamp     string
}

func (m *ClientManager) DownloadData() (err error) {
	logger := m.Logger
	dw := m.DwapiClient
	owner := configuration.Env.Owner
	datasetid := configuration.Env.DatasetID
	filename := configuration.Env.TargetFile
	savePath := filename
	logger.Sugar().Infof("Saving into %s", filename)
	_, err = dw.File.DownloadAndSave(owner, datasetid, filename, savePath)
	return err
}

func ReadFile(filename string) (records [][]string, err error) {
	// Open the existing CSV file for reading
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()
	// Read the CSV data
	reader := csv.NewReader(file)
	records, err = reader.ReadAll()
	if err != nil {
		return
	}
	return
}

func AppendIfNotExists(h []string, el string) []string {
	reference := make(map[string]bool)
	for _, v := range h {
		reference[v] = true
	}
	if !reference[el] {
		h = append(h, el)
	}
	return h
}

func WriteData(targetFile *os.File, data [][]string) error {
	writer := csv.NewWriter(targetFile)
	defer writer.Flush()
	return writer.WriteAll(data)
}

func PrepareData(targetRecordIndex int, data [][]string) [][]string {
	records := make([][]string, len(data))
	copy(records, data)
	// Add a new column header
	records[0] = AppendIfNotExists(records[0], "Timestamp")
	currentTs := time.Now().Format("2006-01-02 15:04:05")
	if targetRecordIndex > 0 {
		// Modify the timestamp for a specific row. After uploading it to kinesis
		records[targetRecordIndex][11] = currentTs
	} else {
		// Add values to the new column, for all rows. Typically used to avoid having errors when reading the file after adding only the timestamp column
		for i := 1; i < len(records); i++ {
			records[i] = append(records[i], currentTs)
		}
	}
	return records
}

func ChangeDataStructure(data []string) *FishingInfo {
	return &FishingInfo{data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11]}
}
