package configuration

import (
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
)

type Config struct {
	AuthToken  string `default:""`
	Logger     *zap.Logger
	TargetFile string `default:"recommended-fishing-rivers-and-streams-1.csv"`
	DatasetID  string `default:"jcxg-7gnm"`
	Owner      string `default:"data-ny-gov"`
}

var (
	Env        Config
	StreamName = "rts-test"
)

func init() {
	Env.Logger, _ = zap.NewProduction()
	defer Env.Logger.Sync()
	// ENV vars
	if err := envconfig.Process("", &Env); err != nil {
		Env.Logger.Error(err.Error())
	}
}
