package producer

import (
	"rts-aws/pkg/configuration"

	"github.com/datadotworld/dwapi-go/dwapi"
	"go.uber.org/zap"
)

type ClientManager struct {
	DwapiClient *dwapi.Client
	Logger      *zap.Logger
}

func New() ClientManager {
	token := configuration.Env.AuthToken
	logger, err := zap.NewDevelopment()
	logger.Level()
	if err != nil {
		return ClientManager{}
	}
	return ClientManager{
		DwapiClient: dwapi.NewClient(token),
		Logger:      logger,
	}
}
