package tests

import (
	"rts-aws/pkg/configuration"
	"rts-aws/pkg/producer"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type ClientManagerTest struct {
	suite.Suite
}

func (suite *ClientManagerTest) TestNewClient() {
	manager := producer.New()
	assert.NotNil(suite.T(), manager.DwapiClient.BaseURL)
	assert.Equal(suite.T(), configuration.Env.AuthToken, manager.DwapiClient.Token)
	user, err := manager.DwapiClient.User.Self()
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), user.DisplayName)
}

func (suite *ClientManagerTest) TestLogger() {
	manager := producer.New()
	assert.NotNil(suite.T(), manager.Logger)
	assert.Empty(suite.T(), manager.Logger.Name())
}

func TestClient(t *testing.T) {
	s := new(ClientManagerTest)
	suite.Run(t, s)
}
