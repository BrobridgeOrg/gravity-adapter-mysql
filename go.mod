module git.brobridge.com/gravity/gravity-adapter-mysql

go 1.15

require (
	github.com/BrobridgeOrg/broton v0.0.2
	github.com/BrobridgeOrg/gravity-api v0.2.11
	github.com/BrobridgeOrg/gravity-sdk v0.0.2
	github.com/cfsghost/parallel-chunked-flow v0.0.6
	github.com/json-iterator/go v1.1.10
	github.com/pkg/errors v0.9.1
	github.com/siddontang/go-mysql v1.1.0
	github.com/sirupsen/logrus v1.7.0
	github.com/spf13/viper v1.7.1
)

//replace github.com/BrobridgeOrg/gravity-sdk => ./gravity-sdk

//replace github.com/cfsghost/parallel-chunked-flow => ./parallel-chunked-flow
