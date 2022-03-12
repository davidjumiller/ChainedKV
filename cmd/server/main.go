package main

import (
	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/util"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"os"
)

func main() {
	serverId := os.Args[1]
	filename := fmt.Sprintf("config/server_config_%s.json", serverId)
	var config chainedkv.ServerConfig
	util.ReadJSONConfig(filename, &config)
	stracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	server := chainedkv.NewServer()
	server.Start(config.ServerId, config.CoordAddr, config.ServerAddr, config.ServerServerAddr, config.ServerListenAddr, config.ClientListenAddr, stracer)
}
