package chainedkv

import (
	"net"
	"net/rpc"
	"github.com/DistributedClocks/tracing"
)

// Actions to be recorded by coord (as part of ctrace, ktrace, and strace):

type CoordStart struct {
}

type ServerFail struct {
	ServerId uint8
}

type ServerFailHandledRecvd struct {
	FailedServerId   uint8
	AdjacentServerId uint8
}

type NewChain struct {
	Chain []uint8
}

type AllServersJoined struct {
}

type HeadReqRecvd struct {
	ClientId string
}

type HeadRes struct {
	ClientId string
	ServerId uint8
}

type TailReqRecvd struct {
	ClientId string
}

type TailRes struct {
	ClientId string
	ServerId uint8
}

type ServerJoiningRecvd struct {
	ServerId uint8
}

type ServerJoinedRecvd struct {
	ServerId uint8
}

type CoordConfig struct {
	ClientAPIListenAddr string
	ServerAPIListenAddr string
	LostMsgsThresh      uint8
	NumServers          uint8
	TracingServerAddr   string
	Secret              []byte
	TracingIdentity     string
}

type Head struct {
	Head string
}

type Tail struct {
	Tail string
}

type ClientArgs struct {
	ClientId string
	ClientAddr string
	KToken tracing.TracingToken
}

type ClientRes struct {
	ServerId uint8
	ServerAddr string
	KToken tracing.TracingToken
}

type ServerJoiningArgs struct {
	serverId uint8
	sToken tracing.TracingToken
}

type ServerJoiningRes struct {
	tailServerListenAddr string
	sToken tracing.TracingToken
}

type ServerJoinedArgs struct {
	serverId         uint8
	ackAddr          string
	coordListenAddr  string // coord -> this server
	serverListenAddr string // (caller) server -> this server
	sToken           tracing.TracingToken
}

type ServerJoinedRes struct {
	sToken tracing.TracingToken
}

type Coord struct {
	// Coord state may go here
	Tracer *tracing.Tracer
	Trace *tracing.Trace
	NumServers uint8
	AvailableServers uint8
	AllJoined bool
	Head ServerInfo
	Tail ServerInfo
	Chain [16]ServerInfo
}

type ServerInfo struct {
	ServerAddr string
	ServerId uint8
}

func NewCoord() *Coord {
	return &Coord{}
}

func (c *Coord) Start(clientAPIListenAddr string, serverAPIListenAddr string, lostMsgsThresh uint8, numServers uint8, ctrace *tracing.Tracer) error {

	c.Tracer = ctrace
	c.Trace = ctrace.CreateTrace()
	trace := c.Trace
	cStart := CoordStart{}
	trace.RecordAction(cStart)

	c.AvailableServers = 0
	c.NumServers = numServers
	c.AllJoined = false

	lnClient, err := net.Listen("tcp", clientAPIListenAddr)
	if err != nil {
		return err
	}
	lnServer, err := net.Listen("tcp", serverAPIListenAddr)
	if err != nil {
		return err
	}
	
	rpc.Register(c)

	go rpc.Accept(lnClient)
	go rpc.Accept(lnServer)

	for true {

	}

	return nil
}

func (c *Coord) OnServerJoining(serverJoiningArgs *ServerJoiningArgs, serverJoiningRes *ServerJoiningRes) error {
	trace := c.Tracer.ReceiveToken(serverJoiningArgs.sToken)
	trace.RecordAction(ServerJoiningRecvd{
		ServerId: serverJoiningArgs.serverId,
	})

	// Simple blocking until its the servers turn to join (Poor Efficieny?)
	for c.Tail.ServerId+1 != serverJoiningArgs.serverId {} 

	*serverJoiningRes = ServerJoiningRes{
		tailServerListenAddr: c.Tail.ServerAddr, // TailAddr can be nil on empty chain, server should recognize this
		sToken: trace.GenerateToken(),
	}
	return nil
}

func (c *Coord) OnJoined(serverJoinedArgs *ServerJoinedArgs, serverJoinedRes *ServerJoinedRes) error {
	trace := c.Tracer.ReceiveToken(serverJoinedArgs.sToken)
	trace.RecordAction(ServerJoinedRecvd{
		ServerId: serverJoinedArgs.serverId,
	})

	c.Chain[c.AvailableServers] = ServerInfo{
		ServerAddr: serverJoinedArgs.serverListenAddr,
		ServerId: serverJoinedArgs.serverId,
	}
	c.AvailableServers++
	traceChain := NewChain{Chain: []uint8{}}
	for i := range c.Chain {
		traceChain.Chain = append(traceChain.Chain, c.Chain[i].ServerId)
	}
	trace = c.Trace
	trace.RecordAction(traceChain)
	// Ack?

	if c.AvailableServers == c.NumServers && c.AllJoined == false {
		trace.RecordAction(AllServersJoined{})
		c.AllJoined = true
	}

	// TODO: Start running fcheck (async) on this new server (new UDP connection)
	// TODO: On failure, run c.OnServerFailure(failedServer)

	return nil
}

func (c *Coord) OnServerFailure(failedServer ServerInfo) {
	// TODO: Remove server from the chain
	// TODO: RPC servers that need to change their prev/next
}

func (c *Coord) GetHead(args *ClientArgs, headAddr *string) error {
	trace := c.Tracer.ReceiveToken(args.KToken)
	req := HeadReqRecvd{
		ClientId: args.ClientId,
	}
	trace.RecordAction(req)

	// Simple blocking until all servers join (Poor Efficiency?)
	for c.AllJoined == false {}

	res := HeadRes{}
	res.ClientId = args.ClientId
	res.ServerId = c.Head.ServerId
	trace.RecordAction(res)

	*headAddr = c.Head.ServerAddr
	return nil
}

func (c *Coord) GetTail(args *ClientArgs, tailAddr *string) error {
	trace := c.Tracer.ReceiveToken(args.KToken)
	req := TailReqRecvd{
		ClientId: args.ClientId,
	}
	trace.RecordAction(req)

	// Simple blocking until all servers join (Poor Efficiency?)
	for c.AllJoined == false {}

	res := TailRes{}
	res.ClientId = args.ClientId
	res.ServerId = c.Tail.ServerId
	trace.RecordAction(res)

	*tailAddr = c.Tail.ServerAddr
	return nil
}