package chainedkv

import (
	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"
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

type Coord struct {
	// Coord state may go here
	Tracer *tracing.Tracer
	Trace *tracing.Trace
	lostMsgsThresh uint8
	NumServers uint8
	AvailableServers uint8
	AllJoined bool
	Chain []ServerInfo
}

type ServerInfo struct {
	ServerAddr string
	ServerId uint8
}

func NewCoord() *Coord {
	return &Coord{
		Chain: []ServerInfo{},
	}
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
	c.lostMsgsThresh = lostMsgsThresh

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

	tail := c.Chain[c.AvailableServers]

	// Simple blocking until its the servers turn to join (Poor Efficieny?)
	for tail.ServerId+1 != serverJoiningArgs.serverId {} 

	*serverJoiningRes = ServerJoiningRes{
		tailServerListenAddr: tail.ServerAddr, // TailAddr can be nil on empty chain, server should recognize this
		sToken: trace.GenerateToken(),
	}
	return nil
}

func (c *Coord) OnJoined(serverJoinedArgs *ServerJoinedArgs, serverJoinedRes *ServerJoinedRes) error {
	trace := c.Tracer.ReceiveToken(serverJoinedArgs.sToken)
	trace.RecordAction(ServerJoinedRecvd{
		ServerId: serverJoinedArgs.serverId,
	})

	serverInfo := ServerInfo{
		ServerAddr: serverJoinedArgs.serverListenAddr,
		ServerId: serverJoinedArgs.serverId,
	}
	c.Chain[c.AvailableServers] = serverInfo

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
	startStruct := fchecker.StartStruct {
		AckLocalIPAckLocalPort: "Temp",
		EpochNonce: 1,
		HBeatLocalIPHBeatLocalPort: "Temp",
		HBeatRemoteIPHBeatRemotePort: "Temp",
		LostMsgThresh: c.lostMsgsThresh,
	}
	fcheckChan, err := fchecker.Start(startStruct)
	if err != nil {
		return err // Log failure?
	}
	
	go c.OnServerFailure(serverInfo, fcheckChan)

	return nil
}

func (c *Coord) OnServerFailure(failedServer ServerInfo, failReport <-chan fchecker.FailureDetected) {
	// TODO: Create new chain with server removed
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
	res.ServerId = c.Chain[0].ServerId
	trace.RecordAction(res)

	*headAddr = c.Chain[0].ServerAddr
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
	res.ServerId = c.Chain[c.AvailableServers-1].ServerId
	trace.RecordAction(res)

	*tailAddr = c.Chain[c.AvailableServers-1].ServerAddr
	return nil
}