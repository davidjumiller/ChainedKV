package chainedkv

import (
	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"
	"net"
	"net/rpc"
	"fmt"
	"math/rand"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
	"net"
	"net/rpc"
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
	Tracer *tracing.Tracer
	Trace *tracing.Trace
	ClientAPIListenAddr string
	ServerAPIListenAddr string
	LostMsgsThresh uint8
	NumServers uint8
	AvailableServers uint8
	AllJoined bool
	Chain []ServerInfo
}

type ServerInfo struct {
	ServerAddr string
	ServerId   uint8
}

func NewCoord() *Coord {
	return &Coord{
		Chain: []ServerInfo{},
	}
}

type RemoteCoord struct {
	Coord *Coord
}

func NewRemoteCoord() *RemoteCoord {
	return &RemoteCoord{
		Coord: nil,
	}
}

func (c *Coord) Start(clientAPIListenAddr string, serverAPIListenAddr string, lostMsgsThresh uint8, numServers uint8, ctrace *tracing.Tracer) error {
	// Tracing: CoordStart
	c.Tracer = ctrace
	c.Trace = ctrace.CreateTrace()
	trace := c.Trace
	cStart := CoordStart{}
	trace.RecordAction(cStart)

	// Set initial Coord values
	c.AvailableServers = 0
	c.NumServers = numServers
	c.AllJoined = false
	c.LostMsgsThresh = lostMsgsThresh
	c.ClientAPIListenAddr = clientAPIListenAddr
	c.ServerAPIListenAddr = serverAPIListenAddr

	// Start accepting RPCs from clients and servers
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

	for {
		// stay alive?
		looping := 0
		looping++
	}
	return nil
}

func (remoteCoord *RemoteCoord) OnServerJoining(serverJoiningArgs *ServerJoiningArgs, serverJoiningRes *ServerJoiningRes) error {
	c := remoteCoord.Coord

  // Tracing: ServerJoiningRecvd
	trace := c.Tracer.ReceiveToken(serverJoiningArgs.SToken)
	trace.RecordAction(ServerJoiningRecvd{
		ServerId: serverJoiningArgs.ServerId,
	})

	// Simple blocking until its the servers turn to join (Poor Efficiency?)
	tail := c.Chain[c.AvailableServers-1]
	for tail.ServerId+1 != serverJoiningArgs.ServerId {} 

	*serverJoiningRes = ServerJoiningRes{
		TailServerListenAddr: c.Chain[c.AvailableServers-1].ServerAddr, // TailAddr can be nil on empty chain, server should recognize this
		SToken:               trace.GenerateToken(),
	}
	fmt.Println("Joining for", serverJoiningArgs.ServerId)
	return nil
}

func (remoteCoord *RemoteCoord) OnJoined(serverJoinedArgs *ServerJoinedArgs, serverJoinedRes *ServerJoiningRes) error {
	c := remoteCoord.Coord

  // Tracing: ServerJoinedRecvd
	trace := c.Tracer.ReceiveToken(serverJoinedArgs.SToken)
	trace.RecordAction(ServerJoinedRecvd{
		ServerId: serverJoinedArgs.ServerId,
	})


	// Adding server to chain
	serverInfo := ServerInfo{
		ServerAddr: serverJoinedArgs.ServerListenAddr,
		ServerId:   serverJoinedArgs.ServerId,
	}
  c.Chain = append(c.Chain, serverInfo)
	c.AvailableServers++
  fmt.Println("We have", c.AvailableServers, "available servers")
  
	// Tracing: NewChain
	traceChain := NewChain{Chain: []uint8{}}
	for i := range c.Chain {
		traceChain.Chain = append(traceChain.Chain, c.Chain[i].ServerId)
	}
	trace = c.Trace
	trace.RecordAction(traceChain)

	// Check if all servers have joined
	if c.AvailableServers == c.NumServers && c.AllJoined == false {
		trace.RecordAction(AllServersJoined{})
		c.AllJoined = true
	}

	// Starts fcheck on new server
	r := rand.New(rand.NewSource(10))
	startStruct := fchecker.StartStruct {
		AckLocalIPAckLocalPort: serverJoinedArgs.AckAddr,
		EpochNonce: r.Uint64(),
		HBeatLocalIPHBeatLocalPort: serverJoinedArgs.CoordListenAddr,
		HBeatRemoteIPHBeatRemotePort: serverJoinedArgs.ServerListenAddr,
		LostMsgThresh: c.LostMsgsThresh,
	}
	fcheckChan, err := fchecker.Start(startStruct)
	if err != nil {
		return err // Log failure?
	}
	
	go c.OnServerFailure(serverJoinedArgs.SToken, serverInfo, fcheckChan)

	return nil
}

func (c *Coord) OnServerFailure(token tracing.TracingToken, failedServer ServerInfo, failReport <-chan fchecker.FailureDetected) {
	// Tracing: ServerFail
	trace := c.Tracer.ReceiveToken(token)
	trace.RecordAction(ServerFail{
		ServerId: failedServer.ServerId,
	})

	// Finds the failed server in the chain by id
	i := func() int {
		for i := range c.Chain {
			if c.Chain[i].ServerId == 4 {
				return i
			}
		}
		return -1
	}()

	// Notifies prev and next nodes of failure
	coordIP := getIPFromAddr(c.ServerAPIListenAddr)
	coordServerAddr := fmt.Sprint(coordIP, ":")
	prevNode := c.Chain[i-1]
	nextNode := c.Chain[i+1]

	prevConn, prevClient, err := establishRPCConnection(coordServerAddr, prevNode.ServerAddr)
	if err != nil {
		return
	}
	args := ReplaceServerArgs{
		FailedServerId: failedServer.ServerId,
		ReplacementServerId: nextNode.ServerId,
		ReplacementServerListenAddr: nextNode.ServerAddr,
		CToken: token,
	}
	res := ReplaceServerRes{}
	err = prevClient.Call("Server.ReplaceSuccessor", args, res) // Maybe run this async?
	if err != nil {
		return
	}
	prevClient.Close()
	prevConn.Close()

	trace.RecordAction(ServerFailHandledRecvd{
		FailedServerId: failedServer.ServerId,
		AdjacentServerId: nextNode.ServerId,
	})

	nextConn, nextClient, err := establishRPCConnection(coordServerAddr, nextNode.ServerAddr)
	if err != nil {
		return
	}
	args = ReplaceServerArgs{
		FailedServerId: failedServer.ServerId,
		ReplacementServerId: prevNode.ServerId,
		ReplacementServerListenAddr: prevNode.ServerAddr,
		CToken: token,
	}
	res = ReplaceServerRes{}
	err = prevClient.Call("Server.ReplacePredecessor", args, res)
	if err != nil {
		return
	}
	nextClient.Close()
	nextConn.Close()

	trace.RecordAction(ServerFailHandledRecvd{
		FailedServerId: failedServer.ServerId,
		AdjacentServerId: prevNode.ServerId,
	})

	// Removes the failed server from the chain
	newChain := c.Chain[:i]
	i++
	for ; i < len(c.Chain); i++ {
		newChain = append(newChain, c.Chain[i])
	}
	c.Chain = newChain
	c.AvailableServers--

	traceChain := []uint8{}
	for i := range c.Chain {
		traceChain = append(traceChain, c.Chain[i].ServerId)
	}
	trace.RecordAction(NewChain{
		Chain: traceChain,
	})
}

func (remoteCoord *RemoteCoord) GetHead(args *ClientArgs, headRes *ClientRes) error {
	c := remoteCoord.Coord
  
  // Tracing: HeadReqRecvd
	trace := c.Tracer.ReceiveToken(args.KToken)
	req := HeadReqRecvd{
		ClientId: args.ClientId,
	}
	trace.RecordAction(req)

	// Simple blocking until all servers join (Poor Efficiency?)
	for c.AllJoined == false {
	}

  // Tracing: TailRes
	res := HeadRes{}
	res.ClientId = args.ClientId
	res.ServerId = c.Chain[0].ServerId
	trace.RecordAction(res)

  // Response to client
	*headRes = ClientRes{
		ServerId:   c.Chain[0].ServerId,
		ServerAddr: c.Chain[0].ServerAddr,
		KToken:     args.KToken,
	}
	fmt.Println("Ending headreq")
	return nil
}

func (remoteCoord *RemoteCoord) GetTail(args *ClientArgs, tailRes *ClientRes) error {
	c := remoteCoord.Coord
  // Tracing: TailReqRecvd
	trace := c.Tracer.ReceiveToken(args.KToken)
	req := TailReqRecvd{
		ClientId: args.ClientId,
	}
	trace.RecordAction(req)

	// Simple blocking until all servers join (Poor Efficiency?)
	for c.AllJoined == false {
	}

  // Tracing: TailRes
	res := TailRes{}
	res.ClientId = args.ClientId
	res.ServerId = c.Chain[c.AvailableServers-1].ServerId
	trace.RecordAction(res)

  // Response to client
	*tailRes = ClientRes{
		ServerId:   c.Chain[c.AvailableServers-1].ServerId,
		ServerAddr: c.Chain[c.AvailableServers-1].ServerAddr,
		KToken:     args.KToken,
	}
	return nil
}