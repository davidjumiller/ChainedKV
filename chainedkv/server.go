package chainedkv

import (
	"errors"
	"github.com/DistributedClocks/tracing"
	"net"
	"net/rpc"
	"sync"
)

type ServerStart struct {
	ServerId uint8
}

type ServerJoining struct {
	ServerId uint8
}

type NextServerJoining struct {
	NextServerId uint8
}

type NewJoinedSuccessor struct {
	NextServerId uint8
}

type ServerJoined struct {
	ServerId uint8
}

type ServerFailRecvd struct {
	FailedServerId uint8
}

type NewFailoverSuccessor struct {
	NewNextServerId uint8
}

type NewFailoverPredecessor struct {
	NewPrevServerId uint8
}

type ServerFailHandled struct {
	FailedServerId uint8
}

type PutRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutOrdered struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutFwd struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutFwdRecvd struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type GetRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetOrdered struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
}

type GetResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type ServerConfig struct {
	ServerId          uint8
	CoordAddr         string
	ServerAddr        string
	ServerListenAddr  string
	ClientListenAddr  string
	TracingServerAddr string
	Secret            []byte
	TracingIdentity   string
}

type ServerJoiningArgs struct {
	serverId uint8
	sToken   tracing.TracingToken
}

type ServerJoiningRes struct {
	tailServerListenAddr string
	sToken               tracing.TracingToken
}

type NextServerJoiningArgs struct {
	nextServerId         uint8
	nextServerListenAddr string
	sToken               tracing.TracingToken
}

type NextServerJoiningRes struct {
	sToken tracing.TracingToken
}

type ServerJoinedArgs struct {
	serverId         uint8
	serverListenAddr string
	sToken           tracing.TracingToken
}

type ServerJoinedRes struct {
	sToken tracing.TracingToken
}

type Server struct {
	mutex  sync.Mutex // restrict access to server state
	strace *tracing.Tracer
	isHead bool
	isTail bool
	//predecessorId         uint8  // only valid when !isHead?
	predecessorListenAddr string // only valid when !isHead?
	//successorId           uint8  // only valid when !isTail?
	successorListenAddr string // only valid when !isTail?
}

func NewServer() *Server {
	return &Server{
		isHead: false,
		isTail: true,
	}
}

func (s *Server) Start(serverId uint8, coordAddr string, serverAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) error {
	// Spin up goroutine to ack heartbeats coming from coord?

	s.strace = strace
	trace := strace.CreateTrace()
	trace.RecordAction(ServerStart{serverId})

	/* Connect to coord */
	// Connection establishment code adapted from Piazza post @471_f1
	laddr, err := net.ResolveTCPAddr("tcp", serverAddr)
	if err != nil {
		return err
	}
	raddr, err := net.ResolveTCPAddr("tcp", coordAddr)
	if err != nil {
		return err
	}
	cConn, err := net.DialTCP("tcp", laddr, raddr)
	if err != nil {
		return err
	}
	defer cConn.Close()
	cClient := rpc.NewClient(cConn)
	defer cClient.Close()

	/* Join */
	// Send join request to coord; receive current tail and updated trace token
	trace.RecordAction(ServerJoining{serverId})
	serverJoiningArgs := &ServerJoiningArgs{
		serverId: serverId,
		sToken:   trace.GenerateToken(),
	}
	var serverJoiningRes ServerJoiningRes
	err = cClient.Call("Coord.OnServerJoining", serverJoiningArgs, &serverJoiningRes)
	if err != nil {
		return err
	}
	tailServerListenAddr := serverJoiningRes.tailServerListenAddr
	sToken := serverJoiningRes.sToken

	// If tail exists, tell tail that I am new tail; receive updated trace token
	if tailServerListenAddr != "" {
		tailClient, err := rpc.Dial("tcp", tailServerListenAddr)
		if err != nil {
			return err
		}
		defer tailClient.Close()
		nextServerJoiningArgs := &NextServerJoiningArgs{
			nextServerId:         serverId,
			nextServerListenAddr: serverListenAddr,
			sToken:               sToken,
		}
		var nextServerJoiningRes NextServerJoiningRes
		err = tailClient.Call("Server.AddSuccessor", nextServerJoiningArgs, &nextServerJoiningRes)
		if err != nil {
			return err
		}
		sToken = nextServerJoiningRes.sToken
	}

	// Update server state
	s.isHead = tailServerListenAddr == ""
	s.predecessorListenAddr = tailServerListenAddr

	// Start listening for RPCs
	err = s.handleRPC(serverListenAddr)
	if err != nil {
		return err
	}

	// Notify coord that I have successfully joined; receive updated trace token
	trace = strace.ReceiveToken(sToken)
	trace.RecordAction(ServerJoined{serverId})
	serverJoinedArgs := &ServerJoinedArgs{
		serverId:         serverId,
		serverListenAddr: serverListenAddr,
		sToken:           trace.GenerateToken(),
	}
	var serverJoinedRes ServerJoinedRes // TODO: maybe no response necessary?
	err = cClient.Call("Coord.OnServerJoined", serverJoinedArgs, &serverJoinedRes)
	if err != nil {
		return err
	}
	trace = strace.ReceiveToken(serverJoinedRes.sToken)

	return errors.New("not implemented")
}

func (s *Server) AddSuccessor(args *NextServerJoiningArgs, reply *NextServerJoiningRes) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	trace := s.strace.ReceiveToken(args.sToken)
	trace.RecordAction(NextServerJoining{args.nextServerId})

	s.isTail = false
	s.successorListenAddr = args.nextServerListenAddr

	trace.RecordAction(NewJoinedSuccessor{args.nextServerId})
	*reply = NextServerJoiningRes{trace.GenerateToken()}
	return nil
}

func (s *Server) handleRPC(serverListenAddr string) error {
	err := rpc.Register(s)
	if err != nil {
		return err
	}
	resolvedServerListenAddr, err := net.ResolveTCPAddr("tcp", serverListenAddr)
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", resolvedServerListenAddr)
	if err != nil {
		return err
	}
	go rpc.Accept(listener)
	return nil
}

func (s *Server) Get() {
	// If tail, respond with value stored at key
	// Result is empty string for key with no associated value?
}

func (s *Server) Put() {
	// Update stored v at k
	// If tail, send response to client
	//  else propagate Put to next server
}
