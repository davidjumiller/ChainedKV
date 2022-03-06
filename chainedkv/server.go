package chainedkv

import (
	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"
	"errors"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"net"
	"net/rpc"
	"strings"
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
	ServerServerAddr  string
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
	tailServerListenAddr string // this server -> (tail) server
	sToken               tracing.TracingToken
}

type NextServerJoiningArgs struct {
	nextServerId         uint8
	nextServerListenAddr string // (callee) server -> this server
	sToken               tracing.TracingToken
}

type NextServerJoiningRes struct {
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

type Server struct {
	mutex                 sync.Mutex // restrict access to server state
	strace                *tracing.Tracer
	isHead                bool
	isTail                bool
	predecessorListenAddr string // only valid when !isHead?
	successorListenAddr   string // only valid when !isTail?
}

func NewServer() *Server {
	return &Server{
		isHead: false,
		isTail: true,
	}
}

func (s *Server) Start(serverId uint8, coordAddr string, serverAddr string, serverServerAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) error {
	// Initialize trace
	s.strace = strace
	trace := strace.CreateTrace()
	trace.RecordAction(ServerStart{serverId})

	// Connect to coord
	cConn, cClient, err := establishRPCConnection(serverAddr, coordAddr)
	defer cConn.Close()
	defer cClient.Close()

	/* Join */
	// Send join request to coord; receive current tail
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
	sToken := serverJoiningRes.sToken
	s.predecessorListenAddr = serverJoiningRes.tailServerListenAddr
	s.isHead = s.predecessorListenAddr != ""

	// If I am not head, tell predecessor (previous tail) that I am new tail
	if s.isHead {
		predConn, predClient, err := establishRPCConnection(serverServerAddr, s.predecessorListenAddr)
		if err != nil {
			return err
		}
		defer predConn.Close()
		defer predClient.Close()

		nextServerJoiningArgs := &NextServerJoiningArgs{
			nextServerId:         serverId,
			nextServerListenAddr: serverListenAddr,
			sToken:               sToken,
		}
		var nextServerJoiningRes NextServerJoiningRes
		err = predClient.Call("Server.AddSuccessor", nextServerJoiningArgs, &nextServerJoiningRes)
		if err != nil {
			return err
		}
		sToken = nextServerJoiningRes.sToken
	}

	// Start listening to heartbeats on random port
	serverIP := strings.Split(serverAddr, ":")[0]
	serverAckAddr := fchecker.StartListener(fmt.Sprint(serverIP, ":"))

	// Start listening for RPCs
	err = rpc.Register(s)
	if err != nil {
		return err
	}
	_, err = startRPCListener(serverListenAddr) // server -> server RPC
	if err != nil {
		return err
	}
	coordListenAddr, err := startRPCListener(fmt.Sprint(serverIP, ":")) // coord -> server RPC
	if err != nil {
		return err
	}

	// Notify coord that I have successfully joined
	trace = strace.ReceiveToken(sToken)
	trace.RecordAction(ServerJoined{serverId})
	serverJoinedArgs := &ServerJoinedArgs{
		serverId:         serverId,
		ackAddr:          serverAckAddr.String(),
		coordListenAddr:  coordListenAddr.String(),
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

func establishRPCConnection(laddr, raddr string) (*net.TCPConn, *rpc.Client, error) {
	// Code adapted from Piazza post @471_f1
	resolvedLaddr, err := net.ResolveTCPAddr("tcp", laddr)
	if err != nil {
		return nil, nil, err
	}
	resolvedRaddr, err := net.ResolveTCPAddr("tcp", raddr)
	if err != nil {
		return nil, nil, err
	}
	conn, err := net.DialTCP("tcp", resolvedLaddr, resolvedRaddr)
	if err != nil {
		return nil, nil, err
	}
	client := rpc.NewClient(conn)
	return conn, client, nil
}

func startRPCListener(rpcListenAddr string) (net.Addr, error) {
	resolvedRPCListenAddr, err := net.ResolveTCPAddr("tcp", rpcListenAddr)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", resolvedRPCListenAddr)
	if err != nil {
		return nil, err
	}
	go rpc.Accept(listener)
	return listener.Addr(), nil
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

func (s *Server) Get() {
	// If tail, respond with value stored at key
	// Result is empty string for key with no associated value?
}

func (s *Server) Put() {
	// Update stored v at k
	// If tail, send response to client
	//  else propagate Put to next server
}
