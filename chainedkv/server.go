package chainedkv

import (
	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"
	"errors"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"math"
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

type ReplaceServerArgs struct {
	FailedServerId              uint8
	ReplacementServerId         uint8
	ReplacementServerListenAddr string
	CToken                      tracing.TracingToken
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

type ReplaceServerRes struct {
	ServerId       uint8
	FailedServerId uint8
	CToken         tracing.TracingToken
}

type PutArgs struct {
	ClientId     string
	OpId         uint32
	GId          uint64
	Key          string
	Value        string
	ClientIPPort string
	PToken       tracing.TracingToken
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

type PutFwdArgs struct {
	ClientId     string
	OpId         uint32
	GId          uint64
	Key          string
	Value        string
	ClientIPPort string
	PToken       tracing.TracingToken
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

type PutRes struct {
	OpId   uint32
	GId    uint64
	Key    string
	Value  string
	PToken tracing.TracingToken
}

type GetArgs struct {
	ClientId     string
	OpId         uint32
	Key          string
	ClientIPPort string
	GToken       tracing.TracingToken
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

type GetRes struct {
	OpId   uint32
	GId    uint64
	Key    string
	Value  string
	GToken tracing.TracingToken
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
	ServerId uint8
	SToken   tracing.TracingToken
}

type ServerJoiningRes struct {
	TailServerListenAddr string // this server -> (tail) server
	SToken               tracing.TracingToken
}

type NextServerJoiningArgs struct {
	NextServerId         uint8
	NextServerListenAddr string // (callee) server -> this server
	SToken               tracing.TracingToken
}

type NextServerJoiningRes struct {
	Store  map[string]string
	SToken tracing.TracingToken
}

type ServerJoinedArgs struct {
	ServerId         uint8
	AckAddr          string // this server receives heartbeats from coord at this addr
	CoordListenAddr  string // coord -> this server (this server receives RPC from coord at this addr)
	ServerListenAddr string // (caller) server -> this server (this server receives RPC from servers at this addr)
	SToken           tracing.TracingToken
}

type Server struct {
	mutex                 sync.Mutex
	serverId              uint8
	tracer                *tracing.Tracer
	isHead                bool
	isTail                bool
	lastGId               uint64
	serverServerAddr      string
	serverClientIP        string // used to generate random ports for server -> client RPC
	predecessorListenAddr string // only valid when !isHead?
	successorListenAddr   string // only valid when !isTail?
	succConn              *net.TCPConn
	succRPCClient         *rpc.Client // maintains RPC client for successor to use for PutFwd
	store                 map[string]string
	lastClientPuts        map[string]uint32
}

func NewServer() *Server {
	return &Server{
		isHead:         false,
		isTail:         true,
		store:          make(map[string]string),
		lastClientPuts: make(map[string]uint32),
	}
}

type RemoteServer struct {
	Server *Server
}

func NewRemoteServer() *RemoteServer {
	return &RemoteServer{
		Server: nil,
	}
}

func (s *Server) Start(serverId uint8, coordAddr string, serverAddr string, serverServerAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) error {
	s.serverId = serverId
	s.tracer = strace
	trace := strace.CreateTrace()
	trace.RecordAction(ServerStart{serverId})

	// Connect to coord
	_, cClient, err := establishRPCConnection(serverAddr, coordAddr)
	if err != nil || cClient == nil {
		fmt.Println(err)
		fmt.Println("failed to establish connection to coord")
		return err
	}
	defer cClient.Close()

	/* Join */
	// Send join request to coord; receive current tail
	trace.RecordAction(ServerJoining{serverId})
	serverJoiningArgs := &ServerJoiningArgs{
		ServerId: serverId,
		SToken:   trace.GenerateToken(),
	}
	var serverJoiningRes ServerJoiningRes
	err = cClient.Call("Coord.OnServerJoining", serverJoiningArgs, &serverJoiningRes)
	if err != nil {
		fmt.Println("failed to call Coord.OnServerJoining")
		return err
	}
	sToken := serverJoiningRes.SToken
	s.predecessorListenAddr = serverJoiningRes.TailServerListenAddr
	s.isHead = s.predecessorListenAddr == ""

	// If s is not head, tell predecessor (previous tail) that I am new tail
	if !s.isHead {
		predConn, predClient, err := establishRPCConnection(serverServerAddr, s.predecessorListenAddr)
		if err != nil {
			fmt.Println(err)
			fmt.Println("serverserveraddr", serverServerAddr)
			fmt.Println("failed to establish connection to predecessor at", s.predecessorListenAddr)
			return err
		}

		nextServerJoiningArgs := &NextServerJoiningArgs{
			NextServerId:         serverId,
			NextServerListenAddr: serverListenAddr,
			SToken:               sToken,
		}
		var nextServerJoiningRes NextServerJoiningRes
		err = predClient.Call("Server.AddSuccessor", nextServerJoiningArgs, &nextServerJoiningRes)
		if err != nil {
			fmt.Println(err)
			fmt.Println("failed to call predecessor's AddSuccessor")
			return err
		}
		predClient.Close()
		predConn.Close()
		sToken = nextServerJoiningRes.SToken
		s.store = nextServerJoiningRes.Store
	}
	trace = strace.ReceiveToken(sToken)

	// Start listening to heartbeats on random port
	serverIP := getIPFromAddr(serverAddr)
	serverAckNetAddr := fchecker.StartListener(fmt.Sprint(serverIP, ":"))
	_, serverAckPort := splitAddrParts(serverAckNetAddr.String())
	serverAckAddr := fmt.Sprintf("%s:%s", serverIP, serverAckPort)

	// Start listening for RPCs
	remote := NewRemoteServer()
	remote.Server = s
	s.serverClientIP = getIPFromAddr(clientListenAddr)
	err = rpc.RegisterName("Server", remote)
	if err != nil {
		fmt.Println("failed to register this server for RPC")
		return err
	}
	_, err = startRPCListener(serverListenAddr) // server -> server RPC
	if err != nil {
		fmt.Println("failed to start listening for RPCs from adjacent servers at", serverListenAddr)
		return err
	}
	coordListenNetAddr, err := startRPCListener(fmt.Sprint(serverIP, ":")) // coord -> server RPC
	if err != nil {
		fmt.Println("failed to start listening for RPCs from coord at", coordListenNetAddr)
		return err
	}
	_, coordListenPort := splitAddrParts(coordListenNetAddr.String())
	coordListenAddr := fmt.Sprintf("%s:%s", serverIP, coordListenPort)

	// Notify coord that s has successfully joined
	trace.RecordAction(ServerJoined{serverId})
	serverJoinedArgs := &ServerJoinedArgs{
		ServerId:         serverId,
		AckAddr:          serverAckAddr,
		CoordListenAddr:  coordListenAddr,
		ServerListenAddr: serverListenAddr,
		SToken:           trace.GenerateToken(),
	}
	cClient.Go("Coord.OnServerJoined", serverJoinedArgs, nil, nil)

	for {
		// run server indefinitely
	}
	return nil
}

func getIPFromAddr(addr string) string {
	ip, _ := splitAddrParts(addr)
	return ip
}

func splitAddrParts(addr string) (string, string) {
	ends := strings.Split(addr, ":")
	if len(ends) == 0 {
		return "", ""
	}
	return ends[0], ends[len(ends)-1]
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

func (remoteServer *RemoteServer) AddSuccessor(args *NextServerJoiningArgs, reply *NextServerJoiningRes) error {
	s := remoteServer.Server
	s.mutex.Lock()
	defer s.mutex.Unlock()

	trace := s.tracer.ReceiveToken(args.SToken)
	trace.RecordAction(NextServerJoining{args.NextServerId})

	err := s.updateSuccessorInfo(args.NextServerListenAddr)
	if err != nil {
		return err
	}

	trace.RecordAction(NewJoinedSuccessor{args.NextServerId})
	*reply = NextServerJoiningRes{
		Store:  s.store,
		SToken: trace.GenerateToken(),
	}
	return nil
}

func (remoteServer *RemoteServer) ReplacePredecessor(args *ReplaceServerArgs, reply *ReplaceServerRes) error {
	s := remoteServer.Server
	s.mutex.Lock()
	defer s.mutex.Unlock()

	trace := s.tracer.ReceiveToken(args.CToken)
	trace.RecordAction(ServerFailRecvd{args.FailedServerId})

	s.predecessorListenAddr = args.ReplacementServerListenAddr
	s.isHead = s.predecessorListenAddr == ""
	if !s.isHead {
		// Only record NewFailoverPredecessor if a new predecessor exists
		trace.RecordAction(NewFailoverPredecessor{args.ReplacementServerId})
	}

	trace.RecordAction(ServerFailHandled{args.FailedServerId})
	*reply = ReplaceServerRes{
		ServerId:       s.serverId,
		FailedServerId: args.FailedServerId,
		CToken:         trace.GenerateToken(),
	}
	return nil
}

func (remoteServer *RemoteServer) ReplaceSuccessor(args *ReplaceServerArgs, reply *ReplaceServerRes) error {
	s := remoteServer.Server
	s.mutex.Lock()
	defer s.mutex.Unlock()

	trace := s.tracer.ReceiveToken(args.CToken)
	trace.RecordAction(ServerFailRecvd{args.FailedServerId})

	err := s.updateSuccessorInfo(args.ReplacementServerListenAddr)
	if err != nil {
		return err
	}
	if s.isTail {
		// Increment lastGId to account for Gets potentially lost at old tail
		s.lastGId += uint64(math.Pow(2, 24))
	}
	if !s.isTail {
		// Only record NewFailoverSuccessor if a new successor exists
		trace.RecordAction(NewFailoverSuccessor{args.ReplacementServerId})
	}

	trace.RecordAction(ServerFailHandled{args.FailedServerId})
	*reply = ReplaceServerRes{
		ServerId:       s.serverId,
		FailedServerId: args.FailedServerId,
		CToken:         trace.GenerateToken(),
	}
	return nil
}

// Ensure this is called while Server.mutex is locked
func (s *Server) updateSuccessorInfo(successorListenAddr string) error {
	// Not sure if this is the best way to close connections
	if s.succRPCClient != nil {
		s.succRPCClient.Close()
		s.succRPCClient = nil
	}
	if s.succConn != nil {
		s.succConn.Close()
		s.succConn = nil
	}

	s.successorListenAddr = successorListenAddr
	s.isTail = s.successorListenAddr == ""
	if !s.isTail {
		succConn, succClient, err := establishRPCConnection(s.serverServerAddr, s.successorListenAddr)
		if err != nil {
			return err
		}
		s.succConn = succConn
		s.succRPCClient = succClient
	}
	return nil
}

func (remoteServer *RemoteServer) Get(args *GetArgs, _ *interface{}) error {
	s := remoteServer.Server
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if !s.isTail {
		return errors.New("Non-tail servers cannot process Get requests")
	}

	trace := s.tracer.ReceiveToken(args.GToken)
	trace.RecordAction(GetRecvd{args.ClientId, args.OpId, args.Key})

	s.lastGId += 1
	gId := s.lastGId
	trace.RecordAction(GetOrdered{args.ClientId, args.OpId, gId, args.Key})

	callerConn, callerClient, err := establishRPCConnection(fmt.Sprintf(s.serverClientIP, ":"), args.ClientIPPort)
	if err != nil {
		return err
	}
	defer callerConn.Close()
	defer callerClient.Close()

	value := s.store[args.Key] // value == "" if key has no associated value
	trace.RecordAction(GetResult{args.ClientId, args.OpId, gId, args.Key, value})
	getRes := GetRes{
		OpId:   args.OpId,
		GId:    gId,
		Key:    args.Key,
		Value:  value,
		GToken: trace.GenerateToken(),
	}
	callerClient.Go("KVS.GetResult", getRes, nil, nil)
	return nil
}

func (remoteServer *RemoteServer) Put(args *PutArgs, gId *uint64) error {
	s := remoteServer.Server
	s.mutex.Lock()
	defer s.mutex.Unlock()

	trace := s.tracer.ReceiveToken(args.PToken)
	trace.RecordAction(PutRecvd{args.ClientId, args.OpId, args.Key, args.Value})

	if args.GId == 0 {
		// Put is a regular (non-resend) Put

		// 2^64 addresses ÷ (2^32 ops per client × 2^8 clients) = 2^24 addresses per op
		// - head.Put() increments gId by 2^24
		// - tail.Get() increments gId by 1
		// - tail.PutFwd() sets gId to put.gId
		// This avoids gId conflicts at the tail unless
		//  the tail processes > 2^24 consecutive Gets followed by a Put
		if s.isTail {
			s.lastGId += 1
		} else {
			s.lastGId += uint64(math.Pow(2, 24))
		}
	} else {
		// Put is a resend
		if args.GId > s.lastGId {
			// Server has not seen this Put
			s.lastGId = args.GId
		}
	}
	*gId = s.lastGId
	trace.RecordAction(PutOrdered{args.ClientId, args.OpId, *gId, args.Key, args.Value})

	// Update value at key only if server has not seen this Put from this client
	if args.OpId > s.lastClientPuts[args.ClientId] {
		s.store[args.Key] = args.Value
		s.lastClientPuts[args.ClientId] = args.OpId
	}
	return s.fwdOrReturnPut(trace, args.ClientIPPort, args.ClientId, args.OpId, *gId, args.Key, args.Value)
}

func (remoteServer *RemoteServer) PutFwd(args *PutFwdArgs, _ *interface{}) error {
	s := remoteServer.Server
	s.mutex.Lock()
	defer s.mutex.Unlock()

	trace := s.tracer.ReceiveToken(args.PToken)
	trace.RecordAction(PutFwdRecvd{args.ClientId, args.OpId, args.GId, args.Key, args.Value})

	if args.GId > s.lastGId {
		s.lastGId = args.GId
	}
	// Update value at key only if server has not seen this Put from this client
	if args.OpId > s.lastClientPuts[args.ClientId] {
		s.store[args.Key] = args.Value
		s.lastClientPuts[args.ClientId] = args.OpId
	}
	return s.fwdOrReturnPut(trace, args.ClientIPPort, args.ClientId, args.OpId, args.GId, args.Key, args.Value)
}

func (s *Server) fwdOrReturnPut(trace *tracing.Trace, clientIPPort string, clientId string, opId uint32, gId uint64, key, value string) error {
	if s.isTail {
		// Send Put response to client
		callerConn, callerClient, err := establishRPCConnection(fmt.Sprintf(s.serverClientIP, ":"), clientIPPort)
		if err != nil {
			return err
		}
		defer callerConn.Close()
		defer callerClient.Close()

		trace.RecordAction(PutResult{clientId, opId, gId, key, value})
		putRes := PutRes{
			OpId:   opId,
			GId:    gId,
			Key:    key,
			Value:  value,
			PToken: trace.GenerateToken(),
		}
		callerClient.Go("KVS.PutResult", putRes, nil, nil)
	} else {
		// Issue PutFwd to next server in chain
		trace.RecordAction(PutFwd{clientId, opId, gId, key, value})
		putFwdArgs := PutFwdArgs{
			ClientId:     clientId,
			OpId:         opId,
			GId:          gId,
			Key:          key,
			Value:        value,
			ClientIPPort: clientIPPort,
			PToken:       trace.GenerateToken(),
		}
		s.succRPCClient.Go("Server.PutFwd", putFwdArgs, nil, nil)
	}
	return nil
}
