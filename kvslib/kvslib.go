package kvslib

import (
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
	"net"
	"net/rpc"
	"time"
)

// Actions to be recorded by kvslib (as part of ktrace, put trace, get trace):

type KvslibStart struct {
	ClientId string
}

type KvslibStop struct {
	ClientId string
}

type Put struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutResultRecvd struct {
	OpId uint32
	GId  uint64
	Key  string
}

type PutArgs struct {
	ClientId     string
	OpId         uint32
	GId          uint64
	Key          string
	Value        string
	GToken       tracing.TracingToken
	ClientIPPort string
}

type PutRes struct {
	OpId   uint32
	GId    uint64
	Key    string
	Value  string
	PToken tracing.TracingToken
}

type Get struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetResultRecvd struct {
	OpId  uint32
	GId   uint64
	Key   string
	Value string
}

type GetArgs struct {
	ClientId     string
	OpId         uint32
	Key          string
	GToken       tracing.TracingToken
	ClientIPPort string
}

type GetRes struct {
	OpId   uint32
	GId    uint64
	Key    string
	Value  string // Note: this should be "" if a Put for this key does not exist
	GToken tracing.TracingToken
}

type ClientArgs struct {
	ClientId   string
	ClientAddr string // Might not need this field
	KToken     tracing.TracingToken
}

type ClientRes struct {
	ServerId   uint8
	ServerAddr string
	KToken     tracing.TracingToken
}

type HeadReq struct {
	ClientId string
}

type HeadResRecvd struct {
	ClientId string
	ServerId uint8
}

type TailReq struct {
	ClientId string
}

type TailResRecvd struct {
	ClientId string
	ServerId uint8
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId   uint32
	GId    uint64
	Result string
}

type KVS struct {
	NotifyCh NotifyChannel
	// Add more KVS instance state here.
	KTrace          *tracing.Trace
	ClientId        string
	LocalCoordAddr  string
	RemoteCoordAddr string
	LocalHeadAddr   string
	RemoteHeadAddr  string
	LocalTailAddr   string
	RemoteTailAddr  string
	HeadListener    *net.TCPListener
	TailListener    *net.TCPListener
	OpId            uint32
	RpcConduit      RpcConduit
}

type RpcConduit struct {
	NotifyCh   NotifyChannel // Nested so that KVS still has NotifyCh property (in case it matters for grading)
	RTT        time.Duration
	Tracer     *tracing.Tracer
	InProgress map[uint32]time.Time // Map representing sent requests that haven't been responded to
}

func NewKVS() *KVS {
	return &KVS{
		NotifyCh: nil,
	}
}

// Start Starts the instance of KVS to use for connecting to the system with the given coord's IP:port.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the coord, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, coordIPPort string, localCoordIPPort string, localHeadServerIPPort string, localTailServerIPPort string, chCapacity int) (NotifyChannel, error) {
	d.KTrace = localTracer.CreateTrace()
	d.RpcConduit = RpcConduit{
		RTT:        3 * time.Second,
		Tracer:     localTracer,
		NotifyCh:   make(NotifyChannel, chCapacity),
		InProgress: make(map[uint32]time.Time),
	}
	d.NotifyCh = d.RpcConduit.NotifyCh
	d.LocalCoordAddr = localCoordIPPort
	d.RemoteCoordAddr = coordIPPort
	d.LocalHeadAddr = localHeadServerIPPort
	d.LocalTailAddr = localTailServerIPPort
	d.OpId = 0

	// Tracing
	d.KTrace.RecordAction(KvslibStart{clientId})

	// Setup RPC
	err := rpc.RegisterName("KVS", &d.RpcConduit)
	if err != nil {
		return nil, err
	}
	d.HeadListener, err = startRPCListener(localHeadServerIPPort)
	util.CheckErr(err, "Could not start head listener in Start")
	d.TailListener, err = startRPCListener(localTailServerIPPort)
	util.CheckErr(err, "Could not start tail listener in Start")

	// Connect to coord
	err = contactCoord(d)
	util.CheckErr(err, "Could not connect to coord node")

	return d.NotifyCh, nil
}

// Get  non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value OpId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	// Should return OpId or error
	localOpId := d.OpId
	d.OpId++

	trace := tracer.CreateTrace()
	trace.RecordAction(Get{clientId, localOpId, key})

	// Send get to tail via RPC
	getArgs := GetArgs{
		ClientId:     clientId,
		OpId:         localOpId,
		Key:          key,
		GToken:       trace.GenerateToken(),
		ClientIPPort: d.LocalTailAddr, // Receives result from tail
	}
	conn, client := makeClient(d.LocalTailAddr, d.RemoteTailAddr)
	d.RpcConduit.InProgress[localOpId] = time.Now()
	client.Go("Server.Get", getArgs, nil, nil)

	// Result should be received from tail via d.GetResult()
	go handleGetTimeout(d, localOpId, getArgs, conn, client)
	return localOpId, nil
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value OpId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	// Should return OpId or error
	localOpId := d.OpId
	d.OpId++

	trace := tracer.CreateTrace()
	trace.RecordAction(Put{clientId, localOpId, key, value})

	// Send put to head via RPC
	putArgs := PutArgs{
		ClientId:     clientId,
		OpId:         localOpId,
		Key:          key,
		GId:          0,
		Value:        value,
		GToken:       trace.GenerateToken(),
		ClientIPPort: d.LocalTailAddr, // Receives result from tail
	}
	conn, client := makeClient(d.LocalHeadAddr, d.RemoteHeadAddr)
	d.RpcConduit.InProgress[localOpId] = time.Now()
	var gid uint64
	client.Call("Server.Put", putArgs, &gid)

	// Result should be received from tail via d.PutResult()
	putArgs.GId = gid // Update gid in case of re-sends
	go handlePutTimeout(d, localOpId, putArgs, conn, client)
	return localOpId, nil
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	// pass tracer
	d.KTrace.RecordAction(KvslibStop{d.ClientId})
	err := d.RpcConduit.Tracer.Close()
	util.CheckErr(err, "Could not close KVS tracer")
	err = d.HeadListener.Close()
	util.CheckErr(err, "Could not close KVS head listener")
	err = d.TailListener.Close()
	util.CheckErr(err, "Could not close KVS tail listener")
	close(d.NotifyCh)
	return
}

// Creates and returns a TCP connection between localAddr and remoteAddr
func makeConnection(localAddr string, remoteAddr string) *net.TCPConn {
	localTcpAddr, err := net.ResolveTCPAddr("tcp", localAddr)
	util.CheckErr(err, "Could not resolve address: "+localAddr)
	remoteTcpAddr, err := net.ResolveTCPAddr("tcp", remoteAddr)
	util.CheckErr(err, "Could not resolve address: "+remoteAddr)
	conn, err := net.DialTCP("tcp", localTcpAddr, remoteTcpAddr)
	util.CheckErr(err, "Could not connect "+localAddr+" to "+remoteAddr)

	return conn
}

// Send head and tail requests to coordinator
func contactCoord(d *KVS) error {
	conn := makeConnection(d.LocalCoordAddr, d.RemoteCoordAddr)
	defer conn.Close()
	client := rpc.NewClient(conn)
	defer client.Close()
	token := d.KTrace.GenerateToken()

	// Request head server from coord
	headReqArgs := ClientArgs{
		ClientId:   d.ClientId,
		ClientAddr: d.LocalCoordAddr,
		KToken:     token,
	}
	var headRes ClientRes
	d.KTrace.RecordAction(HeadReq{d.ClientId})
	err := client.Call("Coord.GetHead", headReqArgs, &headRes)
	if err != nil {
		return err
	}
	d.RemoteHeadAddr = headRes.ServerAddr // Update global var
	d.KTrace.RecordAction(HeadResRecvd{
		ClientId: d.ClientId,
		ServerId: headRes.ServerId,
	})

	// Request tail server from coord
	tailReqArgs := ClientArgs{
		ClientId:   d.ClientId,
		ClientAddr: d.LocalCoordAddr,
		KToken:     token,
	}
	var tailRes ClientRes
	d.KTrace.RecordAction(TailReq{d.ClientId})
	err = client.Call("Coord.GetTail", tailReqArgs, &tailRes)
	if err != nil {
		return err
	}
	d.RemoteTailAddr = tailRes.ServerAddr // Update global var
	d.KTrace.RecordAction(TailResRecvd{
		ClientId: d.ClientId,
		ServerId: tailRes.ServerId,
	})
	return nil
}

// GetResult Confirms that a Get succeeded
// Does not reply to callee!
func (rpcConduit *RpcConduit) GetResult(args *GetRes, _ *interface{}) error {
	trace := rpcConduit.Tracer.ReceiveToken(args.GToken)
	trace.RecordAction(GetResultRecvd{
		OpId:  args.OpId,
		GId:   args.GId,
		Key:   args.Key,
		Value: args.Value,
	})
	result := ResultStruct{
		OpId:   args.OpId,
		GId:    args.GId,
		Result: args.Value,
	}
	updateRTT(rpcConduit, args.OpId)
	rpcConduit.NotifyCh <- result
	return nil
}

// PutResult Confirms that a Put succeeded
// Does not reply to callee!
func (rpcConduit *RpcConduit) PutResult(args *PutRes, _ *interface{}) error {
	trace := rpcConduit.Tracer.ReceiveToken(args.PToken)
	trace.RecordAction(PutResultRecvd{
		OpId: args.OpId,
		GId:  args.GId,
		Key:  args.Key,
	})
	result := ResultStruct{
		OpId:   args.OpId,
		GId:    args.GId,
		Result: args.Value,
	}
	updateRTT(rpcConduit, args.OpId)
	rpcConduit.NotifyCh <- result
	return nil
}

// Code from ubcars server implementation (chainedkv/server.go)
// Starts an RPC listener and returns the address it is on
func startRPCListener(rpcListenAddr string) (*net.TCPListener, error) {
	resolvedRPCListenAddr, err := net.ResolveTCPAddr("tcp", rpcListenAddr)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", resolvedRPCListenAddr)
	if err != nil {
		return nil, err
	}
	go rpc.Accept(listener)
	return listener, nil
}

// Creates an RPC client given between a local and remote address
func makeClient(localAddr string, remoteAddr string) (*net.TCPConn, *rpc.Client) {
	conn := makeConnection(localAddr, remoteAddr)
	client := rpc.NewClient(conn)
	return conn, client
}

// Checks if a Get request may have been interrupted due to server failure and responds accordingly
func handleGetTimeout(d *KVS, opId uint32, getArgs GetArgs, conn *net.TCPConn, client *rpc.Client) {
	for {
		select {
		case <-time.After(d.RpcConduit.RTT):
			_, exists := d.RpcConduit.InProgress[opId]
			if exists {
				// opId is still in progress
				err := contactCoord(d)
				util.CheckErr(err, "Could not contact coord")
				client.Go("Server.Get", getArgs, nil, nil)
				return
			} else {
				// opId is not in progress
				client.Close()
				conn.Close()
			}
		}
	}
}

// Checks if a Put request may have been interrupted due to server failure and responds accordingly
func handlePutTimeout(d *KVS, opId uint32, putArgs PutArgs, conn *net.TCPConn, client *rpc.Client) {
	for {
		select {
		case <-time.After(d.RpcConduit.RTT):
			_, exists := d.RpcConduit.InProgress[opId]
			if exists {
				// opId is still in progress
				err := contactCoord(d)
				util.CheckErr(err, "Could not contact coord")
				client.Go("Server.Put", putArgs, nil, nil)
				return
			} else {
				// opId is not in progress
				client.Close()
				conn.Close()
			}
		}
	}
}

// Updates a KVS's estmated RTT based on an operation's RTT
func updateRTT(rpcConduit *RpcConduit, opId uint32) {
	newRtt := time.Now().Sub(rpcConduit.InProgress[opId])
	rpcConduit.RTT = (rpcConduit.RTT + newRtt) / 2
	delete(rpcConduit.InProgress, opId)
}
