package kvslib

import (
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
	"net"
	"net/rpc"
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
	Key          string
	Value        string
	GToken       tracing.TracingToken
	ClientIPPort string
}

type PutResultArgs struct {
	OpId   uint32
	GId    uint64
	Key    string
	GToken tracing.TracingToken
	Value  string
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

type GetResultArgs struct {
	OpId   uint32
	GId    uint64
	Key    string
	Value  string // Note: this should be "" if a Put for this key does not exist
	GToken tracing.TracingToken
}

type ClientArgs struct {
	ClientId   string
	ClientAddr string
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
	Tracer           *tracing.Tracer
	KTrace           *tracing.Trace
	ClientId         string
	CoordLocalIPPort string
	HeadLocalIP      string
	HeadRemoteIP     string
	TailLocalIP      string
	TailRemoteIP     string
	CoordListener    *net.TCPListener
	HeadListener     *net.TCPListener
	TailListener     *net.TCPListener
	OpId             uint32
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
	d.CoordLocalIPPort = localCoordIPPort
	d.NotifyCh = make(NotifyChannel, chCapacity)
	d.OpId = 0
	d.Tracer = localTracer
	d.KTrace = d.Tracer.CreateTrace()
	d.KTrace.RecordAction(KvslibStart{clientId})

	// Setup RPC
	err := rpc.Register(d)
	if err != nil {
		return nil, err
	}
	d.CoordListener, err = startRPCListener(localCoordIPPort)
	util.CheckErr(err, "Could not start coord listener in Start")
	d.HeadListener, err = startRPCListener(localHeadServerIPPort)
	util.CheckErr(err, "Could not start head listener in Start")
	d.TailListener, err = startRPCListener(localTailServerIPPort)
	util.CheckErr(err, "Could not start tail listener in Start")

	// Connect to coord
	conn := makeConnection(localCoordIPPort, coordIPPort)
	client := rpc.NewClient(conn)
	// TODO function that services calls from coord OR we make request to coord every time we detect failure
	// this should receive updates on new head / tail, and can update d. struct accordingly
	// TODO head/tail failure detection and handling

	// Get head server from coord
	headReqArgs := ClientArgs{
		ClientId:   clientId,
		ClientAddr: localCoordIPPort,
		KToken:     nil,
	}
	var headRes ClientRes
	// TODO sync method name with coord.go
	err = client.Call("Coord.HeadRequest", headReqArgs, &headRes)
	if err != nil {
		return nil, err
	}

	// Get tail server from coord
	tailReqArgs := ClientArgs{
		ClientId:   clientId,
		ClientAddr: localCoordIPPort,
		KToken:     nil,
	}
	var tailRes ClientRes
	// TODO sync method name with coord.go
	err = client.Call("Coord.TailRequest", tailReqArgs, &tailRes)
	if err != nil {
		return nil, err
	}

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
	conn := makeConnection(d.TailLocalIP, d.TailRemoteIP)
	client := rpc.NewClient(conn)
	defer client.Close()
	getArgs := GetArgs{
		ClientId:     clientId,
		OpId:         localOpId,
		Key:          key,
		GToken:       trace.GenerateToken(),
		ClientIPPort: d.TailLocalIP,
	}
	// TODO sync method name with server.go
	client.Go("Server.Get", getArgs, nil, nil)
	// Result should be received from tail via d.GetResult()
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
	conn := makeConnection(d.HeadLocalIP, d.HeadRemoteIP)
	client := rpc.NewClient(conn)
	defer client.Close()
	putArgs := PutArgs{
		ClientId:     clientId,
		OpId:         localOpId,
		Key:          key,
		Value:        value,
		GToken:       trace.GenerateToken(),
		ClientIPPort: d.TailLocalIP,
	}
	// TODO sync method name with server.go
	client.Go("Server.Put", putArgs, nil, nil)

	// Result should be received from tail via d.PutResult()
	return localOpId, nil
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	// pass tracer
	d.KTrace.RecordAction(KvslibStop{d.ClientId})
	err := d.CoordListener.Close()
	util.CheckErr(err, "Could not close KVS coord listener")
	err = d.HeadListener.Close()
	util.CheckErr(err, "Could not close KVS head listener")
	err = d.TailListener.Close()
	util.CheckErr(err, "Could not close KVS tail listener")
	return
}

// Creates and returns a TCP connection between localAddr and remoteAddr
func makeConnection(localAddr string, remoteAddr string) *net.TCPConn {
	localTcpAddr, err := net.ResolveTCPAddr("tcp", localAddr)
	util.CheckErr(err, "Could not resolve address: "+localAddr)
	remoteTcpAddr, err := net.ResolveTCPAddr("tcp", remoteAddr)
	util.CheckErr(err, "Could not resolve address: "+remoteAddr)
	conn, err := net.DialTCP("tcp", localTcpAddr, remoteTcpAddr)
	util.CheckErr(err, "Could not connect: "+localAddr+" and "+remoteAddr)

	return conn
}

// GetResult
// Does not reply to callee!
func (d *KVS) GetResult(args *GetResultArgs, reply *interface{}) error {
	trace := d.Tracer.ReceiveToken(args.GToken)
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
	d.NotifyCh <- result
	return nil
}

// PutResult
// Does not reply to callee!
func (d *KVS) PutResult(args *PutResultArgs, reply *interface{}) error {
	trace := d.Tracer.ReceiveToken(args.GToken)
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
	d.NotifyCh <- result
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
