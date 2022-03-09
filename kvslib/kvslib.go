package kvslib

import (
	"container/list"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
	"math"
	"net"
	"net/rpc"
	"sync"
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
	Token        tracing.TracingToken
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
	RemoteKVS       RemoteKVS
}

type RemoteKVS struct {
	NotifyCh     NotifyChannel // Nested so that KVS still has NotifyCh property (in case it matters for grading)
	RTT          time.Duration
	Tracer       *tracing.Tracer
	InProgress   map[uint32]time.Time // Map representing sent requests that haven't been responded to
	Mutex        *sync.RWMutex
	Puts         map[string]uint32 // int representing number of puts on the key
	BufferedGets map[string]*list.List
	LowerOpId    uint32
	UpperOpId    uint32
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
	d.RemoteKVS = RemoteKVS{
		RTT:          3 * time.Second,
		Tracer:       localTracer,
		NotifyCh:     make(NotifyChannel, chCapacity),
		InProgress:   make(map[uint32]time.Time),
		Mutex:        new(sync.RWMutex),
		Puts:         make(map[string]uint32),
		BufferedGets: make(map[string]*list.List),
		LowerOpId:    0,
		UpperOpId:    uint32(math.Pow(2, 16)),
	}
	d.NotifyCh = d.RemoteKVS.NotifyCh
	d.LocalCoordAddr = localCoordIPPort
	d.RemoteCoordAddr = coordIPPort
	d.LocalHeadAddr = localHeadServerIPPort
	d.LocalTailAddr = localTailServerIPPort

	// Tracing
	d.KTrace.RecordAction(KvslibStart{clientId})

	// Setup RPC
	err := rpc.RegisterName("KVS", &d.RemoteKVS)
	if err != nil {
		return nil, err
	}
	d.HeadListener, err = startRPCListener(localHeadServerIPPort)
	util.CheckErr(err, "Could not start head listener in Start")
	d.TailListener, err = startRPCListener(localTailServerIPPort)
	util.CheckErr(err, "Could not start tail listener in Start")

	// Connect to coord
	err = d.contactCoord()
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

	d.RemoteKVS.Mutex.RLock()
	numOutstanding, exists := d.RemoteKVS.Puts[key]
	d.RemoteKVS.Mutex.RUnlock()
	var localOpId uint32
	if exists {
		localOpId = d.RemoteKVS.UpperOpId
		d.RemoteKVS.UpperOpId++

		if numOutstanding > 0 {
			// Outstanding put(s); buffer for later
			getArgs := d.createGetArgs(tracer, clientId, key, localOpId)
			d.RemoteKVS.BufferedGets[key].PushBack(getArgs)
			return localOpId, nil
		}
	} else {
		localOpId = d.RemoteKVS.LowerOpId
		d.RemoteKVS.LowerOpId++
		if d.RemoteKVS.LowerOpId == uint32(math.Pow(2, 16)) {
			temp := d.RemoteKVS.UpperOpId
			d.RemoteKVS.LowerOpId = temp
			d.RemoteKVS.UpperOpId = (uint32(math.Pow(2, 32)) - d.RemoteKVS.LowerOpId) / 2
		}
	}
	getArgs := d.createGetArgs(tracer, clientId, key, localOpId)
	d.sendGet(getArgs)
	return localOpId, nil

}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value OpId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	// Should return OpId or error
	localOpId := d.RemoteKVS.UpperOpId
	d.RemoteKVS.UpperOpId++

	// Update map to have an outstanding put
	d.RemoteKVS.Mutex.Lock()
	num, exists := d.RemoteKVS.Puts[key]
	if exists {
		d.RemoteKVS.Puts[key] = num + 1
	} else {
		d.RemoteKVS.Puts[key] = 1
		d.RemoteKVS.BufferedGets[key] = list.New()
	}
	d.RemoteKVS.Mutex.Unlock()

	trace := tracer.CreateTrace()
	trace.RecordAction(Put{clientId, localOpId, key, value})

	// Send put to head via RPC
	putArgs := PutArgs{
		ClientId:     clientId,
		OpId:         localOpId,
		Key:          key,
		GId:          0,
		Value:        value,
		Token:        trace.GenerateToken(),
		ClientIPPort: d.LocalTailAddr, // Receives result from tail
	}
	conn, client := makeClient(d.LocalHeadAddr, d.RemoteHeadAddr)
	d.RemoteKVS.Mutex.Lock()
	d.RemoteKVS.InProgress[localOpId] = time.Now()
	d.RemoteKVS.Mutex.Unlock()
	var gid uint64
	err := client.Call("Server.Put", putArgs, &gid)
	if err != nil {
		return 0, err
	}

	// Result should be received from tail via KVS.PutResult()
	go handlePutTimeout(d, gid, putArgs, conn, client)
	return localOpId, nil
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	// pass tracer
	d.KTrace.RecordAction(KvslibStop{d.ClientId})
	err := d.RemoteKVS.Tracer.Close()
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
func (d *KVS) contactCoord() error {
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
func (remoteKVS *RemoteKVS) GetResult(args *GetRes, _ *interface{}) error {
	remoteKVS.Mutex.RLock()
	_, exists := remoteKVS.InProgress[args.OpId]
	remoteKVS.Mutex.RUnlock()
	if !exists {
		// Do nothing
		return nil
	}
	trace := remoteKVS.Tracer.ReceiveToken(args.GToken)
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
	remoteKVS.updateInProgressAndRtt(args.OpId)
	remoteKVS.NotifyCh <- result
	return nil
}

// PutResult Confirms that a Put succeeded
// Does not reply to callee!
func (remoteKVS *RemoteKVS) PutResult(args *PutRes, _ *interface{}) error {
	remoteKVS.Mutex.RLock()
	_, exists := remoteKVS.InProgress[args.OpId]
	remoteKVS.Mutex.RUnlock()
	if !exists {
		// Do nothing
		return nil
	}
	trace := remoteKVS.Tracer.ReceiveToken(args.PToken)
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
	remoteKVS.updateInProgressAndRtt(args.OpId)
	remoteKVS.NotifyCh <- result
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
func handleGetTimeout(d *KVS, getArgs GetArgs, conn *net.TCPConn, client *rpc.Client) {
	for {
		select {
		case <-time.After(d.RemoteKVS.RTT):
			d.RemoteKVS.Mutex.RLock()
			_, exists := d.RemoteKVS.InProgress[getArgs.OpId]
			d.RemoteKVS.Mutex.RUnlock()
			if exists {
				// opId is still in progress
				err := d.contactCoord()
				util.CheckErr(err, "Could not contact coord")
				client.Go("Server.Get", getArgs, nil, nil)
			} else {
				// opId is not in progress; Get has been completed
				client.Close()
				conn.Close()
				return
			}
		}
	}
}

// Checks if a Put request may have been interrupted due to server failure and responds accordingly
func handlePutTimeout(d *KVS, gId uint64, putArgs PutArgs, conn *net.TCPConn, client *rpc.Client) {
	for {
		select {
		case <-time.After(d.RemoteKVS.RTT):
			d.RemoteKVS.Mutex.RLock()
			_, exists := d.RemoteKVS.InProgress[putArgs.OpId]
			d.RemoteKVS.Mutex.RUnlock()
			if exists {
				// opId is still in progress
				err := d.contactCoord()
				util.CheckErr(err, "Could not contact coord")
				putArgs.GId = gId // Update gid for re-send identification
				client.Go("Server.Put", putArgs, nil, nil)
				return
			} else {
				// opId is not in progress
				// Send buffered gets (if any)
				d.RemoteKVS.Mutex.Lock()
				num, _ := d.RemoteKVS.Puts[putArgs.Key]
				d.RemoteKVS.Puts[putArgs.Key] = num - 1
				d.RemoteKVS.Mutex.Unlock()
				d.sendBufferedGets(putArgs.Key)
				client.Close()
				conn.Close()
				return
			}
		}
	}
}

// Updates a KVS's estimated RTT based on an operation's RTT
func (remoteKVS *RemoteKVS) updateInProgressAndRtt(opId uint32) {
	remoteKVS.Mutex.Lock()
	newRtt := time.Now().Sub(remoteKVS.InProgress[opId])
	remoteKVS.RTT = (remoteKVS.RTT + newRtt) / 2
	delete(remoteKVS.InProgress, opId)
	remoteKVS.Mutex.Unlock()
}

// Creates GetArgs struct for a new Get
func (d *KVS) createGetArgs(tracer *tracing.Tracer, clientId string, key string, localOpId uint32) GetArgs {
	trace := tracer.CreateTrace()
	getArgs := GetArgs{
		ClientId:     clientId,
		OpId:         localOpId,
		Key:          key,
		GToken:       trace.GenerateToken(),
		ClientIPPort: d.LocalTailAddr, // Receives result from tail
	}
	return getArgs
}

// Sends a Get request to a server and prepares to receive the result
func (d *KVS) sendGet(getArgs GetArgs) {
	// Send get to tail via RPC
	trace := d.RemoteKVS.Tracer.ReceiveToken(getArgs.GToken)
	trace.RecordAction(Get{getArgs.ClientId, getArgs.OpId, getArgs.Key})
	conn, client := makeClient(d.LocalTailAddr, d.RemoteTailAddr)
	d.RemoteKVS.Mutex.Lock()
	d.RemoteKVS.InProgress[getArgs.OpId] = time.Now()
	d.RemoteKVS.Mutex.Unlock()
	client.Go("Server.Get", getArgs, nil, nil)

	// Result should be received from tail via KVS.GetResult()
	go handleGetTimeout(d, getArgs, conn, client)
}

// Sends the buffered Gets in a KVS associated with key
func (d *KVS) sendBufferedGets(key string) {
	bufferedGets := d.RemoteKVS.BufferedGets[key]
	d.RemoteKVS.Mutex.Lock()
	for bufferedGets.Len() > 0 {
		getArgs := bufferedGets.Front()
		bufferedGets.Remove(getArgs)
		d.sendGet(getArgs.Value.(GetArgs))
	}
	d.RemoteKVS.Mutex.Unlock()
}
