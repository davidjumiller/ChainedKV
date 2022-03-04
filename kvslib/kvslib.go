package kvslib

import (
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
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
	notifyCh NotifyChannel
	// Add more KVS instance state here.
	clientId    string
	tracer      *tracing.Tracer
	coordIPPort string
	opId        uint32
	localMap    map[string]string
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,
	}
}

// Start Starts the instance of KVS to use for connecting to the system with the given coord's IP:port.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the coord, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, coordIPPort string, localCoordIPPort string, localHeadServerIPPort string, localTailServerIPPort string, chCapacity int) (NotifyChannel, error) {
	d.tracer = localTracer
	trace := localTracer.CreateTrace()
	trace.RecordAction(KvslibStart{clientId})

	d.notifyCh = make(NotifyChannel, chCapacity)
	d.opId = 0

	// TODO use chained servers
	// TODO add fcheck to monitor coord
	/*
		// Connect to coord
		localCoordAddr, err := net.ResolveUDPAddr("udp", localCoordIPPort)
		if err != nil {
			return nil, err
		}
		coordAddr, err := net.ResolveUDPAddr("udp", coordIPPort)
		if err != nil {
			return nil, err
		}
		coordConn, err := net.DialUDP("udp", localCoordAddr, coordAddr)
		if err != nil {
			return nil, err
		}

		// Send head request
		var bufArray bytes.Buffer
		headReq := HeadReq{clientId}
		trace.RecordAction(headReq)
		gob.NewEncoder(&bufArray).Encode(&headReq)
		coordConn.Write(bufArray.Bytes())

		// Get head response
		recvBuf := make([]byte, 1024)
		var headRes HeadResRecvd
		numRead, err := coordConn.Read(recvBuf)
		err = gob.NewDecoder(bytes.NewBuffer(recvBuf[0:numRead])).Decode(&headRes)
		if err != nil {
			return nil, err
		}
		trace.RecordAction(headRes)

		// Send tail request
		tailReq := TailReq{clientId}
		trace.RecordAction(tailReq)
		gob.NewEncoder(&bufArray).Encode(&tailReq)
		coordConn.Write(bufArray.Bytes())

		// Get tail response
		recvBuf = make([]byte, 1024)
		var tailRes TailResRecvd
		numRead, err = coordConn.Read(recvBuf)
		err = gob.NewDecoder(bytes.NewBuffer(recvBuf[0:numRead])).Decode(&tailRes)
		if err != nil {
			return nil, err
		}
		trace.RecordAction(tailRes)
	*/

	return d.notifyCh, nil
}

// Get  non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value opId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	// Should return opId or error

	localOpId := d.opId
	d.opId++

	// TODO convert to async with server
	// Send get to tail
	trace := tracer.CreateTrace()
	trace.RecordAction(Get{clientId, localOpId, key})

	// Get result from tail
	value := d.localMap[key]
	trace.RecordAction(GetResultRecvd{localOpId, uint64(localOpId), key, value})

	//return 0, errors.New("not implemented")
	return localOpId, nil
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value opId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	// Should return opId or error

	localOpId := d.opId
	d.opId++

	// TODO convert to async with server
	// Send put to head
	trace := tracer.CreateTrace()
	trace.RecordAction(Put{clientId, localOpId, key, value})

	// Get result from tail
	trace.RecordAction(PutResultRecvd{localOpId, uint64(localOpId), key})

	//return 0, errors.New("not implemented")
	return localOpId, nil
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	trace := d.tracer.CreateTrace()
	trace.RecordAction(KvslibStop{d.clientId})
	err := d.tracer.Close()
	util.CheckErr(err, "The KVS tracer could not be closed")
	return
}
