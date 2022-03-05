package kvslib

import (
	"bytes"
	"cs.ubc.ca/cpsc416/a3/util"
	"encoding/gob"
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
	clientId         string
	tracer           *tracing.Tracer
	localCoordIPPort string
	coordIPPort      string
	headLocalIP      string
	headRemoteIP     string
	tailLocalIP      string
	tailRemoteIP     string
	headServerId     uint8
	tailServerId     uint8
	opId             uint32
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

	// Connect to coord
	conn := makeConnection(localCoordIPPort, coordIPPort)

	// Get head server from coord
	traceAndSendHeadReq(conn, clientId, trace)
	d.headServerId = recvAndTraceHeadRes(conn, trace)

	// Get tail server from coord
	traceAndSendTailReq(conn, clientId, trace)
	d.tailServerId = recvAndTraceTailRes(conn, trace)

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

	// Send get to tail
	conn := makeConnection(d.tailLocalIP, d.tailRemoteIP)
	client := rpc.NewClient(conn)
	// TODO encode get?

	get := Get{clientId, localOpId, key}
	trace := tracer.CreateTrace()
	trace.RecordAction(get)

	err := client.Call("Get", get, &d.notifyCh)
	if err != nil {
		return 0, err
	}
	go receiveGetResult(get, trace, d.notifyCh)

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

	// Send put to head
	conn := makeConnection(d.headLocalIP, d.headRemoteIP)
	client := rpc.NewClient(conn)

	put := Put{clientId, localOpId, key, value}
	trace := tracer.CreateTrace()
	trace.RecordAction(put)

	// Get result from tail
	err := client.Call("Put", put, &d.notifyCh)
	if err != nil {
		return 0, err
	}
	go receivePutResult(put, trace, d.notifyCh)

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

// Creates a TCP connection between localAddr and remoteAddr
func makeConnection(localAddr string, remoteAddr string) *net.TCPConn {
	localTcpAddr, err := net.ResolveTCPAddr("tcp", localAddr)
	util.CheckErr(err, "Could not resolve address: "+localAddr)
	remoteTcpAddr, err := net.ResolveTCPAddr("tcp", remoteAddr)
	util.CheckErr(err, "Could not resolve address: "+remoteAddr)
	conn, err := net.DialTCP("tcp", localTcpAddr, remoteTcpAddr)
	util.CheckErr(err, "Could not connect: "+localAddr+" and "+remoteAddr)

	return conn
}

func traceAndSendHeadReq(conn *net.TCPConn, clientId string, trace *tracing.Trace) {
	var bufArray bytes.Buffer
	headReq := HeadReq{clientId}
	trace.RecordAction(headReq)
	err := gob.NewEncoder(&bufArray).Encode(&headReq)
	util.CheckErr(err, "Could not encode KVS head request")
	_, err = conn.Write(bufArray.Bytes())
	util.CheckErr(err, "Could not send KVS head request")
}

func recvAndTraceHeadRes(conn *net.TCPConn, trace *tracing.Trace) uint8 {
	recvBuf := make([]byte, 1024)
	var headRes HeadResRecvd
	// TODO use fcheck
	err := conn.SetReadDeadline(time.Now().Add(time.Duration(1) * time.Second))
	util.CheckErr(err, "Could not set read deadline for KVS head response")
	numRead, err := conn.Read(recvBuf)
	err = gob.NewDecoder(bytes.NewBuffer(recvBuf[0:numRead])).Decode(&headRes)
	if err != nil {
		// Timeout; try again
		recvAndTraceHeadRes(conn, trace)
	}
	trace.RecordAction(headRes)
	return headRes.ServerId
}

func traceAndSendTailReq(conn *net.TCPConn, clientId string, trace *tracing.Trace) {
	var bufArray bytes.Buffer
	tailReq := TailReq{clientId}
	trace.RecordAction(tailReq)
	err := gob.NewEncoder(&bufArray).Encode(&tailReq)
	util.CheckErr(err, "Could not encode KVS tail request")
	_, err = conn.Write(bufArray.Bytes())
	util.CheckErr(err, "Could not send KVS tail request")
}

func recvAndTraceTailRes(conn *net.TCPConn, trace *tracing.Trace) uint8 {
	recvBuf := make([]byte, 1024)
	var tailRes TailResRecvd
	err := conn.SetReadDeadline(time.Now().Add(time.Duration(1) * time.Second))
	util.CheckErr(err, "Could not set read deadline for KVS tail response")
	numRead, err := conn.Read(recvBuf)
	err = gob.NewDecoder(bytes.NewBuffer(recvBuf[0:numRead])).Decode(&tailRes)
	if err != nil {
		// Timeout; try again
		recvAndTraceTailRes(conn, trace)
	}
	trace.RecordAction(tailRes)
	return tailRes.ServerId
}

func receiveGetResult(get Get, trace *tracing.Trace, notifyCh NotifyChannel) {
	for {
		select {
		case result := <-notifyCh:
			// TODO decode result?
			if result.OpId == get.OpId {
				getResult := GetResultRecvd{
					OpId:  result.OpId,
					GId:   result.GId,
					Key:   get.Key,
					Value: result.Result,
				}
				trace.RecordAction(getResult)
				return
			}
		case <-time.After(time.Duration(100)):
			// TODO resend request?
		}
	}
}

func receivePutResult(put Put, trace *tracing.Trace, ch NotifyChannel) {
	for {
		select {
		case result := <-ch:
			// TODO decode result?
			if result.OpId == put.OpId {
				putResult := PutResultRecvd{
					OpId: result.OpId,
					GId:  result.GId,
					Key:  put.Key,
				}
				trace.RecordAction(putResult)
			}
		case <-time.After(time.Duration(100)):
			// TODO resend request?
		}
	}
}
