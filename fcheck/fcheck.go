/*

This package specifies the API to the failure checking library to be
used in assignment 2 of UBC CS 416 2021W2.

You are *not* allowed to change the API below. For example, you can
modify this file by adding an implementation to Stop, but you cannot
change its API.

*/

package fchecker

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"os"
)
import "time"

////////////////////////////////////////////////////// DATA
// Define the message types fchecker has to use to communicate to other
// fchecker instances. We use Go's type declarations for this:
// https://golang.org/ref/spec#Type_declarations

// Heartbeat message.
type HBeatMessage struct {
	EpochNonce uint64 // Identifies this fchecker instance/epoch.
	SeqNum     uint64 // Unique for each heartbeat in an epoch.
}

// An ack message; response to a heartbeat.
type AckMessage struct {
	HBEatEpochNonce uint64 // Copy of what was received in the heartbeat.
	HBEatSeqNum     uint64 // Copy of what was received in the heartbeat.
}

// Notification of a failure, signal back to the client using this
// library.
type FailureDetected struct {
	UDPIpPort string    // The RemoteIP:RemotePort of the failed node.
	Timestamp time.Time // The time when the failure was detected.
}

////////////////////////////////////////////////////// API

type StartStruct struct {
	AckLocalIPAckLocalPort       string
	EpochNonce                   uint64
	HBeatLocalIPHBeatLocalPort   string
	HBeatRemoteIPHBeatRemotePort string
	LostMsgThresh                uint8
}

// Global variables
var conn *net.UDPConn
var ln *net.UDPConn
var globalCh = make(chan FailureDetected)
var listenCh = make(chan int)
var monitorCh = make(chan int)

// Starts the fcheck library.
func Start(arg StartStruct) (notifyCh <-chan FailureDetected, err error) {
	// Respond to heartbeats
	StartListener(arg.AckLocalIPAckLocalPort)

	if arg.HBeatLocalIPHBeatLocalPort == "" {
		// ONLY arg.AckLocalIPAckLocalPort is set; do not monitor any nodes
		return nil, nil
	}
	// Else: ALL fields in arg are set; start monitoring a node

	// Initialize connection to remote node
	localAddr, err := net.ResolveUDPAddr("udp", arg.HBeatLocalIPHBeatLocalPort)
	CheckErr(err, "Error converting UDP address: %v\n", err)
	remoteAddr, err := net.ResolveUDPAddr("udp", arg.HBeatRemoteIPHBeatRemotePort)
	CheckErr(err, "Error converting UDP address: %v\n", err)
	conn, err = net.DialUDP("udp", localAddr, remoteAddr)
	CheckErr(err, "Couldn't create a connection between", localAddr, "and", remoteAddr)

	// Monitor node
	go monitor(conn, arg.EpochNonce, arg.LostMsgThresh, arg.HBeatRemoteIPHBeatRemotePort)

	return globalCh, nil
}

// Starts a heartbeat listener
func StartListener(ackLocalIPAckLocalPort string) net.Addr {
	// Create listener for heartbeats
	ackAddr, err := net.ResolveUDPAddr("udp", ackLocalIPAckLocalPort)
	ln, err := net.ListenUDP("udp", ackAddr)
	CheckErr(err, "Could not listen on", ackLocalIPAckLocalPort)

	// Listen forever...
	go listen(ln)

	// Return address that fcheck is listening on
	return ln.LocalAddr()
}

// Tells the library to stop monitoring/responding acks.
func Stop() {
	listenCh <- 1
	fmt.Println("Notified to stop listening")
	err := ln.Close()
	CheckErr(err, "Error closing listener conn", err)

	monitorCh <- 1
	fmt.Println("Notified to stop monitoring")
	err = conn.Close()
	CheckErr(err, "Error closing monitor conn conn", err)

	return
}

func CheckErr(err error, errfmsg string, fargs ...interface{}) {
	if err != nil {
		fmt.Fprintf(os.Stderr, errfmsg, fargs...)
		os.Exit(1)
	}
}

// helper fn for listen
func sendAck(ack *AckMessage, ln *net.UDPConn, remoteAddr *net.UDPAddr) {
	ln.WriteTo(encodeAck(ack), remoteAddr)
	// assume it went through, if it didn't, we'll just retry after a timeout
}

// helper fn for monitor
func sendHeartbeat(heartbeat *HBeatMessage, conn net.Conn) {
	conn.Write(encodeHeartbeat(heartbeat))
	// assume it went through, if it didn't, we'll just retry after a timeout
}

// helper fn for listen
func receiveHeartbeat(heartbeat *HBeatMessage, ln *net.UDPConn) (*net.UDPAddr, error) {
	recvBuf := make([]byte, 1024)

	ln.SetReadDeadline(time.Now().Add(time.Duration(1) * time.Second))
	numRead, remoteAddr, err := ln.ReadFromUDP(recvBuf)
	if err != nil {
		// timeout, try again
		return nil, err
	} else {
		if numRead > 0 {
			decoded, err := decodeHeartbeat(recvBuf, numRead)
			if err != nil {
				return remoteAddr, err

			}
			*heartbeat = decoded
			return remoteAddr, nil
		}
		return remoteAddr, errors.New("this did not read anything")
	}
}

// helper fn for monitor
func receiveTimedAck(ack *AckMessage, conn net.Conn, rtt time.Duration) (time.Time, error) {
	recvBuf := make([]byte, 1024)

	// Set timeout based on RTT
	conn.SetReadDeadline(time.Now().Add(rtt))
	numRead, err := conn.Read(recvBuf)
	if err != nil {
		// Timeout
		return time.Now(), err
	}
	decoded, err := decodeAck(recvBuf, numRead)
	if err != nil {
		// Corruption
		return time.Now(), err
	}
	*ack = decoded
	return time.Now(), nil
}

// helper fn for listen
func encodeAck(msg *AckMessage) []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(msg)
	return buf.Bytes()
}

// helper fn for monitor
func encodeHeartbeat(msg *HBeatMessage) []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(msg)
	return buf.Bytes()
}

// helper function for monitor
func decodeAck(buf []byte, len int) (AckMessage, error) {
	var decoded AckMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decoded)
	if err != nil {
		return AckMessage{}, err
	}
	return decoded, nil
}

// helper function for listen
func decodeHeartbeat(buf []byte, len int) (HBeatMessage, error) {
	var decoded HBeatMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decoded)
	if err != nil {
		return HBeatMessage{}, err
	}
	return decoded, nil
}

// helper fn for monitor
func createAndSendHeartbeat(conn net.Conn, nonce uint64, num uint64) time.Time {
	heartbeat := HBeatMessage{
		EpochNonce: nonce,
		SeqNum:     num,
	}
	sendHeartbeat(&heartbeat, conn)
	return time.Now()
}

// listens for heartbeats and responds
func listen(ln *net.UDPConn) {
	for {
		select {
		case <-listenCh:
			fmt.Println("--- STOP LISTENING ---")
			return
		case <-time.After(time.Duration(10) * time.Microsecond):
			// Receive heartbeats
			var heartbeat HBeatMessage
			remoteAddr, err := receiveHeartbeat(&heartbeat, ln)
			if err != nil {
				continue
			}

			// Send ack
			ack := AckMessage{
				HBEatEpochNonce: heartbeat.EpochNonce,
				HBEatSeqNum:     heartbeat.SeqNum,
			}
			sendAck(&ack, ln, remoteAddr)
			break
		}
	}
}

// monitors a node by sending heartbeats and checking for acks
func monitor(conn net.Conn, epochNonce uint64, lostMsgThreshold uint8, remotePort string) {
	var seqNum uint64 = 0
	var lostMsgs uint8 = 0    // num un-acked messages so far
	var rtt = time.Second * 3 // default 3 second timeout
	var sendTimes = make(map[uint64]time.Time)
	var failed = false

	for {
		select {
		case <-monitorCh:
			return
		case <-time.After(rtt / 2):
			if !failed {
				sendTimes[seqNum] = createAndSendHeartbeat(conn, epochNonce, seqNum)

				var ack AckMessage
				timeReceived, err := receiveTimedAck(&ack, conn, rtt)

				if err != nil {
					fmt.Println("timeout occurred")
					// Timeout
					if lostMsgs >= lostMsgThreshold {
						fmt.Println("Failure has been detected.")
						globalCh <- FailureDetected{UDPIpPort: remotePort, Timestamp: time.Now()}
						fmt.Println("Notification sent.")
						failed = true
					} else {
						lostMsgs++
					}
				} else {
					// No timeout
					if ack.HBEatEpochNonce == epochNonce {
						// Update RTT
						newRtt := timeReceived.Sub(sendTimes[seqNum])
						rtt = (rtt + newRtt) / 2 // Update RTT
						fmt.Println("new rtt", rtt)

						lostMsgs = 0
						if ack.HBEatSeqNum == seqNum {
							seqNum++
						}

					} else {
						fmt.Println("ignored message")
						// ignore the msg; it's not ours
					}
				}
			}
			break
		}
	}
}
