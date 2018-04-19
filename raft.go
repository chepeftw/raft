package main

import (
	"os"
	"fmt"
	"net"
	"time"
	"strconv"
	"math/rand"
	"encoding/json"

	"github.com/op/go-logging"
	"github.com/chepeftw/bchainlibs"
)

// RAFT_MESSAGE_SIZE
// RAFT_SENDING_MESSAGE
// RAFT_AVG_TIME
// RAFT_WINNER
// RAFT_ELECTION_TIME
// RAFT_ATTEND_BUFFER_CHANNEL_START_TIME

// +++++++++ Go-Logging Conf
var log = logging.MustGetLogger("raft")
var format = logging.MustStringFormatter(
	"%{level:.4s}=> %{time:0102 15:04:05.999} %{shortfile} %{message}",
)

// +++++++++ Constants
const (
	DefTimeout    = 2000
)

const (
	FOLLOWER  = iota
	CANDIDATE
	LEADER
	IDLE
)

// +++++++++ Global vars
var state = IDLE
var myIP = net.ParseIP(bchainlibs.LocalhostAddr)

var timeout = 0
var pingSent = 0
var monitoringStartTime = int64(0)

var timer *time.Timer

var rndm = rand.New(rand.NewSource(time.Now().UnixNano()))

// +++++++++ Channels
var buffer = make(chan string)
var output = make(chan string)
var done = make(chan bool)

var votes = make(map[string]int)

var forwarded = make(map[string]bool)
var timestamps = make(map[string]int64)

var timediffs []int64

type Packet struct {
	Source    string `json:"src"`
	Type      int    `json:"tpe"`
	Action    int    `json:"act,omitempty"`
	Vote      string `json:"vot,omitempty"`
	Message   string `json:"msg,omitempty"`
	Timestamp int64  `json:"tst"`
}

func startTimer() {
	startTimerStar(float32(timeout), bchainlibs.RaftTimeout)
}

func startTimerRand() {
	s1 := rndm.Intn(2000000)
	s2 := rndm.Intn(2000000)
	s3 := rndm.Intn(1000000)
	randomTimeout := float32((s1 + s2 + s3) / 2500)

	startTimerStar(randomTimeout, bchainlibs.RaftTimeout)
}

func startTimerStar(localTimeout float32, timeoutType int) {
	if timer != nil {
		timer.Stop()
	}
	log.Debug( myIP.String() + " Starting timer for " + strconv.Itoa(timeoutType) + "ms" )
	timer = time.NewTimer(time.Millisecond * time.Duration( localTimeout ))

	go func() {
		<-timer.C

		payload := Packet{ Source: "0.0.0.0", Type: timeoutType }

		js, err := json.Marshal(payload)
		bchainlibs.CheckError(err, log)
		buffer <- string(js)
		//log.Debug("Timer expired")
	}()
}

func sendRequestVote() {
	payload := Packet{
		Source:    myIP.String(),
		Type:      bchainlibs.RequestForVote,
		Vote:      myIP.String(),
		Timestamp: time.Now().UnixNano(),
	}

	sendMessage(payload)
}

func sendVote(voteFor string) {
	time.Sleep(time.Millisecond * time.Duration(rndm.Intn(500)))
	payload := Packet{
		Source:    myIP.String(),
		Type:      bchainlibs.Vote,
		Vote:      voteFor,
		Timestamp: time.Now().UnixNano(),
	}

	sendMessage(payload)
}

func sendPing() {
	pingSent = pingSent + 1
	payload := Packet{
		Source:    myIP.String(),
		Type:      bchainlibs.LeaderPing,
		Message:   "ping",
		Timestamp: time.Now().UnixNano(),
	}

	sendMessage(payload)
}

func sendMessage(payload Packet) {
	js, err := json.Marshal(payload)
	bchainlibs.CheckError(err, log)
	output <- string(js)

	forwarded[getMessageKey(payload)] = true
	timestamps[getMessageKey(payload)] = time.Now().UnixNano()
}

func getMessageKey(payload Packet) string {
	return payload.Source + "_" + strconv.FormatInt(payload.Timestamp, 10)
}

// Function that handles the output channel
func attendOutputChannel() {
	ServerAddr, err := net.ResolveUDPAddr(bchainlibs.Protocol, bchainlibs.BroadcastAddr+bchainlibs.RaftPort)
	bchainlibs.CheckError(err, log)
	LocalAddr, err := net.ResolveUDPAddr(bchainlibs.Protocol, myIP.String()+":0")
	bchainlibs.CheckError(err, log)
	Conn, err := net.DialUDP(bchainlibs.Protocol, LocalAddr, ServerAddr)
	bchainlibs.CheckError(err, log)
	defer Conn.Close()

	for {
		j, more := <-output
		if more {
			if Conn != nil {
				buf := []byte(j)
				_, err = Conn.Write(buf)
				log.Debug( myIP.String() + " " + j + " RAFT_MESSAGE_SIZE=" + strconv.Itoa(len(buf)) )
				log.Debug( myIP.String() + " RAFT_SENDING_MESSAGE=1" )
				bchainlibs.CheckError(err, log)
			}
		} else {
			fmt.Println("closing channel")
			done <- true
			return
		}
	}
}

func eqIp(a net.IP, b net.IP) bool {
	return bchainlibs.CompareIPs(a, b)
}

func applyVote(ip string) {
	if _, ok := votes[ip]; !ok {
		votes[ip] = 0
	}
	votes[ip] += 1
}

func stopRaft() {
	state = IDLE
	votes = make(map[string]int)
	forwarded = make(map[string]bool)
	timestamps = make(map[string]int64)
	timediffs = []int64{}

	if timer != nil {
		timer.Stop()
	}
}

// Function that handles the buffer channel
func attendBufferChannel() {
	for {
		j, more := <-buffer
		if more {
			attendBufferChannelStartTime := time.Now().UnixNano() // Start time of the reception of a message

			// First we take the json, unmarshal it to an object
			payload := Packet{}
			json.Unmarshal([]byte(j), &payload)

			if _, ok := forwarded[ getMessageKey(payload) ]; !ok && !eqIp(myIP, net.ParseIP(payload.Source)) {
				// Actually any message should be broadcasted
				if !(payload.Type == bchainlibs.RaftTimeout || payload.Type == bchainlibs.EndElection || payload.Type == bchainlibs.StartRaft) { // then broadcast
					// Broadcast it
					sendMessage(payload)
				}
				//log.Debug( myIP.String() + " => message => " + j )

				// Now we start! FSM TIME!
				switch state {
				case IDLE:
					log.Debug( myIP.String() + " => message => " + j )
					if payload.Type == bchainlibs.StartRaft {
						log.Info("ANNOUNCEMENT: START RAFT")
						state = FOLLOWER
						monitoringStartTime = time.Now().UnixNano() // Start time of the leader election process
						startTimerRand()
					}
					break
				case FOLLOWER:
					if payload.Type == bchainlibs.RaftTimeout {
						state = CANDIDATE
						log.Debug(myIP.String() + " => Changing to CANDIDATE!")
						startTimerRand()
					} else if payload.Type == bchainlibs.RequestForVote {
						sendVote(payload.Vote)
						log.Debug(myIP.String() + " => Sending vote for " + payload.Vote)
						startTimer()
					} else if payload.Type == bchainlibs.Vote {
						applyVote(payload.Vote)
						startTimer()
					} else if payload.Type == bchainlibs.LeaderPing {

						var total int64 = 0
						for _, value:= range timediffs {
							total += value
						}
						avgTime := total/int64(len(timediffs))

						startTimer()
						log.Debug(myIP.String() + " => got ping from leader! ")
						log.Debug(myIP.String() + " => RAFT_AVG_TIME=" + strconv.FormatInt(avgTime, 10))
					} else if payload.Type == bchainlibs.StopRaft {
						log.Info("ANNOUNCEMENT: STOP RAFT")
						stopRaft()
					}
					break
				case CANDIDATE:
					if payload.Type == bchainlibs.RaftTimeout {
						sendRequestVote()
						log.Debug(myIP.String() + " => ASKING FOR VOTES!")
						log.Debug(myIP.String() + " => Timeout in " + strconv.Itoa(timeout/2))
						startTimerStar(float32(timeout/2), bchainlibs.EndElection)
					} else if payload.Type == bchainlibs.RequestForVote && !eqIp(myIP, net.ParseIP(payload.Source)) {
						state = FOLLOWER
						sendVote(payload.Vote)
						log.Debug(myIP.String() + " => Sending vote for " + payload.Vote)
						startTimer()
					} else if payload.Type == bchainlibs.Vote {
						log.Debug(myIP.String() + " => Received vote for " + payload.Vote + " from " + payload.Source)
						applyVote(payload.Vote)
					} else if payload.Type == bchainlibs.EndElection {
						winner := "0.0.0.0"
						numberVotes := 0
						for key, value := range votes {
							if value > numberVotes {
								winner = key
								numberVotes = value
							}
						}

						log.Debug(myIP.String() + " => THE WINNER IS " + winner + " with " + strconv.Itoa(numberVotes) + " votes!")

						if winner == myIP.String() {
							state = LEADER
							log.Debug(myIP.String() + " => I AM THE MASTER OF THE UNIVERSE!!! ALL HAIL THE NEW LEADER!")
							log.Debug("RAFT_WINNER=" + myIP.String())
							log.Debug("RAFT_ELECTION_TIME=" + strconv.FormatInt( (time.Now().UnixNano() - monitoringStartTime) / int64(time.Nanosecond), 10 ))
							startTimerStar(float32(timeout/2), bchainlibs.RaftTimeout)
						}
					} else if payload.Type == bchainlibs.LeaderPing {
						state = FOLLOWER
						startTimer()
					} else if payload.Type == bchainlibs.StopRaft {
						log.Info("ANNOUNCEMENT: STOP RAFT")
						stopRaft()
					}
					break
				case LEADER:
					if payload.Type == bchainlibs.RaftTimeout {

						if pingSent > 2 {
							log.Debug("PLEASE_EXIT=1234" )
						}

						sendPing()
						startTimerStar(float32(timeout/2), bchainlibs.RaftTimeout)
					} else if payload.Type == bchainlibs.StopRaft {
						log.Info("ANNOUNCEMENT: STOP RAFT")
						stopRaft()
					}
					break
				default:
					// Welcome to Stranger Things ... THIS REALLY SHOULD NOT HAPPEN
					break
				}
			} else {
				time1 := timestamps[getMessageKey(payload)]
				timediff := time.Now().UnixNano() - time1
				timediffs = append(timediffs, timediff)
			}

			log.Debug("RAFT_ATTEND_BUFFER_CHANNEL_START_TIME=" + strconv.FormatInt( (time.Now().UnixNano() - attendBufferChannelStartTime) / int64(time.Nanosecond), 10 ))

		} else {
			log.Debug("closing channel")
			done <- true
			return
		}

	}
}

// ------------

func main() {
	// Getting timeout from env
	timeout = DefTimeout
	if raftTimeout := os.Getenv("RAFT_TIMEOUT"); raftTimeout != "" {
		timeout, _ = strconv.Atoi(raftTimeout)
	}

	// Getting targetSync from confFile
	targetSync := float64(0)
	standAlone := 1
	if _, err := os.Stat("/app/conf.yml"); err == nil {
		var c bchainlibs.Conf
		c.GetConf("/app/conf.yml")
		targetSync = c.TargetSync
		standAlone = c.RaftStandAlone
	} else {
		if raftTargetSync := os.Getenv("RAFT_TARGET_SYNC"); raftTargetSync != "" {
			targetSyncInt, _ := strconv.Atoi(raftTargetSync)
			targetSync = float64(targetSyncInt)
		}
	}

	// Logger configuration
	var logPath = "/var/log/golang/"
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		os.MkdirAll(logPath, 0777)
	}

	var logFile = logPath + "raft.log"
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Printf("error opening file: %v", err)
	}
	defer f.Close()

	backend := logging.NewLogBackend(f, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logging.DEBUG, "")
	logging.SetBackend(backendLeveled)

	log.Info("")
	log.Info("ENV : RAFT_PORT = " + os.Getenv("RAFT_PORT"))
	log.Info("ENV : RAFT_TIMEOUT = " + os.Getenv("RAFT_TIMEOUT"))
	log.Info("ENV : RAFT_TARGET_SYNC = " + os.Getenv("RAFT_TARGET_SYNC"))
	log.Info("")
	log.Info("FLAGS : Port is " + bchainlibs.RaftPort)
	log.Info("FLAGS : Timeout is " + strconv.Itoa(timeout))
	log.Info("FLAGS : TargetSync is " + strconv.Itoa(int(targetSync)))
	log.Info("")
	for _, element := range os.Args {
		log.Info("FLAG : " + element)
	}
	log.Info("")
	log.Info("------------------------------------------------------------------------")
	log.Info("")
	log.Info("")
	log.Info("Starting RAFT process, waiting some time to get my own IP...")
	// ------------

	// It gives some time for the network to get configured before it gets its own IP.
	// This value should be passed as a environment variable indicating the time when
	// the simulation starts, this should be calculated by an external source so all
	// Go programs containers start at the same UnixTime.
	now := float64(time.Now().Unix())
	sleepTime := 0
	if targetSync > now {
		sleepTime = int(targetSync - now)
		log.Info("SYNC: Sync time is " + strconv.FormatFloat(targetSync, 'f', 6, 64))
	}

	log.Info("SYNC: sleepTime is " + strconv.Itoa(sleepTime))
	time.Sleep(time.Second * time.Duration(sleepTime))
	// ------------

	// But first let me take a selfie, in a Go lang program is getting my own IP
	myIP = bchainlibs.SelfieIP()
	log.Info("Good to go, my ip is " + myIP.String() + " and port is " + bchainlibs.RaftPort)

	// Lets prepare a address at any address at port Port
	ServerAddr, err := net.ResolveUDPAddr(bchainlibs.Protocol, bchainlibs.RaftPort)
	bchainlibs.CheckError(err, log)

	// Now listen at selected port
	ServerConn, err := net.ListenUDP(bchainlibs.Protocol, ServerAddr)
	bchainlibs.CheckError(err, log)
	defer ServerConn.Close()

	// Run the FSM! The one in charge of everything
	go attendBufferChannel()
	// Run the Output! The channel for communicating with the outside world!
	go attendOutputChannel()

	if standAlone > 0 {
		log.Info("ANNOUNCEMENT: Running RAFT Leader Election in Stand Alone MODE")
		log.Info("ANNOUNCEMENT: Forcing Start")
		genesis := Packet{
			Source: "0.0.0.0",
			Type: bchainlibs.StartRaft,
		}
		js, err := json.Marshal(genesis)
		bchainlibs.CheckError(err, log)
		buffer <- string(js)
	} else {
		log.Info("ANNOUNCEMENT: Running RAFT Leader Election with Blockchain Implementation MODE")
		log.Info("ANNOUNCEMENT: Waiting")
	}

	buf := make([]byte, 1024)

	for {
		n, _, err := ServerConn.ReadFromUDP(buf)
		str := string(buf[0:n])

		buffer <- str
		bchainlibs.CheckError(err, log)
	}

	close(buffer)
	close(output)

	<-done
}
