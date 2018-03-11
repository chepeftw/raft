package main

import (
	"os"
	"fmt"
	"net"
	"time"
	"strconv"
	"encoding/json"

	"github.com/op/go-logging"
	"github.com/chepeftw/treesiplibs"
	"math/rand"
)

// +++++++++ Go-Logging Conf
var log = logging.MustGetLogger("raft")
var format = logging.MustStringFormatter(
	"%{level:.4s}=> %{time:0102 15:04:05.999} %{shortfile} %{message}",
)

// +++++++++ Constants
const (
	DefPort       = 10001
	DefTimeout    = 10000
	Protocol      = "udp"
	BroadcastAddr = "255.255.255.255"
	LocalhostAddr = "127.0.0.1"
)

const (
	FOLLOWER  = iota
	CANDIDATE
	LEADER
)

const (
	REQUESTFORVOTETYPE = iota
	VOTETYPE
	TIMEOUTTYPE
	ENDELECTIONTYPE
	PINGTYPE
)

// +++++++++ Global vars
var state = FOLLOWER
var myIP = net.ParseIP(LocalhostAddr)

var timeout = 0

var timer *time.Timer

var rndm = rand.New(rand.NewSource(time.Now().UnixNano()))

// +++++++++ Multi node support
var Port = ":0"
var PortInt = 0

// +++++++++ Channels
var buffer = make(chan string)
var output = make(chan string)
var done = make(chan bool)

var votes = make(map[string]int)

var forwarded = make(map[string]bool)

type Packet struct {
	Source    string `json:"src"`
	Type      int    `json:"tpe"`
	Action    int    `json:"act,omitempty"`
	Vote      string `json:"vot,omitempty"`
	Message   string `json:"msg,omitempty"`
	Timestamp int64  `json:"tst"`
}

func startTimer() {
	startTimerStar(float32(timeout), TIMEOUTTYPE)
}

func startTimerRand() {
	s1 := rndm.Intn(2000000)
	s2 := rndm.Intn(2000000)
	s3 := rndm.Intn(1000000)
	randomTimeout := float32((s1 + s2 + s3) / 1000)

	startTimerStar(randomTimeout, TIMEOUTTYPE)
}

func startTimerStar(localTimeout float32, timeoutType int) {
	if timer != nil {
		timer.Stop()
	}
	timer = time.NewTimer(time.Millisecond * time.Duration( localTimeout ))

	go func() {
		<-timer.C

		payload := Packet{
			Source: "0.0.0.0",
			Type:   timeoutType,
		}

		js, err := json.Marshal(payload)
		treesiplibs.CheckError(err, log)
		buffer <- string(js)
		//log.Debug("Timer expired")
	}()
}

func sendRequestVote() {
	payload := Packet{
		Source:    myIP.String(),
		Type:      REQUESTFORVOTETYPE,
		Vote:      myIP.String(),
		Timestamp: time.Now().UnixNano(),
	}

	sendMessage(payload)
}

func sendVote(voteFor string) {
	time.Sleep(time.Millisecond * time.Duration(rndm.Intn(1000)))
	payload := Packet{
		Source:    myIP.String(),
		Type:      VOTETYPE,
		Vote:      voteFor,
		Timestamp: time.Now().UnixNano(),
	}

	sendMessage(payload)
}

func sendPing() {
	payload := Packet{
		Source:    myIP.String(),
		Type:      PINGTYPE,
		Message:   "ping",
		Timestamp: time.Now().UnixNano(),
	}

	sendMessage(payload)
}

func sendMessage(payload Packet) {
	js, err := json.Marshal(payload)
	treesiplibs.CheckError(err, log)
	output <- string(js)

	forwarded[getMessageKey(payload)] = true
}

func getMessageKey(payload Packet) string {
	return payload.Source + "_" + strconv.FormatInt(payload.Timestamp, 10)
}

// Function that handles the output channel
func attendOutputChannel() {
	ServerAddr, err := net.ResolveUDPAddr(Protocol, BroadcastAddr+Port)
	treesiplibs.CheckError(err, log)
	LocalAddr, err := net.ResolveUDPAddr(Protocol, myIP.String()+":0")
	treesiplibs.CheckError(err, log)
	Conn, err := net.DialUDP(Protocol, LocalAddr, ServerAddr)
	treesiplibs.CheckError(err, log)
	defer Conn.Close()

	for {
		j, more := <-output
		if more {
			if Conn != nil {
				buf := []byte(j)
				_, err = Conn.Write(buf)
				//log.Debug( myIP.String() + " " + j + " MESSAGE_SIZE=" + strconv.Itoa(len(buf)) )
				//log.Info( myIP.String() + " SENDING_MESSAGE=1" )
				treesiplibs.CheckError(err, log)
			}
		} else {
			fmt.Println("closing channel")
			done <- true
			return
		}
	}
}

func eqIp(a net.IP, b net.IP) bool {
	return treesiplibs.CompareIPs(a, b)
}

func getCurrentState() string {
	switch state {
	case FOLLOWER:
		return "Follower"
	case CANDIDATE:
		return "Candidate"
	case LEADER:
		return "Leader"
	default:
		return "Error"
	}
}

func applyVote(ip string) {
	if _, ok := votes[ip]; !ok {
		votes[ip] = 0
	}
	votes[ip] += 1
}

// Function that handles the buffer channel
func attendBufferChannel() {
	for {
		j, more := <-buffer
		if more {
			//attendBufferChannelStartTime := time.Now().UnixNano() // Start time of the monitoring process

			// First we take the json, unmarshal it to an object
			payload := Packet{}
			json.Unmarshal([]byte(j), &payload)

			if _, ok := forwarded[ getMessageKey(payload) ]; !ok && !eqIp(myIP, net.ParseIP(payload.Source)) {
				// Actually any message should be broadcasted
				if !(payload.Type == TIMEOUTTYPE || payload.Type == ENDELECTIONTYPE) { // then broadcast
					// Broadcast it
					sendMessage(payload)
				}

				//log.Debug( myIP.String() + " => message => " + j )

				// Now we start! FSM TIME!
				switch state {
				case FOLLOWER:
					if payload.Type == TIMEOUTTYPE {
						state = CANDIDATE
						log.Debug(myIP.String() + " => Changing to CANDIDATE!")
						startTimerRand()
					} else if payload.Type == REQUESTFORVOTETYPE {
						sendVote(payload.Vote)
						log.Debug(myIP.String() + " => Sending vote for " + payload.Vote)
						startTimer()
					} else if payload.Type == VOTETYPE {
						applyVote(payload.Vote)
						startTimer()
					} else if payload.Type == PINGTYPE {
						startTimer()
						log.Debug(myIP.String() + " => got ping from leader!")
					}
					break
				case CANDIDATE:
					if payload.Type == TIMEOUTTYPE {
						sendRequestVote()
						log.Debug(myIP.String() + " => ASKING FOR VOTES!")
						log.Debug(myIP.String() + " => Timeout in " + strconv.Itoa(timeout/2))
						log.Debug(myIP.String() + " => My current state is " + getCurrentState())
						startTimerStar(float32(timeout/2), ENDELECTIONTYPE)
					} else if payload.Type == REQUESTFORVOTETYPE && !eqIp(myIP, net.ParseIP(payload.Source)) {
						state = FOLLOWER

						sendVote(payload.Vote)
						log.Debug(myIP.String() + " => Sending vote for " + payload.Vote)
						log.Debug(myIP.String() + " => My current state is " + getCurrentState())
						startTimer()
					} else if payload.Type == VOTETYPE {
						log.Debug(myIP.String() + " => Received vote for " + payload.Vote + " from " + payload.Source)
						log.Debug(myIP.String() + " => My current state is " + getCurrentState())
						applyVote(payload.Vote)
					} else if payload.Type == ENDELECTIONTYPE {
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
							startTimerStar(float32(timeout/2), TIMEOUTTYPE)
						}
					}
					break
				case LEADER:
					if payload.Type == TIMEOUTTYPE {
						sendPing()
						startTimerStar(float32(timeout/2), TIMEOUTTYPE)
					}
					break
				default:
					// Welcome to Stranger Things ... THIS REALLY SHOULD NOT HAPPEN
					break
				}
			}

			//log.Debug("ATTEND_BUFFER_CHANNEL_START_TIME=" + strconv.FormatInt( (time.Now().UnixNano() - attendBufferChannelStartTime) / int64(time.Nanosecond), 10 ))

		} else {
			log.Debug("closing channel")
			done <- true
			return
		}

	}
}

// ------------

func main() {
	// Getting port from env
	PortInt = DefPort
	if raftPort := os.Getenv("RAFT_PORT"); raftPort != "" {
		PortInt, _ = strconv.Atoi(raftPort)
	}
	Port = ":" + strconv.Itoa(PortInt)

	// Getting timeout from env
	timeout = DefTimeout
	if raftTimeout := os.Getenv("RAFT_TIMEOUT"); raftTimeout != "" {
		timeout, _ = strconv.Atoi(raftTimeout)
	}

	// Getting targetSync from confFile
	targetSync := float64(0)
	if _, err := os.Stat("/app/conf.yml"); err == nil {
		var c treesiplibs.Conf
		c.GetConf("/app/conf.yml")
		targetSync = c.TargetSync
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

	var logFile = logPath + "raft" + strconv.Itoa(PortInt) + ".log"
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
	log.Info("FLAGS : PortInt is " + strconv.Itoa(PortInt))
	log.Info("FLAGS : Port is " + Port)
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
	myIP = treesiplibs.SelfieIP()
	log.Info("Good to go, my ip is " + myIP.String() + " and port is " + Port)

	// Lets prepare a address at any address at port Port
	ServerAddr, err := net.ResolveUDPAddr(Protocol, Port)
	treesiplibs.CheckError(err, log)

	// Now listen at selected port
	ServerConn, err := net.ListenUDP(Protocol, ServerAddr)
	treesiplibs.CheckError(err, log)
	defer ServerConn.Close()

	// Run the FSM! The one in charge of everything
	go attendBufferChannel()
	// Run the Output! The channel for communicating with the outside world!
	go attendOutputChannel()

	startTimerRand()

	buf := make([]byte, 1024)

	for {
		n, _, err := ServerConn.ReadFromUDP(buf)
		str := string(buf[0:n])

		buffer <- str
		treesiplibs.CheckError(err, log)
	}

	close(buffer)
	close(output)

	<-done
}
