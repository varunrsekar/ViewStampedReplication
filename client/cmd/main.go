package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"
	"viewStampedReplication/client/serviceconfig"
	"viewStampedReplication/clientrpc"
	"viewStampedReplication/common"
	log2 "viewStampedReplication/log"
)

var opTypes []string
var createdKeys map[string]bool
type Replica struct {
	clientrpc.Client
	Id int
	Port int
}
type App struct {
	Id string
	Hostname string
	RequestId int
	replicas []*Replica
	replyQueue chan *common.ClientReply
}

type timestamp struct {
	start int64
	end int64

}

var reqRate = 3000000 * time.Microsecond
var reqLines[]timestamp
var done chan bool
var app *App

func (app *App) Reply(reply *common.ClientReply, res *clientrpc.EmptyResponse) error {
	reply.LogRequest(true)
	if reply.Err == nil || !strings.Contains(*(reply.Err), "invalid operation status") {
		reqLines[reply.Res.RequestId-1].end = time.Now().UnixNano()
		if reply.Res.RequestId == 500 {
			done <- true
		}
	}
	return nil
}

func (app *App) Init() {
	opTypes = []string{"CREATE", "READ", "UPDATE", "DELETE"}
	createdKeys = make(map[string]bool)
	config := serviceconfig.GetConfig()
	log2.Init(config.Id)
	app.Id = config.Id
	app.Hostname = fmt.Sprintf("localhost:%d", config.Port)
	for _, replica := range config.Replicas {
		r := &Replica{
			Client: clientrpc.Client{
				Hostname: fmt.Sprintf("localhost:%d", replica.Port),
			},
			Id: replica.Id,
			Port: replica.Port,
		}
		app.replicas = append(app.replicas, r)
	}
	rand.Seed(time.Now().Unix())
	go app.MakeCalls()
	go shutdownHandler()
	go func() {
			<- done
			syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	}()
}

func (app *App) MakeCalls() {
		for {
			time.Sleep(reqRate)
			keys := reflect.ValueOf(createdKeys).MapKeys()
			var existingKey string
			if len(keys) == 0 {
				existingKey = fmt.Sprintf("%s-%d-key", app.Id, time.Now().Unix())
			} else {
				existingKey = keys[rand.Intn(len(keys))].String()
			}
			newVal := fmt.Sprintf("%s-%d-val", app.Id, time.Now().Unix())
			op := opTypes[rand.Intn(len(opTypes))]
			switch op {
			case "CREATE":
				key := fmt.Sprintf("%s-%d-key", app.Id, time.Now().Unix())
				createdKeys[key] = true
				app.MakeReq(op, key, &newVal)
			case "READ", "DELETE":
				app.MakeReq(op, existingKey, nil)
			case "UPDATE":
				app.MakeReq(op, existingKey, &newVal)
			}
		}
}

func (app *App) MakeReq(action string, key string, val *string) {
	app.RequestId++
	req := &common.ClientRequest{
		Op:        log2.Message{
			Action: action,
			Key:    key,
			Val:    val,
		},
		ClientId:  app.Id,
		RequestId: app.RequestId,
	}

	reqLines = append(reqLines, timestamp{
		start: time.Now().UnixNano(),
	})
	//app.f.WriteString(fmt.Sprintf("%d,", time.Now().UnixNano()))
	for _, replica := range app.replicas {
		req.LogRequest(false)
		replica.Client.AsyncDo("Application.Request", req, &clientrpc.EmptyResponse{})
	}

}

func main() {
	reqLines = make([]timestamp, 0)
	done = make(chan bool)
	serviceconfig.Init()
	app = &App{
		replicas: make([]*Replica, 0),
		replyQueue: make(chan *common.ClientReply),
	}
	app.Init()
	//defer app.f.Close()
	err := rpc.RegisterName("Client", app)
	if err != nil {
		log.Fatal("Failed to register RPC; err: ", err)
	}
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", app.Hostname)
	if err != nil {
		log.Fatal("Failed to create RPC listener; err: ", err)
	}
	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatal("Failed to serve HTTP; err: ", err)
	}
}

// Handle shutdown and dump perf analysis.
func shutdownHandler() {
	c := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	diffs := make([]float64, 0)
	var total float64
	for _, req := range reqLines {
		if req.end == 0 {
			continue
		}
		diffs = append(diffs, float64(req.end-req.start)/1000000)
		total += diffs[len(diffs)-1]
	}
	log.Printf("Mean response time: %f", math.Round(total/float64(len(diffs))))
	sort.Float64s(diffs)
	medianIdx := len(diffs)/2
	if len(diffs)%2 != 0 {
		log.Printf("Median response time: %f", diffs[medianIdx])
	} else {
		log.Printf("Median response time: %f", (diffs[medianIdx-1]+diffs[medianIdx])/2)
	}
	log.Printf("Min response time: %f", diffs[0])
	log.Printf("Max response time: %f", diffs[len(diffs)-1])
	os.Exit(1)
}

func init() {
	http.DefaultClient.Timeout = time.Second
}