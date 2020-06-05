package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"reflect"
	"strings"
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
	f *os.File
}

var app *App

func (app *App) Reply(reply *common.ClientReply, res *clientrpc.EmptyResponse) error {
	reply.LogRequest(true)
	if reply.Err == nil || !strings.Contains(*(reply.Err), "invalid operation status") {
		app.f.WriteString(fmt.Sprintf("%d", time.Now().UnixNano()))
		app.f.Write([]byte{'\n'})
	}
	return nil
}

func (app *App) Init() {
	opTypes = []string{"CREATE", "READ", "UPDATE", "DELETE"}
	createdKeys = make(map[string]bool)
	config := serviceconfig.GetConfig()
	log2.Init(config.Id)
	var file *os.File
	file, err := os.OpenFile("client_reqs.csv", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("Failed to open file client_reqs.csv; err: %v", err)
	}
	app.f = file
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
}

func (app *App) MakeCalls() {
		for {
			time.Sleep(1000 * time.Millisecond)
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

	app.f.WriteString(fmt.Sprintf("%d,", time.Now().UnixNano()))
	for _, replica := range app.replicas {
		req.LogRequest(false)
		replica.Client.AsyncDo("Application.Request", req, &clientrpc.EmptyResponse{})
	}

}

func main() {
	serviceconfig.Init()
	app = &App{
		replicas: make([]*Replica, 0),
		replyQueue: make(chan *common.ClientReply),
	}
	app.Init()
	defer app.f.Close()
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
