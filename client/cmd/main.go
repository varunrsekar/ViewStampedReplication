package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"viewStampedReplication/client/serviceconfig"
	"viewStampedReplication/clientrpc"
	"viewStampedReplication/common"
	log2 "viewStampedReplication/log"
)

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

var app *App

func (app *App) Reply(reply *common.ClientReply, res *clientrpc.EmptyResponse) error {
	reply.LogRequest(true)
	return nil
}

func (app *App) Init() {
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
	app.MakeCalls()
}

func (app *App) MakeCalls() {
	go func() {
		time.Sleep(5 * time.Second)
		var val = "a1val"
		app.MakeReq("CREATE", "a1key", &val)
		app.MakeReq("GET", "a1key", nil)
	}()
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
