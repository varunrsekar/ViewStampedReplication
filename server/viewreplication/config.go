package viewreplication

import (
	"fmt"
	"log"
	"net"
	"time"
	"viewStampedReplication/clientrpc"
)

type Status int
const (
	StatusNormal Status = iota
	StatusViewChange
	StatusRecover
)

func (s Status) String() string {
	switch s{
	case StatusNormal:
		return "Normal"
	case StatusViewChange:
		return "ViewChange"
	case StatusRecover:
		return "Recover"
	}
	return "Unknown"
}

type Role int

const (
	RolePrimary Role = iota
	RoleBackup
)

func (r Role) String() string {
	switch r {
	case RolePrimary:
		return "Primary"
	case RoleBackup:
		return "Backup"
	}
	return "Unknown"
}

type Configuration struct {
	Id         int
	Self       *Replica
	Replicas   map[int]Replica
	primary    bool
	QuorumSize int
	clients    map[string]Client
}

func (c *Configuration) IsPrimary() bool {
	return c.Self.IsPrimary()
}

func (c *Configuration) SetPrimary(id int, self bool) {
	if self {
		c.primary = true
		c.Self.SetPrimary()
	} else {
		c.primary = false
		c.Self.SetBackup()
	}
	for i, _  := range c.Replicas {
		if c.Replicas[i].Id == id {
			c.Replicas[i] = NewReplica(id, c.Replicas[i].port, RolePrimary)
		} else {
			c.Replicas[i] = NewReplica(c.Replicas[i].Id, c.Replicas[i].port, RoleBackup)
		}
	}
}

func (c *Configuration) GetClient(id string) Client {
	return c.clients[id]
}

type Replica struct {
	Id   int
	c    clientrpc.Client
	Role Role
	port int
}

func (r Replica) IsConnected() bool {
	conn, err := net.DialTimeout("tcp", r.c.Hostname, time.Second)
	log.Printf("Attempted to connect to :%v, err: %v", r.c.Hostname, err)
	if conn != nil {
		conn.Close()
		return true
	}
	return false
}

func (r *Replica) IsPrimary() bool {
	if r.Role == RolePrimary {
		return true
	}
	return false
}

func (r *Replica) SetPrimary() {
	r.Role = RolePrimary
	log.Printf("Changed Role for replica %d to Primary", r.Id)
}

func (r *Replica) SetBackup() {
	r.Role = RoleBackup
	log.Printf("Changed Role for replica %d to Backup", r.Id)
}

func NewReplica(id int, port int, role Role) Replica {
	return Replica{
		Id: id,
		c: clientrpc.Client{
			Hostname: fmt.Sprintf("localhost:%d", port),
		},
		port: port,
		Role: role,
	}
}

func(r *Replica) Do(api string, req clientrpc.Request, res clientrpc.Response, async bool) error {
	var err error
	if async {
		err = r.c.AsyncDo(api, req, res)
	} else {
		err = r.c.Do(api, req, res)
	}
	return err
}

func (r *Replica) GetPort() int {
	return r.port
}

type Client struct {
	Id   string
	port int
	c    clientrpc.Client
}

func NewClient(id string, port int) Client {
	return Client{
		Id:   id,
		port: port,
		c:    clientrpc.Client{
			Hostname: fmt.Sprintf("localhost:%d", port),
		},
	}
}

func(c *Client) Do(api string, req clientrpc.Request, res clientrpc.Response, async bool) {
	if async {
		c.c.AsyncDo(api, req, res)
	} else {
		c.c.Do(api, req, res)
	}
}
