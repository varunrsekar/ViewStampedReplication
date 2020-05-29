package viewreplication

import (
	"viewStampedReplication/server/clientrpc"
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
	RoleUnknown
)

func (r Role) String() string {
	switch r {
	case RolePrimary:
		return "Primary"
	case RoleBackup:
		return "Replica"
	}
	return "Unknown"
}

type Configuration struct {
	Id int
	Self *Replica
	replicas map[int]*Replica
	primary bool
	QuorumSize int
	clients map[string]*Client
}

func (c *Configuration) IsPrimary() bool {
	return c.primary
}

func (c *Configuration) SetPrimary() {
	c.primary = true
	c.Self.SetPrimary()
}

func (c *Configuration) SetBackup() {
	c.primary = false
	c.Self.SetBackup()
}

func (c *Configuration) GetClient(id string) *Client {
	return c.clients[id]
}

type Replica struct {
	Id int
	c    clientrpc.Client
	role Role
	port int
}

func (r *Replica) IsPrimary() bool {
	if r.role == RolePrimary {
		return true
	}
	return false
}

func (r *Replica) SetPrimary() {
	r.role = RolePrimary
}

func (r *Replica) SetBackup() {
	r.role = RoleBackup
}

func NewReplica(id int, client clientrpc.Client, port int, role Role) *Replica {
	return &Replica{
		Id: id,
		c: client,
		port: port,
		role: role,
	}
}

func(r *Replica) Do(api string, req clientrpc.Request, res clientrpc.Response, async bool) {
	if async {
		r.c.AsyncDo(api, req, res)
	} else {
		r.c.Do(api, req, res)
	}
}

func (r *Replica) GetPort() int {
	return r.port
}

type Client struct {
	Id string
	port int
	c clientrpc.Client
}

func NewClient(id string, client clientrpc.Client, port int) *Client {
	return &Client{
		Id:   id,
		port: port,
		c:    client,
	}
}

func(c *Client) Do(api string, req clientrpc.Request, res clientrpc.Response, async bool) {
	if async {
		c.c.AsyncDo(api, req, res)
	} else {
		c.c.Do(api, req, res)
	}
}
