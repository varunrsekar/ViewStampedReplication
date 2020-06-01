package main

import (
	"viewStampedReplication/server/app"
	"viewStampedReplication/server/serviceconfig"
)

func main() {
	serviceconfig.Init()
	app.Init()
}
