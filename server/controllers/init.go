package controllers

import (
	"github.com/auula/urnadb/server/response"
	"github.com/auula/urnadb/server/services"
	"github.com/auula/urnadb/vfs"
)

var (
	ts services.TablesService
	qs services.QueryService
	ls services.LockService
	hs *services.HealthService
)

var (
	missingKeyParam = response.Fail("missing key in request path")
)

func InitAllComponents(storage *vfs.LogStructuredFS) error {
	hs = services.NewHealthService(storage)
	ls = services.NewLockServiceImpl(storage)
	qs = services.NewQueryServiceImpl(storage)
	ts = services.NewTableLFSServiceImpl(storage)
	return nil
}
