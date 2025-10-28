package controllers

import (
	"github.com/auula/urnadb/server/services"
	"github.com/auula/urnadb/vfs"
)

var (
	ts services.TableService
	qs services.QueryService
	ls services.LockService
	hs *services.HealthService
)

func InitAllComponents(storage *vfs.LogStructuredFS) error {
	hs = services.NewHealthService(storage)
	ls = services.NewLockServiceImpl(storage)
	qs = services.NewQueryServiceImpl(storage)
	ts = services.NewTableLFSServiceImpl(storage)
	return nil
}
