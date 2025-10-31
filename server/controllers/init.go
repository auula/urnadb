package controllers

import (
	"github.com/auula/urnadb/server/response"
	"github.com/auula/urnadb/server/services"
	"github.com/auula/urnadb/vfs"
)

var (
	ts services.TablesService
	qs services.QueryService
	ls services.LocksService
	rs services.RecordsService
	vs services.VariantService
	hs *services.HealthService
)

var (
	missingKeyParam = response.Fail("missing key in request path")
)

func InitAllComponents(storage *vfs.LogStructuredFS) error {
	hs = services.NewHealthService(storage)
	rs = services.NewRecordsService(storage)
	ls = services.NewLocksServiceImpl(storage)
	qs = services.NewQueryServiceImpl(storage)
	ts = services.NewTablesServiceImpl(storage)
	vs = services.NewVariantServiceImpl(storage)
	return nil
}
