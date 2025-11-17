// Copyright 2022 Leon Ding <ding_ms@outlook.com> https://urnadb.github.io

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	vs services.VariantsService
	hs *services.HealthService
)

var (
	missKey = response.Fail("missing key in request path")
)

func InitAllComponents(storage *vfs.LogStructuredFS) error {
	hs = services.NewHealthService(storage)
	rs = services.NewRecordsService(storage)
	ls = services.NewLocksServiceImpl(storage)
	qs = services.NewQueryServiceImpl(storage)
	ts = services.NewTablesServiceImpl(storage)
	vs = services.NewVariantsServiceImpl(storage)
	return nil
}
