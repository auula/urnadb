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

package controller

import (
	"github.com/auula/urnadb/server/response"
	"github.com/auula/urnadb/server/service"
	"github.com/auula/urnadb/vfs"
)

var (
	ts service.TablesService
	qs service.QueryService
	ls service.LocksService
	rs service.RecordsService
	vs service.VariantsService
	hs *service.HealthService
)

var (
	miss_key = response.FailJSON("missing key in request path")
)

func InitAllComponents(storage *vfs.LogStructuredFS) error {
	hs = service.NewHealthService(storage)
	rs = service.NewRecordsService(storage)
	ls = service.NewLocksServiceImpl(storage)
	qs = service.NewQueryServiceImpl(storage)
	ts = service.NewTablesServiceImpl(storage)
	vs = service.NewVariantsServiceImpl(storage)
	return nil
}
