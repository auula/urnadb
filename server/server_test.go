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

package server

import (
	"io/fs"
	"net/http"
	"testing"
	"time"

	"github.com/auula/urnadb/conf"
	"github.com/auula/urnadb/vfs"
	"github.com/stretchr/testify/assert"
)

// 测试 New 方法
func TestNewHttpServer(t *testing.T) {
	opt := &Options{Port: 8080, Auth: "secret1234567890"}
	server, err := New(opt)
	assert.NoError(t, err)
	assert.NotNil(t, server)
	assert.Equal(t, uint16(8080), server.Port())

	// 测试端口非法情况
	opt = &Options{Port: 80} // 端口小于 1024
	server, err = New(opt)
	assert.Error(t, err)
	assert.Nil(t, server)
}

// 测试 HttpServer 的 IPv4 方法
func TestHttpServer_IPv4(t *testing.T) {
	server, err := New(&Options{Port: 8080, Auth: "secret1234567890"})
	assert.NoError(t, err)
	assert.NotEmpty(t, server.IPv4())
}

// 测试 HttpServer 的 Port 方法
func TestHttpServer_Port(t *testing.T) {
	server, err := New(&Options{Port: 8080, Auth: "secret1234567890"})
	assert.NoError(t, err)
	assert.Equal(t, uint16(8080), server.Port())
}

// 测试 Startup 方法（非阻塞）
func TestHttpServer_Startup(t *testing.T) {
	conf.Settings.Path = "./_temp/"
	server, err := New(&Options{Port: 8081, Auth: "secret1234567890"})
	assert.NoError(t, err)

	// 启动服务器（在 goroutine 中运行）
	go func() {
		fss, err := vfs.OpenFS(&vfs.Options{
			FSPerm:    fs.FileMode(0755),
			Path:      conf.Settings.Path,
			Threshold: 3,
		})
		assert.NoError(t, err)

		server.SetupFS(fss)
		err = server.Startup()
		assert.NoError(t, err)
	}()

	// 等待服务器启动
	time.Sleep(500 * time.Millisecond)
	assert.NoError(t, err)

	// 关闭服务器
	err = server.Shutdown()
	assert.NoError(t, err)
}

// 测试 SetupFS 方法
func TestHttpServer_SetupFS(t *testing.T) {
	hts, err := New(&Options{
		Port: 6379,
		Auth: "secret1234567890",
	})
	if err != nil {
		assert.NoError(t, err)
	}

	assert.NotNil(t, hts)

	fss, err := vfs.OpenFS(&vfs.Options{
		FSPerm:    fs.FileMode(0755),
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		assert.NoError(t, err)
	}

	assert.NotNil(t, fss)

	if err != nil {
		assert.NoError(t, err)
	}

	hts.SetupFS(fss)
}

// 测试 Shutdown 方法
func TestHttpServer_Shutdown(t *testing.T) {
	hts, err := New(&Options{
		Port: 6379,
		Auth: "secret1234567890",
	})
	if err != nil {
		assert.NoError(t, err)
	}

	assert.NotNil(t, hts)

	fss, err := vfs.OpenFS(&vfs.Options{
		FSPerm:    fs.FileMode(0755),
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})

	if err != nil {
		assert.NoError(t, err)
	}

	hts.SetupFS(fss)

	// 使用 channel 来同步启动状态
	started := make(chan error, 1)
	go func() {
		started <- hts.Startup()
	}()

	// 等待一小段时间让服务器开始启动
	time.Sleep(100 * time.Millisecond)

	err = hts.Shutdown()
	assert.NoError(t, err)

	// 等待 Startup goroutine 完成
	select {
	case startupErr := <-started:
		// Startup 应该因为 Shutdown 而返回，这是正常的
		if startupErr != nil && startupErr != http.ErrServerClosed {
			t.Logf("Startup returned: %v", startupErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Startup goroutine did not complete in time")
	}
}
