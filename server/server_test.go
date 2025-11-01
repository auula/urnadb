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
	"net"
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

		if err := server.Startup(); err != nil {
			assert.NoError(t, err)
		}
	}()

	// 等待服务器启动
	time.Sleep(500 * time.Millisecond)
	assert.NoError(t, err)

	// 关闭服务器

	if err := server.Shutdown(); err != nil {
		assert.NoError(t, err)
	}

	// 关闭也需要时间
	time.Sleep(500 * time.Millisecond)
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
	// 注意 err 作用于问题，可能会覆盖掉后面的 err 导致单元测试出现数据竞争问题。
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

	go func() {
		if err := hts.Startup(); err != nil {
			assert.NoError(t, err)
		}
	}()

	// 等待一小段时间让服务器开始启动
	time.Sleep(500 * time.Millisecond)

	if err := hts.Shutdown(); err != nil {
		assert.NoError(t, err)
	}

	// 关闭也需要一点时间
	time.Sleep(500 * time.Millisecond)
}

// 测试 getIPv4Address 函数
func TestGetIPv4Address_EmptyInterfaces(t *testing.T) {
	result, err := getIPv4Address([]net.Interface{})
	assert.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestGetIPv4Address_RealInterfaces(t *testing.T) {
	interfaces, _ := net.Interfaces()
	result, err := getIPv4Address(interfaces)
	assert.NoError(t, err)
	// 结果可能为空字符串
	if result != "" {
		ip := net.ParseIP(result)
		assert.NotNil(t, ip)
		assert.NotNil(t, ip.To4())
	}
}

// 测试 init 函数中的错误处理逻辑
func TestInitIPv4Logic(t *testing.T) {
	// 保存原始值
	originalIPv4 := ipv4
	defer func() {
		ipv4 = originalIPv4
	}()

	// 测试正常情况
	interfaces, err := net.Interfaces()
	if err == nil {
		result, err := getIPv4Address(interfaces)
		assert.NoError(t, err)
		// 验证结果是有效的 IP 地址或空字符串
		if result != "" {
			ip := net.ParseIP(result)
			assert.NotNil(t, ip)
		}
	}
}
