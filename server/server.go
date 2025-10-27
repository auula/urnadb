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
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/auula/urnadb/clog"
	"github.com/auula/urnadb/server/controllers"
	"github.com/auula/urnadb/server/middleware"
	"github.com/auula/urnadb/server/routes"
	"github.com/auula/urnadb/vfs"
)

var (
	pkgmut = sync.Mutex{}
	// ipv4 return local IPv4 address
	ipv4    string = "127.0.0.1"
	storage *vfs.LogStructuredFS
)

type serverState int32

const (
	minPort = uint16(1024)
	maxPort = uint16(1<<16 - 1)
	timeout = time.Second * 3

	stateIdle serverState = iota
	stateRunning
	stateStopping
)

func init() {
	// Initialized local server ip address
	addrs, err := net.Interfaces()
	if err != nil {
		clog.Errorf("get server IPv4 address failed: %s", err)
	}

	for _, face := range addrs {
		adders, err := face.Addrs()
		if err != nil {
			clog.Errorf("get server IPv4 address failed: %s", err)
		}

		for _, addr := range adders {
			if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
				if ipNet.IP.To4() != nil {
					ipv4 = ipNet.IP.String()
					return
				}
			}
		}
	}
}

type HttpServer struct {
	serv  *http.Server
	port  uint16
	state atomic.Int32
}

type Options struct {
	Port uint16
	Auth string
	// CertMagic *tls.Config
}

func (opt *Options) Validated() error {
	if opt.Port < minPort || opt.Port > maxPort {
		return errors.New("HTTP server port illegal")
	}

	if len(opt.Auth) == 0 || len(opt.Auth) < 16 {
		return errors.New("HTTP server auth password illegal")
	}
	return nil
}

// New 创建一个新的 HTTP 服务器
func New(opt *Options) (*HttpServer, error) {
	// Validated 独立出来验证，尽量避免使用反射
	err := opt.Validated()
	if err != nil {
		return nil, err
	}

	pkgmut.Lock()
	middleware.SetAuthPassword(opt.Auth)
	pkgmut.Unlock()

	hs := HttpServer{
		serv: &http.Server{
			Handler:      routes.SetupRoutes(),
			Addr:         net.JoinHostPort("0.0.0.0", strconv.Itoa(int(opt.Port))),
			WriteTimeout: timeout,
			ReadTimeout:  timeout,
		},
		port: opt.Port,
	}

	hs.state.Store(int32(stateIdle))

	// 开启 HTTP Keep-Alive 长连接
	hs.serv.SetKeepAlivesEnabled(true)

	return &hs, nil
}

func (hs *HttpServer) SetupFS(fss *vfs.LogStructuredFS) {
	pkgmut.Lock()
	defer pkgmut.Unlock()
	storage = fss
	controllers.InitAllComponents(storage)
}

func (hs *HttpServer) SetAllowIP(allowd []string) {
	pkgmut.Lock()
	defer pkgmut.Unlock()
	middleware.SetAllowIpList(allowd)
}

func (hs *HttpServer) Port() uint16 {
	return hs.port
}

// IPv4 return local IPv4 address
func (hs *HttpServer) IPv4() string {
	return ipv4
}

// Startup blocking goroutine
func (hs *HttpServer) Startup() error {
	// 防止重复启动
	if !hs.state.CompareAndSwap(int32(stateIdle), int32(stateRunning)) {
		return fmt.Errorf("server already started and running")
	}

	// 检查文件存储系统是否已经初始化
	pkgmut.Lock()
	storageInitialized := storage != nil
	pkgmut.Unlock()

	if !storageInitialized {
		return errors.New("file storage system is not initialized")
	}

	// 这个函数是一个阻塞函数
	err := hs.serv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start http api server :%w", err)
	}

	return nil
}

func (hs *HttpServer) Shutdown() error {
	// 使用原子操作防止重复关闭，已经停止或者正在停止
	if !hs.state.CompareAndSwap(int32(stateRunning), int32(stateStopping)) {
		return nil
	}

	// 确保最后状态被重置
	defer hs.state.Store(int32(stateIdle))

	// 先关闭 http 服务器停止接受数据请求
	err := hs.serv.Shutdown(context.Background())
	if err != nil && err != http.ErrServerClosed {
		// 这里发生了错误，外层处理这个错误时也要关闭文件存储系统，
		// 理论上 hs.serv.RegisterOnShutdown 也能处理，但是 func() {} 不支持错误处理。
		inner := closeStorage()
		if inner != nil {
			return fmt.Errorf("failed to shutdown the server: %w", errors.Join(err, inner))
		}
		return err
	}

	return closeStorage()
}

func closeStorage() error {
	pkgmut.Lock()
	defer pkgmut.Unlock()
	if storage != nil {
		// 先停止垃圾回收线程和检查点生成线程
		storage.StopExpireLoop()
		storage.StopCheckpoint()
		storage.StopCompactRegion()
		err := storage.CloseFS()
		if err != nil {
			return err
		}
	}
	return nil
}
