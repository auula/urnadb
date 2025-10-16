# 数据竞争修复记录

## 问题描述

在运行 `go test -race ./server` 时发现数据竞争错误：

```
WARNING: DATA RACE
Write at 0x000104542770 by goroutine 15:
  github.com/auula/urnadb/server.(*HttpServer).SetupFS()
      /Users/dings/go_workspace/src/urnadb/server/server.go:123

Previous read at 0x000104542770 by goroutine 14:
  github.com/auula/urnadb/server.closeStorage()
      /Users/dings/go_workspace/src/urnadb/server/server.go:200
```

## 根本原因

全局变量 `storage *vfs.LogStructuredFS` 在并发访问时缺乏同步保护：
- `SetupFS()` 方法有锁保护写操作
- `closeStorage()` 和 `Startup()` 方法读操作没有锁保护

## 修复方案

### 1. 修复 closeStorage() 函数

```go
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
```

### 2. 修复 Startup() 方法

```go
func (hs *HttpServer) Startup() error {
	// ... 其他代码 ...
	
	// 检查文件存储系统是否已经初始化
	pkgmut.Lock()
	storageInitialized := storage != nil
	pkgmut.Unlock()
	
	if !storageInitialized {
		return errors.New("file storage system is not initialized")
	}
	
	// ... 其他代码 ...
}
```

### 3. 修复服务器状态管理

```go
func (hs *HttpServer) Shutdown() error {
	// ... 其他代码 ...
	
	// 重置状态，允许再次启动
	hs.started.Store(false)
	hs.stopped.Store(false)  // 添加这行

	return nil
}
```

## 验证结果

修复后运行测试：
```bash
go test -race ./server -v
```

所有测试通过，无数据竞争警告。

### 稳定性测试

**1. 单模块测试**
运行 50 次连续测试验证修复稳定性：
```bash
for i in {1..50}; do go test -race ./server -v; done
```
结果：50/50 测试全部通过 ✓

**2. 全项目压力测试**
运行 5 分钟全项目测试（禁用缓存）：
```bash
go test -race -count=1 ./...
```
测试覆盖模块：
- ✅ clog (日志模块)
- ✅ conf (配置模块) 
- ✅ server (服务器模块)
- ✅ types (数据类型模块)
- ✅ utils (工具模块)
- ✅ vfs (虚拟文件系统模块)

**结果：28 轮完整测试全部通过，无任何数据竞争警告 ✓**

修复方案经过长时间压力测试验证，稳定性极佳。

## 修复日期

2025-10-14
