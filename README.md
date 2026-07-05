
<div align="center">
    <!-- <img src="cmd/urnadb.png" style="width: 86px; height: auto; display: inline-block;"> -->
    <h1>🏺️</h1>
</div>

<p align="center">UrnaDB is a NoSQL database support diverse data types and transactions.</p>


---


[![Go Report Card](https://img.shields.io/badge/go%20report-A+-brightgreen.svg?style=flat)](https://img.shields.io/badge/go%20report-A+-brightgreen.svg?style=flat)
[![Go Reference](https://pkg.go.dev/badge/github.com/auula/urnadb.svg)](https://pkg.go.dev/github.com/auula/urnadb)
[![codecov](https://codecov.io/gh/auula/urnadb/graph/badge.svg?token=xTcPzdLFkJ)](https://codecov.io/gh/auula/urnadb)
[![DeepSource](https://app.deepsource.com/gh/auula/urnadb.svg/?label=active+issues&show_trend=false&token=Bymkt0xcKcmlGI8WOo3JMxHc)](https://app.deepsource.com/gh/auula/urnadb/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![release](https://img.shields.io/github/release/auula/urnadb.svg)](https://github.com/auula/urnadb/releases)
![Stars](https://img.shields.io/github/stars/auula/urnadb)



---

[简体中文](README.md) | [English](README_EN.md)

---

## 🎉 Feature

- 支持多种内置的数据结构
- 高吞吐量、低延迟、高效批量数据写入
- 支持磁盘数据存储和磁盘垃圾数据回收
- 支持磁盘数据静态加密和静态数据压缩
- 支持 IP 白名单功能保障数据的安全访问
- 支持通过基于 RESTful API 协议操作数据

---

## 🚀 Quick Start

使用 Docker 可以快速部署 [`urnadb:latest`](https://hub.docker.com/r/auula/urnadb) 的镜像来测试 UrnaDB 提供的服务，运行以下命令即可拉取 UrnaDB 镜像：

```bash
docker pull auula/urnadb:latest
```

运行 UrnaDB 镜像启动容器服务，并且映射端口到外部主机网络，执行下面的命令：

```bash
docker run -p 2668:2668 auula/urnadb:latest
```

UrnaDB 提供使用 RESTful API 的方式进行数据交互，理论上任意具备 HTTP 协议的客户端都支持访问和操作 UrnaDB 服务实例。在调用 RESTful API 时需要在请求头中添加 `Auth-Token` 进行鉴权，该密钥由 UrnaDB 进程自动生成，可通过容器运行时日志获取，使用以下命令查看启动日志：

```text
root@2c2m:~# docker logs 66ae91bc73a6
              __  __              ___  ___
             / / / /______  ___ _/ _ \/ _ )
            / /_/ / __/ _ \/ _ `/ // / _  |
            \____/_/ /_//_/\_,_/____/____/  v1.1.2

  UrnaDB is a NoSQL database based on Log-structured file system.
  Software License: Apache 2.0  Website: https://urnadb.github.io

[UrnaDB:C] 2023/06/04 18:35:15 [WARN] The default password is: QGVkh8niwL2TSkj72icaKBC9B
[UrnaDB:C] 2023/06/04 18:35:15 [INFO] Logging output initialized successfully
[UrnaDB:C] 2023/06/04 18:35:15 [INFO] Loading and parsing region data files...
[UrnaDB:C] 2023/06/04 18:35:15 [INFO] Regions compression activated successfully
[UrnaDB:C] 2023/06/04 18:35:15 [INFO] File system setup completed successfully
[UrnaDB:C] 2023/06/04 18:35:15 [INFO] HTTP server started at http://192.168.31.221:2668 🚀
```

如果计划将 UrnaDB 作为长期运行的服务，推荐直接使用主流 Linux 发行版来运行而非容器技术。采用裸机 Linux 部署 UrnaDB 服务，可手动优化存储引擎参数，以获得更稳定的性能和更高的资源利用率，具体参数配置建议查看[官方文档](https://urnadb.github.io)。

---

## 🕹️ RESTful API 

目前 UrnaDB 服务对外提供数据交互接口是基于 HTTP 协议的 RESTful API ，只需要通过支持  `HTTP` 协议客户端软件就可以进行数据操作。这里推荐使用 `curl` 软件进行数据交互操作，UrnaDB 内部提供了多种数据结构抽象，例如 Table 、Record 、 Variant 、Lock 类型。这些数据类型对应着常见的业务代码所需使用的数据结构，这里以 Tables 类型结构为例进行 RESTful API 数据交互的演示。


Table 结构类似关系数据库 SQL 中的一张表结构，一旦创建完成之后 Table 会为每条记录分配一个全局唯一递增的 `t_id` 索引，Table 的每列可以存储任何有映射关系的半结构化数据，例如编程语言中的 struct 和 class 字段都可以使用 Table 进行存储，下面是一张 Table 结构 Rows 的 JSON 抽象，其中的 `t_id` 为数据库主动分配的自增主键：

```json
{
    "table": {
        "1": {
            "active": true,
            "age": 25,
            "name": "Leon Ding",
            "score": 95.5,
            "tags": [
                "admin",
                "user"
            ]
        },
        "2": {
            "active": false,
            "age": 30,
            "name": "Alice Wang",
            "config": {
                "font": 14,
                "theme": "dark"
            },
            "email": "example@xxx.com"
        },
        "3": {}
    },
    "t_id": 4
}
```

下面是 curl 进行数据存储操作的例子，由于是 RESTful API 设计风格，需要在 HTTP 的请求路径 URL 加上数据类型信息。如何数类型新建操作，都需要使用 HTTP 协议的 PUT 方法进行操作，命令如下：

```bash
curl -X PUT http://localhost:2668/table/key-01 -v \
     -H "Content-Type: application/json" \
     -H "Auth-Token: QGVkh8niwL2TSkj72icaKBC9B" \
     --data @tests/table.json
```

获取数据的方式只需要将 HTTP 的请求改为 GET 方式就会获取得 Key 相应的存储记录，命令如下：

```bash
curl -X GET http://localhost:2668/table/key-01 -v \
-H "Auth-Token: QGVkh8niwL2TSkj72icaKBC9B" 
```

删除对应的数据记录，只需要将 HTTP 的请求改为 DELETE 的方式即可，命令如下：

```bash
curl -X DELETE http://localhost:2668/table/key-01 -v \
-H "Auth-Token: QGVkh8niwL2TSkj72icaKBC9B" 
```

更多针对数据类型复杂的操作使用示例请查看[官方文档](https://urnadb.github.io)。

---

## 📦 Java SDK 使用

相较于基于 HTTP 协议的数据交互方式，官方更推荐使用 SDK 来进行操作的，例如使用 Java 语言的 SDK 可以使用 Lambda 函数式编程风格进行数据交互操作。下面是创建一张名为 `users` 表并且向 `users` 表中插入一条数据记录，这条操作类似于关系数据库 SQL 中的 `INSERT ... INTO ... VALUES ...` 语句，代码如下：

```java
// 第二参数支持设置 120 秒
var isOk = sdk.createTable("users",120);
// tables 支持重载，put 还可以直接插入一个 Object 对象实例。
var tid = sdk.tables("users").put(p -> {
    p.set("name", "Leon")
     .set("age", 18)
     .set("config", Map.of("theme", "dark", "font", 14))
     .set("meta", new Meta("author", "leon"))
     .set("extra", m -> {
         m.put("role", "admin");
         m.put("verified", true);
     });
}).commit();
```

根据条件对 Table 中已有数据记录进行部分字段数据内容更新的操作，这个操作类似于关系数据库 SQL 中的 `UPDATE ... SET ... WHERE ...` 语句，代码示例如下：

```java
var count = sdk.tables("users").patch(p -> {
    p.where(w -> w.eq("name","Leon"));
    p.sets(s -> s
        .set("age", 25)
        .set("active", true)
        .set("meta", m -> {
            m.put("editor", "system");
        })
    );
}).commit();
```

对 Table 中已有的数据记录，执行条件删除操作，类似于关系数据库 SQL 中的 `DELETE FROM ... WHERE ...` 的语句，代码如下：

```java
var count = sdk.tables("users").remove(r -> 
    r.where(w -> w.tid(4))
).commit();
```

对 Table 中已有的数据记录，执行条件查询操作，类似于关系数据库 SQL 中的 `SELECT * FROM ... WHERE ...` 的语句，代码如下：

```java
var user = sdk.tables("users").query(q -> 
    q.where(w -> w.eq("age", 18))
).commit();
```

相关的 SDK 的 API 使用示例请查看[官方文档](https://urnadb.github.io)。

---

## 🧪 Benchmark Test

由于底层存储引擎是以 Append-Only Log 的方式将所有的操作写入到数据文件中，所以这里给出的测试用例报告，是针对的其核心文件系统 [`vfs`](./vfs/) 包的写入性能测试的结果。运行测试代码的硬件设备配置信息为（Intel i5-7360U, 8GB LPDDR3 RAM），写入基准测试结果如下：

```bash
$: go test -benchmem -run=^$ -bench ^BenchmarkVFSWrite$ github.com/auula/urnadb/vfs
goos: darwin
goarch: amd64
pkg: github.com/auula/urnadb/vfs
cpu: Intel(R) Core(TM) i5-7360U CPU @ 2.30GHz
BenchmarkVFSWrite-4   	  173533	      7283 ns/op	     774 B/op	      20 allocs/op
PASS
ok  	github.com/auula/urnadb/vfs	2.806s
```

在项目根目录下有一个 [`tools.sh`](./tools.sh) 的工具脚本文件，可以快速帮助完成各项辅助工作。

---

## 🌟 Stargazers over time

[![Stargazers over time](https://starchart.cc/auula/urnadb.svg?variant=adaptive)](https://starchart.cc/auula/urnadb)

---

## 👬 Contributors

🤝 Thanks to all the contributors below! 

![Contributors](https://contributors-img.web.app/image?repo=auula/urnadb)




