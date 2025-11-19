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

package clog

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/fatih/color"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	processName = "UrnaDB"
)

var (
	// Logger colors and log message prefixes
	warnColor   = color.New(color.Bold, color.FgYellow)
	infoColor   = color.New(color.Bold, color.FgGreen)
	redColor    = color.New(color.Bold, color.FgRed)
	debugColor  = color.New(color.Bold, color.FgBlue)
	errorPrefix = redColor.Sprintf("[ERRO] ")
	warnPrefix  = warnColor.Sprintf("[WARN] ")
	infoPrefix  = infoColor.Sprintf("[INFO] ")
	debugPrefix = debugColor.Sprintf("[DBUG] ")

	IsDebug = false
)

var (
	clog *log.Logger
	dlog *log.Logger
)

func init() {
	// 总共有两套日志记录器
	// [UrnaDB:C] 为主进程记录器记录正常运行状态日志信息
	// [UrnaDB:D] 为辅助记录器记录为 Debug 模式下的日志信息
	clog = newLogger(os.Stdout, "["+processName+":C] ", log.Ldate|log.Ltime)
	// [UrnaDB:D] 只能输出日志信息到标准输出中
	dlog = newLogger(os.Stdout, "["+processName+":D] ", log.Ldate|log.Ltime)
}

func newLogger(out io.Writer, prefix string, flag int) *log.Logger {
	return log.New(out, prefix, flag)
}

func multipleLogger(out io.Writer, prefix string, flag int) {
	clog = log.New(out, prefix, flag)
}

func SetOutput(path string) {
	// 正常模式的日志记录需要输出到控制台和日志文件中
	multipleLogger(io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename:   path, // 使用 lumberjack 设置日志轮转
		MaxSize:    10,   // 每个日志文件最大 10 MB
		MaxBackups: 3,    // 最多保留 3 个备份
		MaxAge:     7,    // 日志文件最多保留 7 天
		Compress:   true, // 启用压缩
	}), "["+processName+":C] ", log.Ldate|log.Ltime)
}

func Error(v ...interface{}) {
	clog.Output(2, errorPrefix+fmt.Sprint(v...))
}

func Errorf(format string, v ...interface{}) {
	clog.Output(2, errorPrefix+fmt.Sprintf(format, v...))
}

func Warn(v ...interface{}) {
	clog.Output(2, warnPrefix+fmt.Sprint(v...))
}

func Warnf(format string, v ...interface{}) {
	clog.Output(2, warnPrefix+fmt.Sprintf(format, v...))
}

func Info(v ...interface{}) {
	clog.Output(2, infoPrefix+fmt.Sprint(v...))
}

func Infof(format string, v ...interface{}) {
	clog.Output(2, infoPrefix+fmt.Sprintf(format, v...))
}

func Debug(v ...interface{}) {
	if IsDebug {
		pc, file, line, _ := runtime.Caller(1)
		fn := runtime.FuncForPC(pc)

		shortFn := filepath.Base(file) + ":" + strconv.Itoa(line)

		message := fmt.Sprintf("[%s::%s()] %s",
			shortFn,
			path.Base(fn.Name()),
			fmt.Sprint(v...),
		)

		dlog.Output(2, debugPrefix+message)
	}
}

func Debugf(format string, v ...interface{}) {
	if IsDebug {
		pc, file, line, _ := runtime.Caller(1)
		fn := runtime.FuncForPC(pc)

		shortFn := filepath.Base(file) + ":" + strconv.Itoa(line)

		message := fmt.Sprintf("[%s::%s()] %s",
			shortFn,
			path.Base(fn.Name()),
			fmt.Sprintf(format, v...),
		)

		dlog.Output(2, debugPrefix+message)
	}
}

func Failed(v ...interface{}) {
	// skip=1 表示跳过 fatalf 和 clog.Output 两层
	pc, file, line, _ := runtime.Caller(1)
	fn := runtime.FuncForPC(pc)

	shortFn := filepath.Base(file) + ":" + strconv.Itoa(line)

	message := fmt.Sprintf("[%s::%s()] %s",
		shortFn,
		path.Base(fn.Name()), // 只取函数名
		fmt.Sprint(v...),
	)

	// 让日志定位到实际调用者
	clog.Output(2, message)

	panic(message)
}

func Failedf(format string, v ...interface{}) {
	// skip=1 表示跳过 fatalf 和 clog.Output 两层
	pc, file, line, _ := runtime.Caller(1)
	fn := runtime.FuncForPC(pc)

	shortFn := filepath.Base(file) + ":" + strconv.Itoa(line)

	message := fmt.Sprintf("[%s::%s()] %s",
		shortFn,
		path.Base(fn.Name()), // 只取函数名
		fmt.Sprintf(format, v...),
	)

	// 让日志定位到实际调用者
	clog.Output(2, message)

	panic(message)
}
