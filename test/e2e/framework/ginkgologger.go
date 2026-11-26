/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package framework 提供与 provider 无关的辅助代码，用于
// 通过 Ginkgo 构建和运行 E2E 测试。实际的 Ginkgo 测试
// 套件由本框架、可选的 provider 支持代码和具体测试共同组装，
// 组装方式见单独的 .go 文件，
// 例如 Kubernetes 的 test/e2e.go。
package framework

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	ginkgotypes "github.com/onsi/ginkgo/v2/types"

	"k8s.io/klog/v2"
	"k8s.io/klog/v2/textlogger"

	_ "k8s.io/component-base/logs/testinit" // 确保命令行 flag 已注册。
)

var (
	logConfig = textlogger.NewConfig(
		textlogger.Output(ginkgo.GinkgoWriter),
		textlogger.Backtrace(unwind),
	)
	ginkgoLogger = textlogger.NewLogger(logConfig)
	TimeNow      = time.Now    // 可在测试中打桩替换。
	Pid          = os.Getpid() // 可在测试中打桩替换。
)

func init() {
	// ktesting 和 testinit 已经注册了 -v 和 -vmodule
	// 命令行 flag。为一致地配置 textlogger 和 klog，
	// 需要拦截 Set 调用。做法是给 -v 和
	// -vmodule 的 flag.Value 换上同时调用两者的
	// 包装器。
	var fs flag.FlagSet
	logConfig.AddFlags(&fs)
	fs.VisitAll(func(loggerFlag *flag.Flag) {
		klogFlag := flag.CommandLine.Lookup(loggerFlag.Name)
		if klogFlag != nil {
			klogFlag.Value = &valueChain{Value: loggerFlag.Value, parentValue: klogFlag.Value}
		}
	})

	// 现在把 textlogger 安装为 klog 的默认 logger。
	// 此后 klog.Info 之类的调用会通过 textlogger 写入 ginkgo.GingoWriter
	//（原文如此）。
	//
	// 但栈回溯仍由 klog 完成，因此会忽略 ginkgo.GingkoHelper。
	// 测试应使用 framework.Logf 或结构化、上下文日志。
	// 测试应使用 framework.Logf 或结构化、上下文日志。
	writer, _ := ginkgoLogger.GetSink().(textlogger.KlogBufferWriter)
	opts := []klog.LoggerOption{
		klog.ContextualLogger(true),
		klog.WriteKlogBuffer(writer.WriteKlogBuffer),
	}
	klog.SetLoggerWithOptions(ginkgoLogger, opts...)
}

type valueChain struct {
	flag.Value
	parentValue flag.Value
}

func (v *valueChain) Set(value string) error {
	if err := v.Value.Set(value); err != nil {
		return err
	}
	if err := v.parentValue.Set(value); err != nil {
		return err
	}
	return nil
}

func unwind(skip int) (string, int) {
	location := ginkgotypes.NewCodeLocation(skip + 1)
	return location.FileName, location.LineNumber
}

// log 重新实现 klog.Info：头部相同，但栈回溯
// 支持 ginkgo.GinkgoWriter 并可跳过指定栈层级。
func log(offset int, msg string) {
	now := TimeNow()
	file, line := unwind(offset + 1)
	if file == "" {
		file = "???"
		line = 1
	} else if slash := strings.LastIndex(file, "/"); slash >= 0 {
		file = file[slash+1:]
	}
	_, month, day := now.Date()
	hour, minute, second := now.Clock()
	header := fmt.Sprintf("I%02d%02d %02d:%02d:%02d.%06d %7d %s:%d]",
		month, day, hour, minute, second, now.Nanosecond()/1000, Pid, file, line)

	fmt.Fprintln(ginkgo.GinkgoWriter, header, msg)
}
