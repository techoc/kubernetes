/*
Copyright 2017 The Kubernetes Authors.

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

package cmd

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"runtime/trace"

	"github.com/spf13/pflag"
)

var (
	profileName   string
	profileOutput string
)

func addProfilingFlags(flags *pflag.FlagSet) {
	flags.StringVar(&profileName, "profile", "none", "Name of profile to capture. One of (none|cpu|heap|goroutine|threadcreate|block|mutex|trace)")
	flags.StringVar(&profileOutput, "profile-output", "profile.pprof", "Name of the file to write the profile to")
}

// initProfiling 初始化性能分析，并返回一个在退出时调用的函数，用于刷新和关闭。
func initProfiling() (func() error, error) {
	var (
		f   *os.File
		err error
	)
	switch profileName {
	case "none":
		return nil, nil
	case "cpu":
		f, err = os.Create(profileOutput)
		if err != nil {
			return nil, err
		}

		err = pprof.StartCPUProfile(f)
		if err != nil {
			f.Close() //nolint:errcheck
			return nil, err
		}
	// Block 和 mutex profile 需要调用 Set{Block,Mutex}ProfileRate 才能
	// 输出任何内容。我们选择对所有事件采样。
	case "block":
		runtime.SetBlockProfileRate(1)
	case "mutex":
		runtime.SetMutexProfileFraction(1)
	case "trace":
		f, err = os.Create(profileOutput)
		if err != nil {
			return nil, err
		}

		// 启用 CPU profiler。采样将记录在执行跟踪中。
		// 这与 pprof.StartCPUProfile 使用的采样率相同。
		runtime.SetCPUProfileRate(100)
		if err := trace.Start(f); err != nil {
			f.Close() //nolint:errcheck
			return nil, err
		}
	default:
		// 检查 profile 名称是否有效。
		if profile := pprof.Lookup(profileName); profile == nil {
			return nil, fmt.Errorf("unknown profile '%s'", profileName)
		}
	}

	// 如果命令在执行结束前被中断（ctrl-c），仍会刷新性能分析文件
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		flushProfiling(f) //nolint:errcheck
		os.Exit(0)
	}()

	return func() error {
		return flushProfiling(f)
	}, nil
}

func flushProfiling(output io.Closer) error {
	if output != nil {
		defer output.Close() //nolint:errcheck
	}

	switch profileName {
	case "none":
		return nil
	case "cpu":
		pprof.StopCPUProfile()
	case "trace":
		trace.Stop()
		runtime.SetCPUProfileRate(0)
	case "heap":
		runtime.GC()
		fallthrough
	default:
		profile := pprof.Lookup(profileName)
		if profile == nil {
			return nil
		}

		f, err := os.Create(profileOutput)
		if err != nil {
			return err
		}

		defer f.Close()
		profile.WriteTo(f, 0)
	}

	return nil
}
