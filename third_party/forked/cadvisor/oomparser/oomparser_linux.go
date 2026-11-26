//go:build linux

/*
Copyright 2014 Google Inc. All Rights Reserved.

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

// Package oomparser 从 github.com/google/cadvisor (utils/oomparser) 派生。
// 它读取内核环形缓冲区 (/dev/kmsg) 并提取 OOM kill 事件；本分支改用
// context.Context、通过 klog 输出日志，并使用仓库内的 go-kmsg-parser 分支读取内核日志。
package oomparser

import (
	"context"
	"path"
	"regexp"
	"strconv"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/third_party/forked/go-kmsg-parser/kmsgparser"
)

var (
	legacyContainerRegexp = regexp.MustCompile(`Task in (.*) killed as a result of limit of (.*)`)
	// 从 Linux 5.0 起，OOM 消息格式发生变化。
	containerRegexp = regexp.MustCompile(`oom-kill:constraint=(.*),nodemask=(.*),cpuset=(.*),mems_allowed=(.*),oom_memcg=(.*),task_memcg=(.*),task=(.*),pid=(.*),uid=(.*)`)
	lastLineRegexp  = regexp.MustCompile(`Killed process ([0-9]+) \((.+)\)`)
	firstLineRegexp = regexp.MustCompile(`invoked oom-killer:`)
)

// OomInstance 保存从一次内核 OOM kill 中提取的信息。
type OomInstance struct {
	// 被杀进程的 PID
	Pid int
	// 被杀进程的进程名
	ProcessName string
	// 报告进程被杀的时间，精确到分钟
	TimeOfDeath time.Time
	// 发生 OOM 的容器的绝对路径名
	ContainerName string
	// 因该 OOM 而被杀的容器的绝对路径名
	VictimContainerName string
	// 触发 OOM 的约束类型（CONSTRAINT_NONE、CONSTRAINT_CPUSET、
	// CONSTRAINT_MEMORY_POLICY、CONSTRAINT_MEMCG）
	Constraint string
}

func getLegacyContainerName(line string, o *OomInstance) {
	parsed := legacyContainerRegexp.FindStringSubmatch(line)
	if parsed == nil {
		return
	}
	o.ContainerName = path.Join("/", parsed[1])
	o.VictimContainerName = path.Join("/", parsed[2])
}

// getContainerName 从一行日志填充容器相关字段；遇到同时携带 PID 和任务名的新版
// oom-kill 行时返回 true。
func getContainerName(line string, o *OomInstance) (bool, error) {
	parsed := containerRegexp.FindStringSubmatch(line)
	if parsed == nil {
		// 回退到 5.0 之前的内核日志格式。
		getLegacyContainerName(line, o)
		return false, nil
	}
	o.ContainerName = parsed[6]
	o.VictimContainerName = parsed[5]
	o.Constraint = parsed[1]
	pid, err := strconv.Atoi(parsed[8])
	if err != nil {
		return false, err
	}
	o.Pid = pid
	o.ProcessName = parsed[7]
	return true, nil
}

// getProcessNamePid 从 "Killed process" 行提取 PID 和进程名。
func getProcessNamePid(line string, o *OomInstance) (bool, error) {
	parsed := lastLineRegexp.FindStringSubmatch(line)
	if parsed == nil {
		return false, nil
	}
	pid, err := strconv.Atoi(parsed[1])
	if err != nil {
		return false, err
	}
	o.Pid = pid
	o.ProcessName = parsed[2]
	return true, nil
}

func checkIfStartOfOomMessages(line string) bool {
	return firstLineRegexp.MatchString(line)
}

// OomParser 读取内核环形缓冲区 (/dev/kmsg) 并提取 OOM 事件。
type OomParser struct {
	parser kmsgparser.Parser
}

// New 创建一个基于内核日志 (/dev/kmsg) 的 OomParser。
func New() (*OomParser, error) {
	parser, err := kmsgparser.NewParser()
	if err != nil {
		return nil, err
	}
	return &OomParser{parser: parser}, nil
}

// StreamOoms 每发现一个内核日志中的 OOM 事件，就向 outStream 写入一个 OomInstance。
// 该方法会阻塞，应在 goroutine 中调用。
func (p *OomParser) StreamOoms(ctx context.Context, outStream chan<- *OomInstance) {
	logger := klog.FromContext(ctx)
	kmsgEntries := make(chan kmsgparser.Message, 32)
	go func() {
		defer func() { _ = p.parser.Close() }()
		if err := p.parser.Parse(kmsgEntries); err != nil {
			logger.Error(err, "Exiting kmsg parse; OOM events will not be reported")
		}
	}()

	for msg := range kmsgEntries {
		if !checkIfStartOfOomMessages(msg.Message) {
			continue
		}
		oom := &OomInstance{
			ContainerName:       "/",
			VictimContainerName: "/",
			TimeOfDeath:         msg.Timestamp,
		}
		for msg := range kmsgEntries {
			finished, err := getContainerName(msg.Message, oom)
			if err != nil {
				logger.Error(err, "Failed to parse OOM container name")
			}
			if !finished {
				finished, err = getProcessNamePid(msg.Message, oom)
				if err != nil {
					logger.Error(err, "Failed to parse OOM process name and pid")
				}
			}
			if finished {
				oom.TimeOfDeath = msg.Timestamp
				break
			}
		}
		outStream <- oom
	}
	logger.Error(nil, "Stopped receiving OOM notifications; OOM events will not be reported")
}
