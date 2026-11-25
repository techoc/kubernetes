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

package services

import (
	"encoding/json"
	"flag"
	"fmt"
)

// LogFileData 描述要通过 journalctl 命令或从节点文件系统获取的日志文件
type LogFileData struct {
	// 日志文件名
	Name string `json:"name"`
	// Files 是日志文件可能的绝对路径
	Files []string `json:"files"`
	// JournalctlCommand 是获取日志的 journalctl 命令
	JournalctlCommand []string `json:"journalctl"`
}

// logFiles 用于收集所有日志文件，key 是收集后日志文件的预期名称
type logFiles map[string]LogFileData

// String 实现 flag.Value 接口
func (l *logFiles) String() string {
	return fmt.Sprint(*l)
}

// Set 实现 flag.Value 的 Set 方法
func (l *logFiles) Set(value string) error {
	var log LogFileData
	if err := json.Unmarshal([]byte(value), &log); err != nil {
		return err
	}
	// 这里假设 flag 字符串中的所有空白都是字段分隔符
	logs := *l
	logs[log.Name] = log
	return nil
}

// extraLogs 是测试运行器指定的额外日志
var extraLogs = make(logFiles)

func init() {
	flag.Var(&extraLogs, "extra-log", "Extra log to collect after test in the json format of LogFile.")
}

// requiredLogs 是测试后必须收集的日志
var requiredLogs = []LogFileData{
	{
		Name:              "kern.log",
		Files:             []string{"/var/log/kern.log"},
		JournalctlCommand: []string{"-k"},
	},
	{
		Name:              "cloud-init.log",
		Files:             []string{"/var/log/cloud-init.log", "/var/log/cloud-init-output.log"},
		JournalctlCommand: []string{"-u", "cloud*"},
	},
	{
		Name:              "containerd.log",
		Files:             []string{"/var/log/containerd.log"},
		JournalctlCommand: []string{"-u", "containerd"},
	},
	{
		Name:              "containerd-installation.log",
		JournalctlCommand: []string{"-u", "containerd-installation"},
	},
}

// getLogFiles 获取测试后需要收集的所有日志
func getLogFiles() logFiles {
	logs := make(logFiles)
	for _, l := range requiredLogs {
		logs[l.Name] = l
	}
	for _, l := range extraLogs {
		logs[l.Name] = l
	}
	return logs
}
