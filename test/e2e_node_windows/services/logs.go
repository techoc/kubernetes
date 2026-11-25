//go:build windows

/*
Copyright The Kubernetes Authors.

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

// LogFileData 保存日志文件的信息：通过 journalctl 命令获取，
// 或直接读取节点文件系统中的文件。
type LogFileData struct {
	Name              string   `json:"name"`
	Files             []string `json:"files"`
	JournalctlCommand []string `json:"journalctl"`
}

// logFiles 是用于收集所有日志文件的类型。
type logFiles map[string]LogFileData

// String 实现 flag.Value 的 String 方法。
func (l *logFiles) String() string {
	return fmt.Sprint(*l)
}

// Set 实现 flag.Value 的 Set 方法。
func (l *logFiles) Set(value string) error {
	var log LogFileData
	if err := json.Unmarshal([]byte(value), &log); err != nil {
		return err
	}
	logs := *l
	logs[log.Name] = log
	return nil
}

var extraLogs = make(logFiles)

func init() {
	flag.Var(&extraLogs, "extra-log", "Extra log to collect after test in the json format of LogFile.")
}

// getLogFiles 返回要收集的日志文件。Windows 既没有 journald，
// 也没有标准的 Linux 日志路径，因此只返回通过 flag 指定的额外日志。
func getLogFiles() logFiles {
	logs := make(logFiles)
	for _, l := range extraLogs {
		logs[l.Name] = l
	}
	return logs
}
