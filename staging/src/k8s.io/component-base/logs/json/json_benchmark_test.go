/*
Copyright 2020 The Kubernetes Authors.

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

package json

import (
	"fmt"
	"testing"

	"go.uber.org/zap/zapcore"
)

// writer 是所有基准共用的丢弃型 write syncer，避免 I/O 干扰测量。
var writer = zapcore.AddSync(&writeSyncer{})

// BenchmarkInfoLoggerInfo 测量并行 Info 调用的吞吐，
// 键值覆盖字符串、整数、浮点、Unicode、结构体与未导出字段。
func BenchmarkInfoLoggerInfo(b *testing.B) {
	logger, _ := NewJSONLogger(0, writer, nil, nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			logger.Info("test",
				"str", "foo",
				"int64A", int64(1),
				"int64B", int64(1),
				"float64", float64(1.0),
				"string1", "\n",
				"string2", "💩",
				"string3", "🤔",
				"string4", "🙊",
				"bool", true,
				"request", struct {
					Method  string `json:"method"`
					Timeout int    `json:"timeout"`
					secret  string
				}{
					Method:  "GET",
					Timeout: 10,
					secret:  "pony",
				},
			)
		}
	})
}

// BenchmarkZapLoggerError 测量带 error 的并行 Error 调用开销。
func BenchmarkZapLoggerError(b *testing.B) {
	logger, _ := NewJSONLogger(0, writer, nil, nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			logger.Error(fmt.Errorf("test for error:%s", "default"),
				"test",
				"str", "foo",
				"int64A", int64(1),
				"int64B", int64(1),
				"float64", float64(1.0),
				"string1", "\n",
				"string2", "💩",
				"string3", "🤔",
				"string4", "🙊",
				"bool", true,
				"request", struct {
					Method  string `json:"method"`
					Timeout int    `json:"timeout"`
					secret  string
				}{
					Method:  "GET",
					Timeout: 10,
					secret:  "pony",
				},
			)
		}
	})
}

// BenchmarkZapLoggerV 测量 verbosity 检查（V(1)）通过后的 Info 路径。
func BenchmarkZapLoggerV(b *testing.B) {
	logger, _ := NewJSONLogger(1, writer, nil, nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			logger.V(1).Info("test",
				"str", "foo",
				"int64A", int64(1),
				"int64B", int64(1),
				"float64", float64(1.0),
				"string1", "\n",
				"string2", "💩",
				"string3", "🤔",
				"string4", "🙊",
				"bool", true,
				"request", struct {
					Method  string `json:"method"`
					Timeout int    `json:"timeout"`
					secret  string
				}{
					Method:  "GET",
					Timeout: 10,
					secret:  "pony",
				},
			)
		}
	})
}

// writeSyncer 丢弃写入内容，用于隔离日志序列化本身的性能。
type writeSyncer struct{}

var _ zapcore.WriteSyncer = (*writeSyncer)(nil)

func (w writeSyncer) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (w writeSyncer) Sync() error {
	return nil
}
