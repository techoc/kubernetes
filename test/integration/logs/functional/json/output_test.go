/*
Copyright 2021 The Kubernetes Authors.

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
	"io"
	"os"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"

	logsapi "k8s.io/component-base/logs/api/v1"
	logsjson "k8s.io/component-base/logs/json"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/test"
)

func init() {
	// hack/make-rules/test-integration.sh 期望所有单元测试
	// 都支持 -v 和 -vmodule。
	klog.InitFlags(nil)
}

// TestJsonOutput 测试 JSON logger，包括直接使用以及作为 klog 后端使用。
func TestJSONOutput(t *testing.T) {
	test.InitKlog(t)
	newLogger := func(out io.Writer, v int, vmodule string) logr.Logger {
		logger, _ := logsjson.NewJSONLogger(logsapi.VerbosityLevel(v), logsjson.AddNopSync(out), nil,
			&zapcore.EncoderConfig{
				MessageKey:     "msg",
				CallerKey:      "caller",
				NameKey:        "logger",
				EncodeDuration: zapcore.StringDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			})
		return logger
	}

	// 如果关闭了 Go modules（例如执行 "make test-integration" 时），
	// 对 klog 的引用（如 k8s.io/klog/v2.ObjectRef.MarshalLog）会变成
	// k8s.io/kubernetes/vendor/k8s.io/klog/v2.ObjectRef.MarshalLog。
	injectVendor := func(mapping map[string]string) map[string]string {
		if os.Getenv("GO111MODULE") != "off" {
			return mapping
		}
		for key, value := range mapping {
			mapping[key] = strings.ReplaceAll(value, "k8s.io/klog/v2", "k8s.io/kubernetes/vendor/k8s.io/klog/v2")
		}
		return mapping
	}

	t.Run("direct", func(t *testing.T) {
		test.Output(t, test.OutputConfig{
			NewLogger:             newLogger,
			ExpectedOutputMapping: injectVendor(test.ZaprOutputMappingDirect()),
		})
	})

	t.Run("klog-backend", func(t *testing.T) {
		test.Output(t, test.OutputConfig{
			NewLogger:             newLogger,
			AsBackend:             true,
			ExpectedOutputMapping: injectVendor(test.ZaprOutputMappingIndirect()),
		})
	})
}
