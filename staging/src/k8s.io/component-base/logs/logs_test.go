/*
Copyright 2022 The Kubernetes Authors.

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

package logs_test

import (
	"os"
	"path"
	"testing"

	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	_ "k8s.io/component-base/logs/json/register"
	"k8s.io/klog/v2"
)

// TestGlogSetter 验证 GlogSetter 在 klog、json、text 三种格式下都能在运行时
// 调整 verbosity：初始级别 1 的日志被丢弃，调高后再写则出现输出。
func TestGlogSetter(t *testing.T) {
	testcases := map[string]struct {
		init func(t *testing.T)
		// write must write at verbosity level 1.
		write func()
	}{
		"klog": {
			init: func(t *testing.T) {},
			write: func() {
				klog.V(1).Info("hello")
			},
		},
		"json": {
			init: func(t *testing.T) {
				c := logsapi.NewLoggingConfiguration()
				c.Format = "json"
				if err := logsapi.ValidateAndApply(c, nil /* feature gates */); err != nil {
					t.Fatalf("Unexpected error enabling json output: %v", err)
				}
			},
			write: func() {
				klog.Background().V(1).Info("hello")
			},
		},
		"text": {
			init: func(t *testing.T) {
				c := logsapi.NewLoggingConfiguration()
				c.Format = "text"
				if err := logsapi.ValidateAndApply(c, nil /* feature gates */); err != nil {
					t.Fatalf("Unexpected error enabling text output: %v", err)
				}
			},
			write: func() {
				klog.Background().V(1).Info("hello")
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			// 保存并恢复 klog 全局状态，避免子测试之间互相污染。
			state := klog.CaptureState()
			t.Cleanup(state.Restore)
			tmpdir := t.TempDir()
			tmpfile := path.Join(tmpdir, "stderr.log")
			oldStderr := os.Stderr
			defer func() {
				os.Stderr = oldStderr
			}()
			// 把 stderr 重定向到临时文件，以便直接断言输出内容。
			newStderr, err := os.Create(tmpfile)
			if err != nil {
				t.Fatalf("Unexpected error creating temp file: %v", err)
			}
			os.Stderr = newStderr

			defer func() {
				if err := logsapi.ResetForTest(nil /* feature gates */); err != nil {
					t.Errorf("Unexpected error resetting the logging configuration: %v", err)
				}
			}()
			tc.init(t)

			// 默认 verbosity 为 0，级别 1 的日志不应产生任何输出。
			// First write with default verbosity level of 0 -> no output.
			tc.write()
			klog.Flush()
			out, err := os.ReadFile(tmpfile)
			if err != nil {
				t.Fatalf("Unexpected error reading temp file: %v", err)
			}
			if len(out) > 0 {
				t.Fatalf("Info message should have been discarded, got instead:\n%s", string(out))
			}

			// Increase verbosity at runtime.
			if _, err := logs.GlogSetter("1"); err != nil {
				t.Fatalf("Unexpected error setting verbosity level: %v", err)
			}

			// Now write again -> output.
			tc.write()
			klog.Flush()
			out, err = os.ReadFile(tmpfile)
			if err != nil {
				t.Fatalf("Unexpected error reading temp file: %v", err)
			}
			if len(out) == 0 {
				t.Fatal("Info message should have been written, got empty file instead.")
			}
		})
	}
}
