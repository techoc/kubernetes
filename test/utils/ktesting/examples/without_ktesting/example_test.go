//go:build example

/*
Copyright 2023 The Kubernetes Authors.

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

package withoutktesting

// 下面的测试会失败，因此通过 "example" 构建标签将其排除在
// 常规的 "make test" 之外。要运行这些测试并查看输出，请使用
// "go test -tags example ."

import (
	"testing"
	"time"
)

func TestTimeout(t *testing.T) {
	tmp := t.TempDir()
	t.Logf("Using %q as temporary directory.", tmp)
	t.Cleanup(func() {
		t.Log("Cleaning up...")
	})
	// 这不会在短时间内完成……
	t.Log("Please kill me.")
	<-time.After(1000 * time.Hour)
}
