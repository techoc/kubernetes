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

package volume_test

import (
	"os"
	"path/filepath"
	"testing"

	utiltesting "k8s.io/client-go/util/testing"
	. "k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
)

// TestMetricsDuGetCapacity 验证 MetricsDu 能读取路径的磁盘用量。
func TestMetricsDuGetCapacity(t *testing.T) {
	tmpDir, err := utiltesting.MkTmpdir("metrics_du_test")
	if err != nil {
		t.Fatalf("Can't make a tmp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	metrics := NewMetricsDu(tmpDir)

	expectedEmptyDirUsage, err := volumetest.FindEmptyDirectoryUsageOnTmpfs()
	if err != nil {
		t.Errorf("Unexpected error finding expected empty directory usage on tmpfs: %v", err)
	}

	actual, err := metrics.GetMetrics()
	if err != nil {
		t.Errorf("Unexpected error when calling GetMetrics %v", err)
	}
	if e, a := expectedEmptyDirUsage.Value(), actual.Used.Value(); e != a {
		t.Errorf("Unexpected value for empty directory; expected %v, got %v", e, a)
	}

	// TODO(pwittroc): 想办法校验这些值的正确性，或许可通过格式化并挂载文件为文件系统来实现。
	if a := actual.Capacity.Value(); a <= 0 {
		t.Errorf("Expected Capacity %d to be greater than 0.", a)
	}
	if a := actual.Available.Value(); a <= 0 {
		t.Errorf("Expected Available %d to be greater than 0.", a)
	}

	// 写入文件后 Used 应随之增加。
	os.WriteFile(filepath.Join(tmpDir, "f1"), []byte("Hello World"), os.ModeTemporary)
	actual, err = metrics.GetMetrics()
	if err != nil {
		t.Errorf("Unexpected error when calling GetMetrics %v", err)
	}
	if e, a := (expectedEmptyDirUsage.Value() + getExpectedBlockSize(filepath.Join(tmpDir, "f1"))), actual.Used.Value(); e != a {
		t.Errorf("Unexpected Used for directory with file.  Expected %v, got %d.", e, a)
	}

	// 创建硬链接不应增加 inode 使用量。
	previousInodes := actual.InodesUsed.Value()
	err = os.Link(filepath.Join(tmpDir, "f1"), filepath.Join(tmpDir, "f2"))
	if err != nil {
		t.Errorf("Unexpected error when creating hard link %v", err)
	}
	actual, err = metrics.GetMetrics()
	if err != nil {
		t.Errorf("Unexpected error when calling GetMetrics %v", err)
	}
	if e, a := previousInodes, actual.InodesUsed.Value(); e != a {
		t.Errorf("Unexpected Used for directory with file.  Expected %v, got %d.", e, a)
	}

}

// TestMetricsDuRequirePath 验证未指定路径的 MetricsDu 调用 GetMetrics 会报错。
func TestMetricsDuRequirePath(t *testing.T) {
	metrics := NewMetricsDu("")
	actual, err := metrics.GetMetrics()
	expected := &Metrics{}
	if !volumetest.MetricsEqualIgnoreTimestamp(actual, expected) {
		t.Errorf("Expected empty Metrics from uninitialized MetricsDu, actual %v", *actual)
	}
	if err == nil {
		t.Errorf("Expected error when calling GetMetrics on uninitialized MetricsDu, actual nil")
	}
}

// TestMetricsDuRequireRealDirectory 验证路径不存在时 GetMetrics 返回错误。
func TestMetricsDuRequireRealDirectory(t *testing.T) {
	metrics := NewMetricsDu("/not/a/real/directory")
	actual, err := metrics.GetMetrics()
	expected := &Metrics{}
	if !volumetest.MetricsEqualIgnoreTimestamp(actual, expected) {
		t.Errorf("Expected empty Metrics from incorrectly initialized MetricsDu, actual %v", *actual)
	}
	if err == nil {
		t.Errorf("Expected error when calling GetMetrics on incorrectly initialized MetricsDu, actual nil")
	}
}
