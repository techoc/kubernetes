/*
Copyright 2019 The Kubernetes Authors.

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

package metrics

import (
	"fmt"
	"testing"

	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestMetricCollection 验证 totalPluginsCollector 能按期望状态与实际状态分别统计插件数量。
func TestMetricCollection(t *testing.T) {
	tCtx := ktesting.Init(t)
	dsw := cache.NewDesiredStateOfWorld()
	asw := cache.NewActualStateOfWorld()
	fakePlugin := cache.PluginInfo{
		SocketPath: fmt.Sprintf("fake/path/plugin.sock"),
	}
	// 向期望状态中添加一个插件。
	err := dsw.AddOrUpdatePlugin(tCtx, fakePlugin.SocketPath)
	if err != nil {
		t.Fatalf("AddOrUpdatePlugin failed. Expected: <no error> Actual: <%v>", err)
	}

	// 向实际状态中添加一个插件。
	err = asw.AddPlugin(tCtx, fakePlugin)
	if err != nil {
		t.Fatalf("AddOrUpdatePlugin failed. Expected: <no error> Actual: <%v>", err)
	}

	metricCollector := &totalPluginsCollector{asw: asw, dsw: dsw}

	// 检查 getPluginCount 返回的数据是否正确。
	count := metricCollector.getPluginCount()
	if len(count) != 2 {
		t.Errorf("getPluginCount failed. Expected <2> states, got <%d>", len(count))
	}

	dswCount, ok := count["desired_state_of_world"]
	if !ok {
		t.Errorf("getPluginCount failed. Expected <desired_state_of_world>, got nothing")
	}

	fakePluginCount := dswCount["fake/path/plugin.sock"]
	if fakePluginCount != 1 {
		t.Errorf("getPluginCount failed. Expected <1> fake/path/plugin.sock in DesiredStateOfWorld, got <%d>",
			fakePluginCount)
	}

	aswCount, ok := count["actual_state_of_world"]
	if !ok {
		t.Errorf("getPluginCount failed. Expected <actual_state_of_world>, got nothing")
	}

	fakePluginCount = aswCount["fake/path/plugin.sock"]
	if fakePluginCount != 1 {
		t.Errorf("getPluginCount failed. Expected <1> fake/path/plugin.sock in ActualStateOfWorld, got <%d>",
			fakePluginCount)
	}
}
