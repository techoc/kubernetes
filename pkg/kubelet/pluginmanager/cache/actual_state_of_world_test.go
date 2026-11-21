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

package cache

import (
	goruntime "runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// Test_ASW_AddPlugin_Positive_NewPlugin 验证新增插件：
// 调用 AddPlugin 后，插件应出现在 GetRegisteredPlugins 中；
// PluginExistsWithCorrectUUID 返回 true；
// PluginExistsWithCorrectTimestamp 返回 true（Windows 除外）。
func Test_ASW_AddPlugin_Positive_NewPlugin(t *testing.T) {
	tCtx := ktesting.Init(t)
	pluginInfo := PluginInfo{
		SocketPath: "/var/lib/kubelet/device-plugins/test-plugin.sock",
		Timestamp:  time.Now(),
		UUID:       uuid.NewUUID(),
		Handler:    nil,
		Name:       "test",
	}
	// 每个用例使用独立的实际状态缓存。
	asw := NewActualStateOfWorld()
	err := asw.AddPlugin(tCtx, pluginInfo)
	// 断言 AddPlugin 没有返回错误。
	if err != nil {
		t.Fatalf("AddPlugin failed. Expected: <no error> Actual: <%v>", err)
	}

	// 获取已注册插件并确认新插件在其中。
	aswPlugins := asw.GetRegisteredPlugins()
	if len(aswPlugins) != 1 {
		t.Fatalf("Actual state of world length should be one but it's %d", len(aswPlugins))
	}
	if aswPlugins[0] != pluginInfo {
		t.Fatalf("Expected\n%v\nin actual state of world, but got\n%v\n", pluginInfo, aswPlugins[0])
	}

	// 校验 UUID 匹配检查返回 true。
	if !asw.PluginExistsWithCorrectUUID(pluginInfo) {
		t.Fatalf("PluginExistsWithCorrectUUID returns false for plugin that should be registered")
	}

	// 在 Windows 上跳过时间戳检查：Windows 时间精度较低，
	// 连续两次 time.Now() 可能返回相同时间。
	if goruntime.GOOS != "windows" && !asw.PluginExistsWithCorrectTimestamp(pluginInfo) {
		t.Fatalf("PluginExistsWithCorrectTimestamp returns false for plugin that should be registered")
	}
}

// Test_ASW_AddPlugin_Negative_EmptySocketPath 验证 socket 路径为空时：
// AddPlugin 返回错误，插件不会出现在 GetRegisteredPlugins 中，
// PluginExistsWithCorrectUUID 与 PluginExistsWithCorrectTimestamp 均返回 false（Windows 除外）。
func Test_ASW_AddPlugin_Negative_EmptySocketPath(t *testing.T) {
	tCtx := ktesting.Init(t)
	asw := NewActualStateOfWorld()
	pluginInfo := PluginInfo{
		SocketPath: "",
		Timestamp:  time.Now(),
		UUID:       uuid.NewUUID(),
		Handler:    nil,
		Name:       "test",
	}
	err := asw.AddPlugin(tCtx, pluginInfo)
	// 错误信息应明确指出 socket 路径为空。
	require.EqualError(t, err, "socket path is empty")

	// 获取已注册插件并确认列表为空。
	aswPlugins := asw.GetRegisteredPlugins()
	if len(aswPlugins) != 0 {
		t.Fatalf("Actual state of world length should be zero but it's %d", len(aswPlugins))
	}

	// 校验 UUID 匹配检查返回 false。
	if asw.PluginExistsWithCorrectUUID(pluginInfo) {
		t.Fatalf("PluginExistsWithCorrectUUID returns true for plugin that's not registered")
	}

	// 校验时间戳匹配检查返回 false。
	if goruntime.GOOS != "windows" && asw.PluginExistsWithCorrectTimestamp(pluginInfo) {
		t.Fatalf("PluginExistsWithCorrectTimestamp returns true for plugin that's not registered")
	}
}

// Test_ASW_RemovePlugin_Positive 验证删除插件：
// 删除后插件不再出现在 GetRegisteredPlugins 中，
// PluginExistsWithCorrectUUID 与 PluginExistsWithCorrectTimestamp 均返回 false（Windows 除外）。
func Test_ASW_RemovePlugin_Positive(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 先添加一个插件。
	asw := NewActualStateOfWorld()
	pluginInfo := PluginInfo{
		SocketPath: "/var/lib/kubelet/device-plugins/test-plugin.sock",
		Timestamp:  time.Now(),
		UUID:       uuid.NewUUID(),
		Handler:    nil,
		Name:       "test",
	}
	err := asw.AddPlugin(tCtx, pluginInfo)
	// Assert
	if err != nil {
		t.Fatalf("AddPlugin failed. Expected: <no error> Actual: <%v>", err)
	}

	// 尝试删除该插件。
	asw.RemovePlugin(pluginInfo.SocketPath)

	// 获取已注册插件并确认列表为空。
	aswPlugins := asw.GetRegisteredPlugins()
	if len(aswPlugins) != 0 {
		t.Fatalf("Actual state of world length should be zero but it's %d", len(aswPlugins))
	}

	// 校验 UUID 匹配检查返回 false。
	if asw.PluginExistsWithCorrectUUID(pluginInfo) {
		t.Fatalf("PluginExistsWithCorrectUUID returns true for the removed plugin")
	}

	// 在 Windows 上跳过时间戳检查：Windows 时间精度较低，
	// 连续两次 time.Now() 可能返回相同时间。
	if goruntime.GOOS != "windows" && asw.PluginExistsWithCorrectTimestamp(pluginInfo) {
		t.Fatalf("PluginExistsWithCorrectTimestamp returns true for the removed plugin")
	}
}

// Test_ASW_PluginExistsWithCorrectUUID_Negative_WrongUUID 验证 UUID 不匹配时返回 false。
func Test_ASW_PluginExistsWithCorrectUUID_Negative_WrongUUID(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 先添加一个插件。
	asw := NewActualStateOfWorld()
	pluginInfo := PluginInfo{
		SocketPath: "/var/lib/kubelet/device-plugins/test-plugin.sock",
		Timestamp:  time.Now(),
		UUID:       uuid.NewUUID(),
		Handler:    nil,
		Name:       "test",
	}
	err := asw.AddPlugin(tCtx, pluginInfo)
	// Assert
	if err != nil {
		t.Fatalf("AddPlugin failed. Expected: <no error> Actual: <%v>", err)
	}

	newerPlugin := PluginInfo{
		SocketPath: "/var/lib/kubelet/device-plugins/test-plugin.sock",
		Timestamp:  time.Now(),
		UUID:       uuid.NewUUID(),
	}
	// 同 socket 路径但 UUID 不同，应返回 false。
	if asw.PluginExistsWithCorrectUUID(newerPlugin) {
		t.Fatalf("PluginExistsWithCorrectUUID returns true for a plugin with a different UUID")
	}
}
