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
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// Test_DSW_AddOrUpdatePlugin_Positive_NewPlugin 验证添加新插件后它出现在待注册列表中，且 PluginExists 返回 true。
func Test_DSW_AddOrUpdatePlugin_Positive_NewPlugin(t *testing.T) {
	tCtx := ktesting.Init(t)
	dsw := NewDesiredStateOfWorld()
	socketPath := "/var/lib/kubelet/device-plugins/test-plugin.sock"
	err := dsw.AddOrUpdatePlugin(tCtx, socketPath)
	// 断言添加成功。
	if err != nil {
		t.Fatalf("AddOrUpdatePlugin failed. Expected: <no error> Actual: <%v>", err)
	}

	// 确认插件出现在待注册列表中。
	dswPlugins := dsw.GetPluginsToRegister()
	if len(dswPlugins) != 1 {
		t.Fatalf("Desired state of world length should be one but it's %d", len(dswPlugins))
	}
	if dswPlugins[0].SocketPath != socketPath {
		t.Fatalf("Expected\n%s\nin desired state of world, but got\n%v\n", socketPath, dswPlugins[0])
	}

	// 新插件应能被查询到。
	if !dsw.PluginExists(socketPath) {
		t.Fatalf("PluginExists returns false for the newly added plugin")
	}
}

// Test_DSW_AddOrUpdatePlugin_Positive_ExistingPlugin 验证重复添加已有插件会刷新其 UUID（时间戳），且插件仍然存在。
func Test_DSW_AddOrUpdatePlugin_Positive_ExistingPlugin(t *testing.T) {
	tCtx := ktesting.Init(t)
	dsw := NewDesiredStateOfWorld()
	socketPath := "/var/lib/kubelet/device-plugins/test-plugin.sock"
	// 首次添加插件。
	err := dsw.AddOrUpdatePlugin(tCtx, socketPath)
	if err != nil {
		t.Fatalf("AddOrUpdatePlugin failed. Expected: <no error> Actual: <%v>", err)
	}

	// 确认插件已登记，并记下当前 UUID 以便比较后续更新。
	dswPlugins := dsw.GetPluginsToRegister()
	if len(dswPlugins) != 1 {
		t.Fatalf("Desired state of world length should be one but it's %d", len(dswPlugins))
	}
	if dswPlugins[0].SocketPath != socketPath {
		t.Fatalf("Expected\n%s\nin desired state of world, but got\n%v\n", socketPath, dswPlugins[0])
	}
	oldUUID := dswPlugins[0].UUID

	// 再次添加同一插件，UUID 应被刷新以代表更新时间戳。
	err = dsw.AddOrUpdatePlugin(tCtx, socketPath)
	if err != nil {
		t.Fatalf("AddOrUpdatePlugin failed. Expected: <no error> Actual: <%v>", err)
	}
	newDswPlugins := dsw.GetPluginsToRegister()
	if len(newDswPlugins) != 1 {
		t.Fatalf("Desired state of world length should be one but it's %d", len(newDswPlugins))
	}
	if newDswPlugins[0].SocketPath != socketPath {
		t.Fatalf("Expected\n%s\nin desired state of world, but got\n%v\n", socketPath, newDswPlugins[0])
	}

	// UUID 变化说明条目已更新，上层据此感知插件重启。
	if newDswPlugins[0].UUID == oldUUID {
		t.Fatal("New UUID is not different from the old UUID", newDswPlugins[0].UUID, oldUUID)
	}

}

// Test_DSW_AddOrUpdatePlugin_Negative_PluginMissingInfo 验证空 socket 路径会被拒绝，插件不会进入待注册列表。
func Test_DSW_AddOrUpdatePlugin_Negative_PluginMissingInfo(t *testing.T) {
	tCtx := ktesting.Init(t)
	dsw := NewDesiredStateOfWorld()
	socketPath := ""
	err := dsw.AddOrUpdatePlugin(tCtx, socketPath)
	// 空路径无法定位插件 socket，必须报错。
	require.EqualError(t, err, "socket path is empty")

	// 确认插件出现在待注册列表中。
	dswPlugins := dsw.GetPluginsToRegister()
	if len(dswPlugins) != 0 {
		t.Fatalf("Desired state of world length should be zero but it's %d", len(dswPlugins))
	}

	// 未注册或已移除的插件不应存在。
	if dsw.PluginExists(socketPath) {
		t.Fatalf("PluginExists returns true for the plugin that should not have been registered")
	}
}

// Test_DSW_RemovePlugin_Positive 验证移除插件后它不再出现在待注册列表中，且 PluginExists 返回 false。
func Test_DSW_RemovePlugin_Positive(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 先添加一个插件。
	dsw := NewDesiredStateOfWorld()
	socketPath := "/var/lib/kubelet/device-plugins/test-plugin.sock"
	err := dsw.AddOrUpdatePlugin(tCtx, socketPath)
	// 断言添加成功。
	if err != nil {
		t.Fatalf("AddOrUpdatePlugin failed. Expected: <no error> Actual: <%v>", err)
	}

	// 再移除该插件。
	dsw.RemovePlugin(socketPath)

	// 确认插件出现在待注册列表中。
	dswPlugins := dsw.GetPluginsToRegister()
	if len(dswPlugins) != 0 {
		t.Fatalf("Desired state of world length should be zero but it's %d", len(dswPlugins))
	}

	// 未注册或已移除的插件不应存在。
	if dsw.PluginExists(socketPath) {
		t.Fatalf("PluginExists returns true for the removed plugin")
	}
}
