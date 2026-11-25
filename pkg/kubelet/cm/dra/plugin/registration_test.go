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

package plugin

import (
	"context"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	cgotesting "k8s.io/client-go/testing"
	drapb "k8s.io/kubelet/pkg/apis/dra/v1beta1"
	timedworkers "k8s.io/kubernetes/pkg/controller/tainteviction"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/utils/ptr"
)

const (
	nodeName = "worker"
	pluginA  = "pluginA"
	pluginB  = "pluginB"
)

// getFakeNode 返回固定名称的假 Node，供 DRA 插件管理器查询节点信息。
func getFakeNode(context.Context) (*v1.Node, error) {
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}, nil
}

// getSlice 构造归属于测试节点的 ResourceSlice。
func getSlice(name string) *resourceapi.ResourceSlice {
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: ptr.To(nodeName),
		},
	}
}

// getFakeClient 构造 fake clientset，并注册 DeleteCollection reactor，
// 校验 kubelet 清理 ResourceSlice 时使用的标签/字段选择器。
func getFakeClient(t *testing.T, nodeName, driverName string, slice *resourceapi.ResourceSlice) kubernetes.Interface {
	expectedSliceFields := fields.Set{"spec.nodeName": nodeName}
	fakeClient := fake.NewClientset(slice)
	fakeClient.AddReactor("delete-collection", "resourceslices", func(action cgotesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(cgotesting.DeleteCollectionAction)
		restrictions := deleteAction.GetListRestrictions()
		fieldsSelector := fields.SelectorFromSet(expectedSliceFields)
		// 字段选择器的条目来自 map，顺序随机，比较前必须先排序。
		normalize := func(selector string) string {
			requirements := strings.Split(selector, ",")
			sort.Strings(requirements)
			return strings.Join(requirements, ",")
		}
		assert.Empty(t, restrictions.Labels.String(), "label selector in DeleteCollection")
		assert.Equal(t, normalize(fieldsSelector.String()), normalize(restrictions.Fields.String()), "field selector in DeleteCollection")

		// 只会匹配到一个对象，直接删除即可；对象已不存在时删除也不报错，
		// 正好符合清理场景“没有可删对象也算成功”的语义。
		err := fakeClient.Tracker().Delete(resourceapi.SchemeGroupVersion.WithResource("resourceslices"), "", slice.Name)

		// 更新下次 reactor 调用期望的选择器：依次经历启动清理、
		// 测试插件注销清理、其他插件注销清理三个阶段。
		switch len(expectedSliceFields) {
		case 1:
			// 启动清理已完成，接下来应看到针对测试插件的清理。
			expectedSliceFields = fields.Set{"spec.nodeName": nodeName, "spec.driver": driverName}
		case 2:
			// 测试插件清理已完成，接下来应看到针对另一个插件的清理。
			otherPlugin := pluginA
			if otherPlugin == driverName {
				otherPlugin = pluginB
			}
			expectedSliceFields = fields.Set{"spec.nodeName": nodeName, "spec.driver": otherPlugin}
		}
		return true, nil, err
	})
	return fakeClient
}

// requireNoSlices 轮询等待并断言集群中不存在任何 ResourceSlice。
func requireNoSlices(tCtx ktesting.TContext) {
	tCtx.Helper()
	tCtx.Eventually(func(tCtx ktesting.TContext) error {
		slices, err := tCtx.Client().ResourceV1().ResourceSlices().List(tCtx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		assert.Empty(tCtx, slices.Items, "slices")
		return nil
	}).Should(gomega.Succeed(), "there should be no slices")
}

// TestRegistrationHandler 表驱动覆盖插件注册流程：无服务、只支持当前服务、多服务、
// 旧版本协议、同名替换、带 API client 时的 ResourceSlice 清理，以及重复注销的幂等性。
func TestRegistrationHandler(t *testing.T) {
	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "test-slice"},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: ptr.To(nodeName),
		},
	}

	socketFileA := "a.sock"
	socketFileB := "b.sock"

	// chosenService 是期望插件协商后选中的服务名；shouldError 表示校验或注册应失败。
	for _, test := range []struct {
		description       string
		driverName        string
		socketFile        string
		withClient        bool
		supportedServices []string
		shouldError       bool
		chosenService     string
	}{
		{
			description: "no-services",
			driverName:  pluginB,
			socketFile:  socketFileB,
			shouldError: true,
		},
		{
			description:       "current-service",
			driverName:        pluginB,
			socketFile:        socketFileB,
			supportedServices: []string{drapb.DRAPluginService},
			chosenService:     drapb.DRAPluginService,
		},
		{
			description:       "two-services",
			driverName:        pluginB,
			socketFile:        socketFileB,
			supportedServices: []string{drapb.DRAPluginService, "v1alpha1.NodeHealth" /* TODO：有 v1 后在此处添加 v1。 */},
			chosenService:     drapb.DRAPluginService,
		},
		// TODO：有 v1 后在此处改用 v1beta1。
		// {
		// 	description:       "old-service",
		// 	driverName:        pluginB,
		// 	socketFile:        socketFileB,
		// 	supportedServices: []string{drapbv1alpha4.NodeService},
		// 	chosenService:     drapbv1alpha4.NodeService,
		// },
		{
			// 不再支持固定选择 v1alpha4 的旧行为。
			description:       "version",
			driverName:        pluginB,
			socketFile:        socketFileB,
			supportedServices: []string{"1.0.0"},
			shouldError:       true,
		},
		{
			description:       "replace",
			driverName:        pluginA,
			socketFile:        socketFileB,
			supportedServices: []string{drapb.DRAPluginService},
			chosenService:     drapb.DRAPluginService,
		},
		{
			description:       "manage-resource-slices",
			withClient:        true,
			driverName:        pluginB,
			socketFile:        socketFileB,
			supportedServices: []string{drapb.DRAPluginService},
			chosenService:     drapb.DRAPluginService,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)

			// 为两个插件都启动 gRPC 服务：否则连接监控会开始清理 slice，
			// 与插件是否注册无关。本用例只关心注册流程，连接状态检测由
			// TestConnectionHandling 覆盖。

			service := drapb.DRAPluginService
			tmp := t.TempDir()
			endpointA := path.Join(tmp, socketFileA)
			// 先为插件 A 启动 gRPC 服务，模拟一个已存在的插件。
			teardownA, err := setupFakeGRPCServer(tCtx, service, endpointA)
			require.NoError(t, err)
			tCtx.Cleanup(teardownA)

			endpoint := path.Join(tmp, test.socketFile)
			teardown, err := setupFakeGRPCServer(tCtx, service, endpoint)
			require.NoError(t, err)
			tCtx.Cleanup(teardown)

			// 独立运行的 kubelet 可能连不上 apiserver，因此 fake client 是可选的。
			var client kubernetes.Interface
			if test.withClient {
				fakeClient := getFakeClient(t, nodeName, test.driverName, getSlice("test-slice"))
				client = fakeClient
				tCtx = tCtx.WithClients(nil, nil, client, nil, nil)
			}

			// DRAPluginManager 启动时会清理该节点上的所有 ResourceSlice。
			draPlugins := NewDRAPluginManager(tCtx, client, getFakeNode, &mockStreamHandler{}, time.Second /* 测试使用极短的擦除延迟。 */)
			// 有 client 时，启动清理会删除启动前遗留的 slice，下面用 requireNoSlices 确认。
			tCtx.Cleanup(draPlugins.Stop)
			if test.withClient {
				requireNoSlices(tCtx)
			}

			// 模拟已有一个插件 A 注册在案。
			err = draPlugins.RegisterPlugin(tCtx, pluginA, endpointA, []string{drapb.DRAPluginService}, nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				tCtx.Logf("Removing plugin %s", pluginA)
				draPlugins.DeRegisterPlugin(tCtx, pluginA, endpointA)
			})

			// ValidatePlugin 只检查服务兼容性，不应改变注册状态。
			err = draPlugins.ValidatePlugin(tCtx, test.driverName, endpoint, test.supportedServices)
			if test.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if err != nil {
				return
			}
			// 校验通过不等于已注册，查询不到插件才是预期状态。
			if test.driverName != pluginA {
				require.Nil(t, draPlugins.get(test.driverName), "not registered yet")
			}

			// 首次注册该插件。
			err = draPlugins.RegisterPlugin(tCtx, test.driverName, endpoint, test.supportedServices, nil)
			if test.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			// 注册成功后应能从管理器中查询到插件。
			plugin := draPlugins.get(test.driverName)
			require.NotNil(t, plugin, "plugin should be registered")
			t.Cleanup(func() {
				if client != nil {
					// 模拟插件运行期间自行创建 ResourceSlice，注销后应被清理。
					_, err := client.ResourceV1().ResourceSlices().Create(tCtx, slice, metav1.CreateOptions{})
					assert.NoError(t, err, "recreate slice")
				}

				tCtx.Logf("Removing plugin %s", test.driverName)
				draPlugins.DeRegisterPlugin(tCtx, test.driverName, endpoint)
				// 重复注销同一插件应是幂等操作，不应报错。
				draPlugins.DeRegisterPlugin(tCtx, test.driverName, endpoint)
				if test.withClient {
					requireNoSlices(tCtx)
				}
			})
			// 此处选中哪个插件取决于连接检测结果（可能两个、一个或零个），带有随机性；
			// 因此只断言两个插件共有的属性。
			assert.Equal(t, test.driverName, plugin.driverName, "DRA driver driver name")
			assert.Equal(t, test.chosenService, plugin.chosenService, "chosen service")
		})
	}
}

// TestConnectionHandling 验证服务连接状态变化时的反应：断连后延迟清理 slice，重连则取消清理。
func TestConnectionHandling(t *testing.T) {
	t.Parallel()
	// 两个子用例分别覆盖“断连触发清理”和“重连取消清理”。
	for description, test := range map[string]struct {
		delay               time.Duration
		requireSliceRemoval bool
	}{
		"wipe-on-disconnect": {
			delay:               time.Second, // 测试用极短清理延迟
			requireSliceRemoval: true,
		},
		"no-wipe-on-reconnect": {
			delay:               time.Hour, // 长延迟，避免测试运行期间触发清理
			requireSliceRemoval: false,
		},
	} {
		t.Run(description, func(t *testing.T) {
			t.Parallel()
			tCtx := ktesting.Init(t)

			service := drapb.DRAPluginService
			driverName := "test-plugin"
			sliceName := "test-slice"

			slice := getSlice(sliceName)
			client := getFakeClient(t, nodeName, driverName, slice)
			tCtx = tCtx.WithClients(nil, nil, client, nil, nil)

			// 管理器启动时先清空已有 slice。
			draPlugins := NewDRAPluginManager(tCtx, client, getFakeNode, &mockStreamHandler{}, test.delay)
			tCtx.Cleanup(draPlugins.Stop)
			requireNoSlices(tCtx)

			// 启动插件 gRPC 服务并注册插件。
			endpoint := path.Join(t.TempDir(), "dra.sock")
			teardown, err := setupFakeGRPCServer(tCtx, service, endpoint)
			require.NoError(t, err)
			defer teardown()

			err = draPlugins.RegisterPlugin(tCtx, driverName, endpoint, []string{service}, nil)
			require.NoError(t, err)

			plugin := draPlugins.get(driverName)
			assert.NotNil(t, plugin, "plugin should be present in the plugin store")

			// 模拟插件运行期间创建 ResourceSlice。
			_, err = client.ResourceV1().ResourceSlices().Create(tCtx, slice, metav1.CreateOptions{})
			require.NoError(t, err, "recreate slice")

			// 关闭 gRPC 服务，模拟插件断连。
			tCtx.Log("Stopping plugin gRPC server")
			teardown()

			if test.requireSliceRemoval {
				// 断连且延迟到期后，slice 应被清理。
				requireNoSlices(tCtx)
			} else {
				// 清理任务先进入队列等待延迟到期，不会立即删除 slice。
				wipingIsPending := func() bool {
					return draPlugins.pendingWipes.GetWorkerUnsafe(timedworkers.NewWorkArgs(driverName, "").KeyFromWorkArgs()) != nil
				}
				require.Eventuallyf(t, wipingIsPending, time.Minute, time.Second, "wiping should be queued for plugin %s", driverName)

				// 在延迟到期前重启 gRPC 服务，模拟插件重连。
				tCtx.Log("Restarting plugin gRPC server")
				teardown, err = setupFakeGRPCServer(tCtx, service, endpoint)
				require.NoError(t, err)
				defer teardown()

				// 重连后应取消挂起的清理任务。
				require.Eventuallyf(t, func() bool {
					return !wipingIsPending()
				}, time.Minute, time.Second, "wiping should be stopped for plugin %s", driverName)

				// 重连及时的话，slice 应仍然存在。
				slices, err := client.ResourceV1().ResourceSlices().List(tCtx, metav1.ListOptions{})
				require.NoError(t, err, "list slices")
				assert.Len(t, slices.Items, 1, "slices")
			}
		})
	}
}
