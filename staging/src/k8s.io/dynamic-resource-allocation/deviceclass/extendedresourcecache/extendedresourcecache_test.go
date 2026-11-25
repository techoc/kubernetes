/*
Copyright 2025 The Kubernetes Authors.

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

package extendedresourcecache

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init" // 添加命令行参数。
	"k8s.io/utils/ptr"
)

// deviceClassResolver 是供测试使用的窄接口，只暴露按扩展资源名查询 DeviceClass 的能力，
// 便于把 nil 缓存实例当作接口值来验证 nil 接收者行为。
type deviceClassResolver interface {
	GetDeviceClass(resourceName v1.ResourceName) *resourceapi.DeviceClass
}

// TestNil 验证 nil 的 ExtendedResourceCache 实例通过接口调用 GetDeviceClass 时返回 nil 而不 panic。
// 输入为 nil 缓存与任意资源名，断言返回结果为 nil。
func TestNil(t *testing.T) {
	var cache *ExtendedResourceCache
	var resolver deviceClassResolver = cache
	if class := resolver.GetDeviceClass("example.com/gpu"); class != nil {
		t.Errorf("Expected the nil class from a nil instance, got instead: %q", class.Name)
	}
}

// TestHandlers 验证附加事件处理器的注册顺序与调用时机：
// 构造两个处理器并依次通过构造参数和 AddEventHandler 注册，输入为 Add/Update/Delete 三类事件；
// 断言处理器按注册顺序各被调用一次、收到正确的事件对象，且第二个处理器中能查到缓存已更新的状态。
func TestHandlers(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 用计数器记录各处理器的调用顺序与次数。
	var numAdd, numUpdate, numDelete int

	resourceName := v1.ResourceName("example.com/gpu")
	class := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-class",
		},
		Spec: resourceapi.DeviceClassSpec{
			ExtendedResourceName: (*string)(&resourceName),
		},
	}
	updatedClass := class.DeepCopy()
	updatedClass.Spec.ExtendedResourceName = nil

	firstHandler := &cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if obj != class {
				t.Errorf("first handler expected added object %v, got %v", class, obj)
			}
			numAdd++
			if numAdd != 1 {
				t.Errorf("first handler expected Add to be called first once, actual add #%d", numAdd)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if oldObj != class {
				t.Errorf("first handler expected old object %v, got %v", class, oldObj)
			}
			if newObj != updatedClass {
				t.Errorf("first handler expected new object %v, got %v", class, newObj)
			}
			numUpdate++
			if numUpdate != 1 {
				t.Errorf("first handler expected Update to be called first once, actual update #%d", numUpdate)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if obj != updatedClass {
				t.Errorf("first handler expected deleted object %v, got %v", class, obj)
			}
			numDelete++
			if numDelete != 1 {
				t.Errorf("first handler expected Delete to be called first once, actual delete #%d", numDelete)
			}
		},
	}
	erCache := NewExtendedResourceCache(logger, firstHandler)
	secondHandler := &cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if obj != class {
				t.Errorf("second handler expected added object %v, got %v", class, obj)
			}
			numAdd++
			if numAdd != 2 {
				t.Errorf("second handler expected Add to be called last once, actual add #%d", numAdd)
			}
			if deviceClass := erCache.GetDeviceClass(resourceName); deviceClass == nil || deviceClass.Name != class.Name {
				t.Errorf("expected %q, got %q", class.Name, deviceClass)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if oldObj != class {
				t.Errorf("second handler expected old object %v, got %v", class, oldObj)
			}
			if newObj != updatedClass {
				t.Errorf("second handler expected new object %v, got %v", class, newObj)
			}
			numUpdate++
			if numUpdate != 2 {
				t.Errorf("second handler expected Update to be called last once, actual update #%d", numUpdate)
			}
			if class := erCache.GetDeviceClass(resourceName); class != nil {
				t.Errorf("expected %q, got %v", "", class)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if obj != updatedClass {
				t.Errorf("second handler expected deleted object %v, got %v", class, obj)
			}
			numDelete++
			if numDelete != 2 {
				t.Errorf("second handler expected Delete to be called last once, actual delete #%d", numDelete)
			}
			if class := erCache.GetDeviceClass(resourceName); class != nil {
				t.Errorf("expected %q, got %q", "", class)
			}
		},
	}
	// 第二个处理器通过 AddEventHandler 注册，排在构造参数中的第一个处理器之后。
	erCache.AddEventHandler(secondHandler)

	// 依次触发三类事件，观察两个处理器的调用顺序及缓存查询状态。
	erCache.OnAdd(class, false)
	erCache.OnUpdate(class, updatedClass)
	erCache.OnDelete(updatedClass)
}

// TestExtendedResourceCache 在 synctest 的受控时钟下运行 testExtendedResourceCache，
// 以便可靠地等待 informer 后台协程处理完事件。
// testExtendedResourceCache 通过 fake 客户端与 informer 端到端验证缓存行为：
// 输入为若干带不同创建时间与扩展资源名的 DeviceClass，以及更新、删除操作；
// 断言显式映射、隐式默认映射、冲突时的新旧与名称优先级、映射变更与删除清理均符合预期。
func TestExtendedResourceCache(t *testing.T) { synctest.Test(t, testExtendedResourceCache) }
func testExtendedResourceCache(t *testing.T) {
	tCtx, client, cache := setup(t)

	now := time.Now()
	deviceClass1 := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-class",
			CreationTimestamp: metav1.Time{
				Time: now,
			},
		},
		Spec: resourceapi.DeviceClassSpec{
			ExtendedResourceName: ptr.To("example.com/gpu"),
		},
	}

	deviceClass2 := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fpga-class",
		},
		// 未显式指定扩展资源名，使用默认映射。
		Spec: resourceapi.DeviceClassSpec{},
	}
	deviceClass3 := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-class-3",
			CreationTimestamp: metav1.Time{
				Time: now.Add(-24 * time.Hour),
			},
		},
		Spec: resourceapi.DeviceClassSpec{
			ExtendedResourceName: ptr.To("example.com/gpu"),
		},
	}
	deviceClass4 := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-class-4",
			CreationTimestamp: metav1.Time{
				Time: now.Add(time.Hour),
			},
		},
		Spec: resourceapi.DeviceClassSpec{
			ExtendedResourceName: ptr.To("example.com/gpu"),
		},
	}
	deviceClass0 := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-class-0",
			CreationTimestamp: metav1.Time{
				Time: now.Add(time.Hour),
			},
		},
		Spec: resourceapi.DeviceClassSpec{
			ExtendedResourceName: ptr.To("example.com/gpu"),
		},
	}

	_, err := client.ResourceV1().DeviceClasses().Create(tCtx, deviceClass1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create device class: %v", err)
	}
	_, err = client.ResourceV1().DeviceClasses().Create(tCtx, deviceClass2, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create device class: %v", err)
	}
	synctest.Wait()

	deviceClass := cache.GetDeviceClass("example.com/gpu")
	if deviceClass == nil || deviceClass.Name != "gpu-class" {
		t.Errorf("Expected to find device class 'gpu-class' for 'example.com/gpu', got %v", deviceClass)
	}

	defaultResourceName := v1.ResourceName("deviceclass.resource.kubernetes.io/fpga-class")
	deviceClass = cache.GetDeviceClass(defaultResourceName)
	if deviceClass == nil || deviceClass.Name != "fpga-class" {
		t.Errorf("Expected to find device class 'fpga-class' for '%s', got %v", defaultResourceName, deviceClass)
	}

	deviceClass = cache.GetDeviceClass("deviceclass.resource.kubernetes.io/gpu-class")
	if deviceClass == nil || deviceClass.Name != "gpu-class" {
		t.Error("Expected default mapping for gpu-class")
	}

	_, err = client.ResourceV1().DeviceClasses().Create(tCtx, deviceClass3, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create device class: %v", err)
	}
	synctest.Wait()

	deviceClass = cache.GetDeviceClass("example.com/gpu")
	if deviceClass == nil || deviceClass.Name != "gpu-class" {
		t.Errorf("Expected to find device class 'gpu-class' for 'example.com/gpu', got %v", deviceClass)
	}

	_, err = client.ResourceV1().DeviceClasses().Create(tCtx, deviceClass4, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create device class: %v", err)
	}
	synctest.Wait()

	deviceClass = cache.GetDeviceClass("example.com/gpu")
	if deviceClass == nil || deviceClass.Name != "gpu-class-4" {
		t.Errorf("Expected to find device class 'gpu-class' for 'example.com/gpu', got %v", deviceClass)
	}

	_, err = client.ResourceV1().DeviceClasses().Create(tCtx, deviceClass0, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create device class: %v", err)
	}
	synctest.Wait()

	deviceClass = cache.GetDeviceClass("example.com/gpu")
	if deviceClass == nil || deviceClass.Name != "gpu-class-0" {
		t.Errorf("Expected to find device class 'gpu-class' for 'example.com/gpu', got %v", deviceClass)
	}

	deviceClass0Modified := deviceClass0.DeepCopy()
	deviceClass0Modified.Spec.ExtendedResourceName = ptr.To("test.com/gpu")
	_, err = client.ResourceV1().DeviceClasses().Update(tCtx, deviceClass0Modified, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update device class: %v", err)
	}
	synctest.Wait()

	deviceClass = cache.GetDeviceClass("test.com/gpu")
	if deviceClass == nil || deviceClass.Name != "gpu-class-0" {
		t.Errorf("Expected to find device class 'gpu-class-0' for 'test.com/gpu' after modification, got %v", deviceClass)
	}
	if cache.GetDeviceClass("example.com/gpu") != nil {
		t.Errorf("Expected 'example.com/gpu' to be removed after modification, got %s", cache.GetDeviceClass("example.com/gpu"))
	}

	err = client.ResourceV1().DeviceClasses().Delete(tCtx, deviceClass0.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Failed to delete device class: %v", err)
	}
	synctest.Wait()

	deviceClass = cache.GetDeviceClass("test.com/gpu")
	if deviceClass != nil {
		t.Errorf("Expected 'test.com/gpu' to be removed after deleting device class, got %s", deviceClass)
	}
	if cache.GetDeviceClass("deviceclass.resource.kubernetes.io/gpu-class-0") != nil {
		t.Errorf("Expected 'deviceclass.resource.kubernetes.io/gpu-class-0' to be removed after deleting device class, got %s", cache.GetDeviceClass("deviceclass.resource.kubernetes.io/gpu-class"))
	}
}

// TestDeviceClassMapping 在 synctest 受控时钟下运行 testDeviceClassMapping。
// testDeviceClassMapping 通过 fake 客户端验证“DeviceClass 名 → 扩展资源名”反向映射：
// 输入为带显式资源名的 gpu-class 与无显式资源名的 tpu-class，随后更新并删除 gpu-class；
// 断言只有显式声明的类出现在反向映射中，且更新、删除后映射相应变更或消失。
func TestDeviceClassMapping(t *testing.T) { synctest.Test(t, testDeviceClassMapping) }
func testDeviceClassMapping(t *testing.T) {
	tCtx, client, cache := setup(t)

	deviceClass1 := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-class",
		},
		Spec: resourceapi.DeviceClassSpec{
			ExtendedResourceName: ptr.To("example.com/gpu"),
		},
	}

	deviceClass2 := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tpu-class",
		},
	}

	_, err := client.ResourceV1().DeviceClasses().Create(tCtx, deviceClass1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create device class: %v", err)
	}
	_, err = client.ResourceV1().DeviceClasses().Create(tCtx, deviceClass2, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create device class: %v", err)
	}

	synctest.Wait()
	name := cache.GetExtendedResource("gpu-class")
	if name != "example.com/gpu" {
		t.Errorf("Expected to find device class 'gpu-class', got %s", name)
	}
	name = cache.GetExtendedResource("tpu-class")
	if name != "" {
		t.Errorf("Expected device class 'tpu-class' not found")
	}

	deviceClass1Modified := deviceClass1.DeepCopy()
	deviceClass1Modified.Spec.ExtendedResourceName = ptr.To("my.com/gpu")
	_, err = client.ResourceV1().DeviceClasses().Update(tCtx, deviceClass1Modified, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update device class: %v", err)
	}

	synctest.Wait()
	name = cache.GetExtendedResource("gpu-class")
	if name != "my.com/gpu" {
		t.Errorf("Expected to find device class 'gpu-class' with  'my.com/gpu' after modification, got %s", name)
	}

	err = client.ResourceV1().DeviceClasses().Delete(tCtx, deviceClass1.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Failed to delete device class: %v", err)
	}

	synctest.Wait()
	name = cache.GetExtendedResource("gpu-class")
	if name != "" {
		t.Error("Expected 'gpu-class' not found after deletion")
	}
}

// setup 为测试搭建 fake 客户端 + DeviceClass informer + ExtendedResourceCache，
// 注册事件处理器、启动并等待缓存同步，最后返回上下文、客户端与缓存实例。
// 清理函数会先取消上下文再关闭 informer，确保后台协程退出。
func setup(t *testing.T) (context.Context, *fake.Clientset, *ExtendedResourceCache) {
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	client := fake.NewClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	ec := NewExtendedResourceCache(logger)
	handle, err := informerFactory.Resource().V1().DeviceClasses().Informer().AddEventHandler(ec)
	if err != nil {
		t.Fatalf("failed to add device class informer event handler: %v", err)
	}
	informerFactory.Start(ctx.Done())
	t.Cleanup(func() {
		cancel()
		informerFactory.Shutdown()
	})
	informerFactory.WaitForCacheSync(ctx.Done())
	cache.WaitForNamedCacheSyncWithContext(ctx, handle.HasSynced)

	// fake.Clientset 的 Watch 不支持 resourceVersion，并假定监听建立后才发生后续变更。
	// 若缓存已同步但 Watch 尚未完成，测试立即创建的对象可能永远不会被事件处理器看到；
	// 竞态位置参见 https://github.com/kubernetes/kubernetes/blob/b53b9fb5573323484af9a19cf3f5bfe80760abba/staging/src/k8s.io/client-go/tools/cache/reflector.go#L431。
	// 因此先等待 observe/watch 相关协程就绪，再创建 DeviceClass。
	synctest.Wait()

	return ctx, client, ec
}
