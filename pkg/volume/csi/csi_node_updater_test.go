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

package csi

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/client-go/informers"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

// testDriverName 是各测试用例复用的测试驱动名。
const testDriverName = "test-driver"

// testFixture 聚合 fake client、informer、updater 及停止通道，便于测试统一收尾。
type testFixture struct {
	client         *fakeclient.Clientset
	factory        informers.SharedInformerFactory
	driverInformer cache.SharedIndexInformer
	updater        *csiNodeUpdater
	stopCh         chan struct{}
}

// setupTest 启动 CSIDriver informer 并等待缓存同步后构造 updater，随后由调用方清理。
func setupTest(t *testing.T, driver *v1.CSIDriver) *testFixture {
	var clientObjects []runtime.Object
	if driver != nil {
		clientObjects = append(clientObjects, driver)
	}

	client := fakeclient.NewSimpleClientset(clientObjects...)
	factory := informers.NewSharedInformerFactory(client, 0)
	driverInformer := factory.Storage().V1().CSIDrivers().Informer()

	stopCh := make(chan struct{})
	factory.Start(stopCh)

	if !cache.WaitForCacheSync(stopCh, driverInformer.HasSynced) {
		close(stopCh)
		t.Fatalf("Timed out waiting for caches to sync")
	}

	updater, err := NewCSINodeUpdater(driverInformer)
	if err != nil {
		close(stopCh)
		t.Fatalf("Failed to create CSINodeUpdater: %v", err)
	}

	return &testFixture{
		client:         client,
		factory:        factory,
		driverInformer: driverInformer,
		updater:        updater,
		stopCh:         stopCh,
	}
}

// TestNewCSINodeUpdater 验证合法 informer 能创建 updater，nil informer 返回错误。
func TestNewCSINodeUpdater(t *testing.T) {
	t.Run("valid informer", func(t *testing.T) {
		f := setupTest(t, nil)
		defer f.cleanup()

		updater, err := NewCSINodeUpdater(f.driverInformer)

		if err != nil {
			t.Errorf("Expected no error but got: %v", err)
		}
		if updater == nil {
			t.Errorf("Expected non-nil updater")
		}
	})

	t.Run("nil informer", func(t *testing.T) {
		updater, err := NewCSINodeUpdater(nil)

		if err == nil {
			t.Errorf("Expected error but got none")
		}
		if updater != nil {
			t.Errorf("Expected nil updater when error occurs")
		}
	})
}

// TestSyncDriverUpdater 驱动卸载、未注册、未设置更新周期时会停止 updater，
// 已存在 updater 时则先停止旧实例再替换为新实例。
func TestSyncDriverUpdater(t *testing.T) {
	t.Run("driver not installed, should stop updater", func(t *testing.T) {
		f := setupTest(t, nil)
		defer f.cleanup()

		updaterStopCh := make(chan struct{})
		f.updater.driverUpdaters.Store(testDriverName, updaterStopCh)

		// 初始状态：先确认已有 updater，后续才能验证它被清理。
		verifyUpdaterState(t, f.updater, testDriverName, true)

		// informer 中没有该驱动，应视为驱动未安装。
		f.updater.syncDriverUpdater(testDriverName)

		// 驱动应从 updater 表中注销。
		verifyUpdaterState(t, f.updater, testDriverName, false)

		if !isChannelClosed(updaterStopCh) {
			t.Errorf("Stop channel was not closed")
		}
	})

	t.Run("driver not found in informer, should stop updater", func(t *testing.T) {
		f := setupTest(t, nil)
		defer f.cleanup()

		// 只注册到全局表，informer 中保持缺失，以覆盖两者不一致的场景。
		registerTestDriver(testDriverName)
		defer unregisterTestDriver(testDriverName)

		updaterStopCh := make(chan struct{})
		f.updater.driverUpdaters.Store(testDriverName, updaterStopCh)

		verifyUpdaterState(t, f.updater, testDriverName, true)

		// 以 informer 为准，全局表里有但 informer 里没有也应停止 updater。
		f.updater.syncDriverUpdater(testDriverName)
		verifyUpdaterState(t, f.updater, testDriverName, false)

		if !isChannelClosed(updaterStopCh) {
			t.Errorf("Stop channel was not closed")
		}
	})

	t.Run("driver with unset updatePeriodSeconds, should stop updater", func(t *testing.T) {
		driver := createTestDriver(testDriverName, nil)
		f := setupTest(t, driver)
		defer f.cleanup()

		registerTestDriver(testDriverName)
		defer unregisterTestDriver(testDriverName)

		updaterStopCh := make(chan struct{})
		f.updater.driverUpdaters.Store(testDriverName, updaterStopCh)

		verifyUpdaterState(t, f.updater, testDriverName, true)

		// 未设置更新周期时无需周期性更新，updater 应被停止。
		f.updater.syncDriverUpdater(testDriverName)
		verifyUpdaterState(t, f.updater, testDriverName, false)

		if !isChannelClosed(updaterStopCh) {
			t.Errorf("Stop channel was not closed")
		}
	})

	t.Run("replace existing updater", func(t *testing.T) {
		updatePeriod := int64(60)
		driver := createTestDriver(testDriverName, &updatePeriod)
		f := setupTest(t, driver)
		defer f.cleanup()

		registerTestDriver(testDriverName)
		defer unregisterTestDriver(testDriverName)

		oldStopCh := make(chan struct{})
		f.updater.driverUpdaters.Store(testDriverName, oldStopCh)
		verifyUpdaterState(t, f.updater, testDriverName, true)

		// 配置变更后需停掉旧实例再启动新实例，避免两个更新循环并存。
		f.updater.syncDriverUpdater(testDriverName)

		if !isChannelClosed(oldStopCh) {
			t.Errorf("Previous stop channel was not closed during updater replacement")
		}

		// 新停止通道必须是不同的实例，否则旧循环并未真正被替换。
		value, exists := f.updater.driverUpdaters.Load(testDriverName)
		if !exists {
			t.Errorf("No updater entry exists after replacement")
		} else {
			newStopCh, ok := value.(chan struct{})
			if !ok {
				t.Errorf("Updated entry is not a channel")
			} else if reflect.ValueOf(newStopCh).Pointer() == reflect.ValueOf(oldStopCh).Pointer() {
				t.Errorf("New stop channel is the same as the old one")
			}

			close(newStopCh)
		}
	})
}

// cleanup 关闭 informer 停止通道，释放测试启动的后台协程。
func (f *testFixture) cleanup() {
	close(f.stopCh)
}

// createTestDriver 构造带有指定更新周期的 CSIDriver 对象。
func createTestDriver(name string, updatePeriodSeconds *int64) *v1.CSIDriver {
	return &v1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.CSIDriverSpec{
			NodeAllocatableUpdatePeriodSeconds: updatePeriodSeconds,
		},
	}
}

// registerTestDriver 将驱动写入全局表，模拟驱动已在节点上注册。
func registerTestDriver(name string) {
	csiDrivers.Set(name, Driver{
		endpoint:                "test-endpoint",
		highestSupportedVersion: &utilversion.Version{},
	})
}

// unregisterTestDriver 从全局表移除驱动，供测试收尾清理。
func unregisterTestDriver(name string) {
	csiDrivers.Delete(name)
}

// isChannelClosed 非阻塞地检查通道是否已关闭。
func isChannelClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// verifyUpdaterState 断言 updater 表中指定驱动的条目存在与否符合预期。
func verifyUpdaterState(t *testing.T, updater *csiNodeUpdater, driverName string, shouldExist bool) {
	_, exists := updater.driverUpdaters.Load(driverName)
	if shouldExist && !exists {
		t.Errorf("Expected updater for driver %s to exist, but it doesn't", driverName)
	} else if !shouldExist && exists {
		t.Errorf("Expected updater for driver %s to not exist, but it does", driverName)
	}
}
