/*
Copyright 2024 The Kubernetes Authors.

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

package dra

import (
	"errors"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/kubernetes/pkg/kubelet/cm/dra/state"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// 测试中固定使用的驱动、资源池、设备以及命名空间/声明名称。
const (
	testDriver    = "test-driver"
	testPool      = "test-pool"
	testDevice    = "test-device"
	testNamespace = "test-namespace"
	testClaim     = "test-claim"
)

// testDeviceHealth 是后续测试反复使用的健康设备状态。
var (
	testDeviceHealth = state.DeviceHealth{
		PoolName:           testPool,
		DeviceName:         testDevice,
		Health:             state.DeviceHealthStatusHealthy,
		HealthCheckTimeout: DefaultHealthTimeout,
	}
)

// TestNewHealthInfoCache 测试缓存的创建与检查点加载。
func TestNewHealthInfoCache(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tests := []struct {
		description string
		stateFile   string
		wantErr     bool
	}{
		{
			description: "successfully created cache",
			stateFile:   path.Join(t.TempDir(), "health_checkpoint"),
		},
		{
			description: "empty state file",
			stateFile:   "",
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			if test.stateFile != "" {
				f, err := os.Create(test.stateFile)
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}
			cache, err := newHealthInfoCache(logger, test.stateFile)
			if test.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, cache)
			if test.stateFile != "" {
				require.NoError(t, os.Remove(test.stateFile))
			}
		})
	}
}

// assertDeviceHealthElementsMatchIgnoreTime 是辅助函数，用于比较 DeviceHealth 切片，
// 比较时忽略 LastUpdated 时间。
func assertDeviceHealthElementsMatchIgnoreTime(t *testing.T, expected, actual []state.DeviceHealth) {
	require.Len(t, actual, len(expected), "Number of changed devices mismatch")

	// 构造去掉 LastUpdated 的可比较版本
	normalize := func(dh state.DeviceHealth) state.DeviceHealth {
		// 将时间清零后再比较
		dh.LastUpdated = time.Time{}
		return dh
	}

	expectedNormalized := make([]state.DeviceHealth, len(expected))
	actualNormalized := make([]state.DeviceHealth, len(actual))

	for i := range expected {
		expectedNormalized[i] = normalize(expected[i])
	}
	for i := range actual {
		actualNormalized[i] = normalize(actual[i])
	}

	assert.ElementsMatch(t, expectedNormalized, actualNormalized, "Changed device elements mismatch (ignoring time)")
}

// TestWithLock 测试 withLock 方法的行为。
func TestWithLock(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	cache, err := newHealthInfoCache(logger, "")
	require.NoError(t, err)
	tests := []struct {
		description string
		f           func() error
		wantErr     bool
	}{
		{
			description: "lock prevents concurrent lock",
			f: func() error {
				if cache.TryLock() {
					defer cache.Unlock()
					return errors.New("Lock succeeded")
				}
				return nil
			},
		},
		{
			description: "erroring function",
			f: func() error {
				return errors.New("test error")
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := cache.withLock(test.f)
			if test.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestWithRLock 测试 withRLock 方法的行为。
func TestWithRLock(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	cache, err := newHealthInfoCache(logger, "")
	require.NoError(t, err)
	tests := []struct {
		description string
		f           func() error
		wantErr     bool
	}{
		{
			description: "rlock allows concurrent rlock",
			f: func() error {
				if !cache.TryRLock() {
					return errors.New("Concurrent RLock failed")
				}
				defer cache.RUnlock()
				return nil
			},
			wantErr: false,
		},
		{
			description: "rlock prevents lock",
			f: func() error {
				if cache.TryLock() {
					defer cache.Unlock()
					return errors.New("Write Lock succeeded: Bad")
				}
				return nil
			},
			wantErr: false,
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := cache.withRLock(test.f)
			if test.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestGetHealthInfo 测试健康状态的查询。
func TestGetHealthInfo(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	cache, err := newHealthInfoCache(logger, "")
	require.NoError(t, err)

	// 初始状态
	assert.Equal(t, state.DeviceHealthStatusUnknown, cache.getHealthInfo(testDriver, testPool, testDevice).Health)

	// 添加一个设备
	_, err = cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{testDeviceHealth})
	require.NoError(t, err)
	assert.Equal(t, state.DeviceHealthStatusHealthy, cache.getHealthInfo(testDriver, testPool, testDevice).Health)

	// 测试超时场景（通过设置旧的 LastUpdated 模拟）
	err = cache.withLock(func() error {
		driverState := (*cache.HealthInfo)[testDriver]
		deviceKey := testPool + "/" + testDevice
		device := driverState.Devices[deviceKey]
		device.LastUpdated = time.Now().Add((-DefaultHealthTimeout) - time.Second)
		driverState.Devices[deviceKey] = device
		(*cache.HealthInfo)[testDriver] = driverState
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, state.DeviceHealthStatusUnknown, cache.getHealthInfo(testDriver, testPool, testDevice).Health)
}

// TestGetHealthInfoRobust 针对大量边界场景单独测试健康状态的查询逻辑。
func TestGetHealthInfoRobust(t *testing.T) {
	tests := []struct {
		name           string
		initialState   *state.DevicesHealthMap
		driverName     string
		poolName       string
		deviceName     string
		expectedHealth state.DeviceHealthStatus
	}{
		{
			name:           "empty cache",
			initialState:   &state.DevicesHealthMap{},
			driverName:     testDriver,
			poolName:       testPool,
			deviceName:     testDevice,
			expectedHealth: state.DeviceHealthStatusUnknown,
		},
		{
			name: "device exists and is healthy",
			initialState: &state.DevicesHealthMap{
				testDriver: {Devices: map[string]state.DeviceHealth{
					testPool + "/" + testDevice: {PoolName: testPool, DeviceName: testDevice, Health: state.DeviceHealthStatusHealthy, LastUpdated: time.Now(), HealthCheckTimeout: DefaultHealthTimeout},
				}},
			},
			driverName:     testDriver,
			poolName:       testPool,
			deviceName:     testDevice,
			expectedHealth: "Healthy",
		},
		{
			name: "device exists and is unhealthy",
			initialState: &state.DevicesHealthMap{
				testDriver: {Devices: map[string]state.DeviceHealth{
					testPool + "/" + testDevice: {PoolName: testPool, DeviceName: testDevice, Health: state.DeviceHealthStatusUnhealthy, LastUpdated: time.Now(), HealthCheckTimeout: DefaultHealthTimeout},
				}},
			},
			driverName:     testDriver,
			poolName:       testPool,
			deviceName:     testDevice,
			expectedHealth: "Unhealthy",
		},
		{
			name: "device exists but timed out",
			initialState: &state.DevicesHealthMap{
				testDriver: {Devices: map[string]state.DeviceHealth{
					testPool + "/" + testDevice: {PoolName: testPool, DeviceName: testDevice, Health: state.DeviceHealthStatusHealthy, LastUpdated: time.Now().Add((-1 * DefaultHealthTimeout) - time.Second), HealthCheckTimeout: DefaultHealthTimeout},
				}},
			},
			driverName:     testDriver,
			poolName:       testPool,
			deviceName:     testDevice,
			expectedHealth: "Unknown",
		},
		{
			name: "device exists, just within timeout",
			initialState: &state.DevicesHealthMap{
				testDriver: {Devices: map[string]state.DeviceHealth{
					testPool + "/" + testDevice: {PoolName: testPool, DeviceName: testDevice, Health: state.DeviceHealthStatusHealthy, LastUpdated: time.Now().Add((-1 * DefaultHealthTimeout) + time.Second), HealthCheckTimeout: DefaultHealthTimeout},
				}},
			},
			driverName:     testDriver,
			poolName:       testPool,
			deviceName:     testDevice,
			expectedHealth: "Healthy",
		},
		{
			name: "device does not exist, just outside of timeout",
			initialState: &state.DevicesHealthMap{
				testDriver: {Devices: map[string]state.DeviceHealth{
					testPool + "/" + testDevice: {PoolName: testPool, DeviceName: testDevice, Health: state.DeviceHealthStatusHealthy, LastUpdated: time.Now().Add((-1 * DefaultHealthTimeout) - time.Second), HealthCheckTimeout: DefaultHealthTimeout},
				}},
			},
			driverName:     testDriver,
			poolName:       testPool,
			deviceName:     "device2",
			expectedHealth: "Unknown",
		},
		{
			name: "device does not exist",
			initialState: &state.DevicesHealthMap{
				testDriver: {Devices: map[string]state.DeviceHealth{
					testPool + "/" + testDevice: {PoolName: testPool, DeviceName: testDevice, Health: state.DeviceHealthStatusHealthy, LastUpdated: time.Now(), HealthCheckTimeout: DefaultHealthTimeout},
				}},
			},
			driverName:     testDriver,
			poolName:       testPool,
			deviceName:     "device2",
			expectedHealth: "Unknown",
		},
		{
			name: "driver does not exist",
			initialState: &state.DevicesHealthMap{
				testDriver: {Devices: map[string]state.DeviceHealth{
					testPool + "/" + testDevice: {PoolName: testPool, DeviceName: testDevice, Health: state.DeviceHealthStatusHealthy, LastUpdated: time.Now(), HealthCheckTimeout: DefaultHealthTimeout},
				}},
			},
			driverName:     "driver2",
			poolName:       testPool,
			deviceName:     testDevice,
			expectedHealth: "Unknown",
		},
		{
			name: "pool does not exist",
			initialState: &state.DevicesHealthMap{
				testDriver: {Devices: map[string]state.DeviceHealth{
					testPool + "/" + testDevice: {PoolName: testPool, DeviceName: testDevice, Health: state.DeviceHealthStatusHealthy, LastUpdated: time.Now(), HealthCheckTimeout: DefaultHealthTimeout},
				}},
			},
			driverName:     testDriver,
			poolName:       "pool2",
			deviceName:     testDevice,
			expectedHealth: "Unknown",
		},
		{
			name: "multiple devices",
			initialState: &state.DevicesHealthMap{
				testDriver: {Devices: map[string]state.DeviceHealth{
					testPool + "/" + testDevice: {PoolName: testPool, DeviceName: testDevice, Health: state.DeviceHealthStatusHealthy, LastUpdated: time.Now(), HealthCheckTimeout: DefaultHealthTimeout},
					testPool + "/device2":       {PoolName: testPool, DeviceName: "device2", Health: state.DeviceHealthStatusUnhealthy, LastUpdated: time.Now(), HealthCheckTimeout: DefaultHealthTimeout},
				}},
			},
			driverName:     testDriver,
			poolName:       testPool,
			deviceName:     "device2",
			expectedHealth: "Unhealthy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := &healthInfoCache{HealthInfo: tt.initialState}
			health := cache.getHealthInfo(tt.driverName, tt.poolName, tt.deviceName).Health
			assert.Equal(t, tt.expectedHealth, health)
		})
	}
}

// TestUpdateHealthInfo 测试设备健康状态的添加、更新与对账。
func TestUpdateHealthInfo(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tmpFile := path.Join(t.TempDir(), "health_checkpoint_test")
	cache, err := newHealthInfoCache(logger, tmpFile)
	require.NoError(t, err)

	// 1 -- 新增设备
	deviceToAdd := testDeviceHealth
	expectedChanged1 := []state.DeviceHealth{deviceToAdd}
	changedDevices, err := cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{testDeviceHealth})
	require.NoError(t, err)
	assertDeviceHealthElementsMatchIgnoreTime(t, expectedChanged1, changedDevices)
	assert.Equal(t, state.DeviceHealthStatusHealthy, cache.getHealthInfo(testDriver, testPool, testDevice).Health)

	// 2 -- 无变化的更新
	changedDevices, err = cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{testDeviceHealth})
	require.NoError(t, err)
	assert.Empty(t, changedDevices, "Scenario 2: Changed devices list should be empty")

	// 3 -- 以新的健康状态更新
	newHealth := testDeviceHealth
	newHealth.Health = state.DeviceHealthStatusUnhealthy
	expectedChanged3 := []state.DeviceHealth{newHealth}
	changedDevices, err = cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{newHealth})
	require.NoError(t, err)
	assertDeviceHealthElementsMatchIgnoreTime(t, expectedChanged3, changedDevices)
	assert.Equal(t, state.DeviceHealthStatusUnhealthy, cache.getHealthInfo(testDriver, testPool, testDevice).Health)

	// 4 -- 新增第二个设备，省略第一个
	secondDevice := state.DeviceHealth{PoolName: testPool, DeviceName: "device2", Health: state.DeviceHealthStatusHealthy, HealthCheckTimeout: DefaultHealthTimeout}
	// 第一个设备被省略后，应在超时后被标记为 "Unknown"。本测试通过不再上报它来模拟超时。
	firstDeviceAsUnknown := newHealth
	firstDeviceAsUnknown.Health = state.DeviceHealthStatusUnknown
	expectedChanged4 := []state.DeviceHealth{secondDevice, firstDeviceAsUnknown}
	// 手动把第一个设备的最后更新时间设置到超时窗口之外
	err = cache.withLock(func() error {
		deviceKey := testPool + "/" + testDevice
		device := (*cache.HealthInfo)[testDriver].Devices[deviceKey]
		device.LastUpdated = time.Now().Add(-DefaultHealthTimeout * 2)
		(*cache.HealthInfo)[testDriver].Devices[deviceKey] = device
		return nil
	})
	require.NoError(t, err)

	changedDevices, err = cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{secondDevice})
	require.NoError(t, err)
	assertDeviceHealthElementsMatchIgnoreTime(t, expectedChanged4, changedDevices)
	assert.Equal(t, state.DeviceHealthStatusHealthy, cache.getHealthInfo(testDriver, testPool, "device2").Health)
	assert.Equal(t, state.DeviceHealthStatusUnknown, cache.getHealthInfo(testDriver, testPool, testDevice).Health)

	// 5 -- 测试持久化
	cache2, err := newHealthInfoCache(logger, tmpFile)
	require.NoError(t, err)
	assert.Equal(t, state.DeviceHealthStatusHealthy, cache2.getHealthInfo(testDriver, testPool, "device2").Health)
	assert.Equal(t, state.DeviceHealthStatusUnknown, cache2.getHealthInfo(testDriver, testPool, testDevice).Health)

	// 6 -- 测试 updateHealthInfo 如何处理设备超时
	timeoutDevice := state.DeviceHealth{PoolName: testPool, DeviceName: "timeoutDevice", Health: "Unhealthy", HealthCheckTimeout: DefaultHealthTimeout}
	_, err = cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{timeoutDevice})
	require.NoError(t, err)

	// 手动修改 timeoutDevice 的最后更新时间，使其看起来已超过健康检查超时。
	err = cache.withLock(func() error {
		driverState := (*cache.HealthInfo)[testDriver]
		deviceKey := testPool + "/timeoutDevice"
		device := driverState.Devices[deviceKey]
		device.LastUpdated = time.Now().Add((-DefaultHealthTimeout) - time.Second)
		driverState.Devices[deviceKey] = device
		(*cache.HealthInfo)[testDriver] = driverState
		return nil
	})
	require.NoError(t, err)

	expectedTimeoutDeviceUnknown := state.DeviceHealth{PoolName: testPool, DeviceName: "timeoutDevice", Health: state.DeviceHealthStatusUnknown, HealthCheckTimeout: DefaultHealthTimeout}
	expectedChanged6 := []state.DeviceHealth{expectedTimeoutDeviceUnknown}
	changedDevices, err = cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{})
	require.NoError(t, err)
	assertDeviceHealthElementsMatchIgnoreTime(t, expectedChanged6, changedDevices)

	driverState := (*cache.HealthInfo)[testDriver]
	_, exists := driverState.Devices[testPool+"/timeoutDevice"]
	assert.False(t, exists, "stale device entry should be removed from the cache after the Unknown notification; a missing entry reads as Unknown")
	assert.Equal(t, state.DeviceHealthStatusUnknown, cache.getHealthInfo(testDriver, testPool, "timeoutDevice").Health, "removed device should still read as Unknown")
}

// TestUpdateHealthInfoStaleEntryRemoval 验证驱动停止上报的条目在过期后会被移出缓存，
// 从而让缓存大小始终以驱动持续担保的设备为界。
func TestUpdateHealthInfoStaleEntryRemoval(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	cache, err := newHealthInfoCache(logger, "")
	require.NoError(t, err)

	healthyDevice := state.DeviceHealth{PoolName: testPool, DeviceName: "healthy-stale", Health: state.DeviceHealthStatusHealthy, HealthCheckTimeout: DefaultHealthTimeout}
	unknownDevice := state.DeviceHealth{PoolName: testPool, DeviceName: "unknown-stale", Health: state.DeviceHealthStatusUnknown, HealthCheckTimeout: DefaultHealthTimeout}
	freshDevice := state.DeviceHealth{PoolName: testPool, DeviceName: "fresh", Health: state.DeviceHealthStatusHealthy, HealthCheckTimeout: DefaultHealthTimeout}
	_, err = cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{healthyDevice, unknownDevice, freshDevice})
	require.NoError(t, err)

	// 把其中两个设备置为过期状态。
	err = cache.withLock(func() error {
		for _, deviceName := range []string{"healthy-stale", "unknown-stale"} {
			key := testPool + "/" + deviceName
			device := (*cache.HealthInfo)[testDriver].Devices[key]
			device.LastUpdated = time.Now().Add(-2 * DefaultHealthTimeout)
			(*cache.HealthInfo)[testDriver].Devices[key] = device
		}
		return nil
	})
	require.NoError(t, err)

	// 仅覆盖未过期设备的上报会移除过期条目。
	changedDevices, err := cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{freshDevice})
	require.NoError(t, err)

	// 只有此前健康的设备会产生到 Unknown 的状态转换；移除本来就为 Unknown 的条目对读者没有影响。
	require.Len(t, changedDevices, 1)
	assert.Equal(t, "healthy-stale", changedDevices[0].DeviceName)
	assert.Equal(t, state.DeviceHealthStatusUnknown, changedDevices[0].Health)

	devices := (*cache.HealthInfo)[testDriver].Devices
	assert.NotContains(t, devices, testPool+"/healthy-stale")
	assert.NotContains(t, devices, testPool+"/unknown-stale")
	assert.Contains(t, devices, testPool+"/fresh")
}

// TestUpdateHealthInfoDeviceCap 验证即使插件持续上报新的设备名，每个驱动的缓存也不会
// 超过 maxDevicesPerDriver 个条目，同时已知设备的更新仍会被接受。
func TestUpdateHealthInfoDeviceCap(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	cache, err := newHealthInfoCache(logger, "")
	require.NoError(t, err)

	// 把缓存填到上限，再多报一个必须被忽略的设备。
	report := make([]state.DeviceHealth, 0, maxDevicesPerDriver+1)
	for i := range maxDevicesPerDriver + 1 {
		report = append(report, state.DeviceHealth{
			PoolName:           testPool,
			DeviceName:         fmt.Sprintf("device-%d", i),
			Health:             state.DeviceHealthStatusHealthy,
			HealthCheckTimeout: DefaultHealthTimeout,
		})
	}
	changedDevices, err := cache.updateHealthInfo(logger, testDriver, report)
	require.NoError(t, err)
	assert.Len(t, changedDevices, maxDevicesPerDriver)
	assert.Len(t, (*cache.HealthInfo)[testDriver].Devices, maxDevicesPerDriver)

	// 缓存已满时，新设备名会被忽略。
	newDevice := state.DeviceHealth{PoolName: testPool, DeviceName: "one-too-many", Health: state.DeviceHealthStatusHealthy, HealthCheckTimeout: DefaultHealthTimeout}
	changedDevices, err = cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{newDevice})
	require.NoError(t, err)
	assert.Empty(t, changedDevices)
	assert.NotContains(t, (*cache.HealthInfo)[testDriver].Devices, testPool+"/one-too-many")

	// 缓存已满时，已知设备的更新仍会被接受。
	updatedDevice := state.DeviceHealth{PoolName: testPool, DeviceName: "device-0", Health: state.DeviceHealthStatusUnhealthy, HealthCheckTimeout: DefaultHealthTimeout}
	changedDevices, err = cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{updatedDevice})
	require.NoError(t, err)
	require.Len(t, changedDevices, 1)
	assert.Equal(t, state.DeviceHealthStatusUnhealthy, changedDevices[0].Health)
	assert.Equal(t, state.DeviceHealthStatusUnhealthy, cache.getHealthInfo(testDriver, testPool, "device-0").Health)
	assert.Len(t, (*cache.HealthInfo)[testDriver].Devices, maxDevicesPerDriver)
}

// TestClearDriver 测试清除某个驱动的健康数据。
func TestClearDriver(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	cache, err := newHealthInfoCache(logger, "")
	require.NoError(t, err)

	_, err = cache.updateHealthInfo(logger, testDriver, []state.DeviceHealth{testDeviceHealth})
	require.NoError(t, err)
	assert.Equal(t, state.DeviceHealthStatusHealthy, cache.getHealthInfo(testDriver, testPool, testDevice).Health)

	err = cache.clearDriver(logger, testDriver)
	require.NoError(t, err)
	assert.Equal(t, state.DeviceHealthStatusUnknown, cache.getHealthInfo(testDriver, testPool, testDevice).Health)
}
