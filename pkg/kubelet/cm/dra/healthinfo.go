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

package dra

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/dra/state"
)

const (
	// DefaultHealthTimeout 是设备健康检查的默认超时时间，插件未指定时使用。
	DefaultHealthTimeout = 30 * time.Second

	// maxDevicesPerDriver 限制缓存为每个驱动保留的设备健康条目数。健康上报不会与
	// ResourceSlices 校验，若不设上限，行为异常的插件可以不断编造新设备名，
	// 使缓存（以及每次变更都写出的 checkpoint 文件）无限增长。该值远高于
	// 单节点真实设备数——即使是大型 SR-IOV 或分区 GPU 场景也只有数千个。
	// 超过上限的新设备会被忽略并记录错误日志；已存在设备的条目始终允许更新。
	maxDevicesPerDriver = 16384
)

// healthInfoCache 是已知设备健康状态的缓存。
type healthInfoCache struct {
	sync.RWMutex
	HealthInfo *state.DevicesHealthMap
	stateFile  string
}

// newHealthInfoCache 创建新的缓存，并在存在 checkpoint 时从中加载。
func newHealthInfoCache(logger klog.Logger, stateFile string) (*healthInfoCache, error) {
	logger = logger.WithName("dra-healthinfo")
	cache := &healthInfoCache{
		HealthInfo: &state.DevicesHealthMap{},
		stateFile:  stateFile,
	}
	// checkpoint 加载失败不阻塞启动，使用空缓存继续运行。
	if err := cache.loadFromCheckpoint(); err != nil {
		logger.Error(err, "Failed to load health checkpoint, proceeding with empty cache")
	}
	return cache, nil
}

// loadFromCheckpoint 从状态文件加载缓存。
func (cache *healthInfoCache) loadFromCheckpoint() error {
	// 未配置状态文件时无需持久化。
	if cache.stateFile == "" {
		return nil
	}
	data, err := os.ReadFile(cache.stateFile)
	if err != nil {
		// 文件不存在说明首次启动，初始化为空映射。
		if os.IsNotExist(err) {
			cache.HealthInfo = &state.DevicesHealthMap{}
			return nil
		}
		return err
	}
	return json.Unmarshal(data, cache.HealthInfo)
}

// withLock 在持有 healthInfoCache 写锁的情况下执行给定函数。
func (cache *healthInfoCache) withLock(f func() error) error {
	cache.Lock()
	defer cache.Unlock()
	return f()
}

// withRLock 在持有 healthInfoCache 读锁的情况下执行给定函数。
func (cache *healthInfoCache) withRLock(f func() error) error {
	cache.RLock()
	defer cache.RUnlock()
	return f()
}

// saveToCheckpointInternal 在不加锁的情况下执行实际保存，
// 调用方必须已持有必要的锁。
func (cache *healthInfoCache) saveToCheckpointInternal(logger klog.Logger) error {
	logger = logger.WithName("dra-healthinfo")
	if cache.stateFile == "" {
		return nil
	}
	data, err := json.Marshal(cache.HealthInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal health info: %w", err)
	}

	// 先写临时文件再原子重命名，避免进程崩溃时留下半截状态文件。
	tempFile, err := os.CreateTemp(filepath.Dir(cache.stateFile), filepath.Base(cache.stateFile)+".tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp checkpoint file: %w", err)
	}

	// 无论成功与否都清理临时文件，避免残留。
	defer func() {
		if err := os.Remove(tempFile.Name()); err != nil && !os.IsNotExist(err) {
			logger.Error(err, "Failed to remove temporary checkpoint file", "path", tempFile.Name())
		}
	}()

	if _, err := tempFile.Write(data); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("failed to write to temporary file: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}

	// 重命名是原子操作：读者要么看到旧文件，要么看到完整的新文件。
	if err := os.Rename(tempFile.Name(), cache.stateFile); err != nil {
		return fmt.Errorf("failed to rename temporary file to state file: %w", err)
	}

	return nil
}

// getHealthInfo 返回当前健康信息，并按超时规则调整结果。
func (cache *healthInfoCache) getHealthInfo(driverName, poolName, deviceName string) state.DeviceHealth {
	// 未上报过的设备默认返回 Unknown 状态。
	res := state.DeviceHealth{
		PoolName:   poolName,
		DeviceName: deviceName,
		Health:     state.DeviceHealthStatusUnknown,
		Message:    "",
	}

	_ = cache.withRLock(func() error {
		now := time.Now()
		if driver, ok := (*cache.HealthInfo)[driverName]; ok {
			key := poolName + "/" + deviceName
			if device, ok := driver.Devices[key]; ok {
				// 优先使用设备自定义的健康检查超时，否则使用默认值。
				timeout := device.HealthCheckTimeout
				if timeout == 0 {
					timeout = DefaultHealthTimeout
				}

				// 超过超时时间未更新时，健康状态视为未知。
				if now.Sub(device.LastUpdated) > timeout {
					// 保留默认的 Unknown 状态，并清空过期设备的消息。
					res.Health = state.DeviceHealthStatusUnknown
					res.Message = ""
				} else {
					res = device
				}
			}
		}
		return nil
	})
	return res
}

// updateHealthInfo 用插件上报的最新设备健康列表对账缓存：识别状态发生变化的设备，
// 并处理插件不再上报的设备。
func (cache *healthInfoCache) updateHealthInfo(logger klog.Logger, driverName string, devices []state.DeviceHealth) ([]state.DeviceHealth, error) {
	logger = logger.WithName("dra-healthinfo")
	changedDevices := []state.DeviceHealth{}
	err := cache.withLock(func() error {
		now := time.Now()
		currentDriver, exists := (*cache.HealthInfo)[driverName]
		if !exists {
			currentDriver = state.DriverHealthState{Devices: make(map[string]state.DeviceHealth)}
			(*cache.HealthInfo)[driverName] = currentDriver
		}

		reportedKeys := make(map[string]struct{})

		// 阶段一：处理插件本次上报。更新已有设备、加入新设备，
		// 并记录本次上报中出现的全部设备。
		ignoredDevices := 0
		for _, reportedDevice := range devices {
			// 收到上报时刷新 LastUpdated，作为超时判断的基准。
			reportedDevice.LastUpdated = now
			key := reportedDevice.PoolName + "/" + reportedDevice.DeviceName
			reportedKeys[key] = struct{}{}

			existingDevice, ok := currentDriver.Devices[key]

			// 超过每驱动上限的新设备会被忽略；已存在设备的更新始终接受。
			if !ok && len(currentDriver.Devices) >= maxDevicesPerDriver {
				ignoredDevices++
				continue
			}

			// 健康状态、消息或超时时间任一变化都视为一次更新。
			if !ok || existingDevice.Health != reportedDevice.Health || existingDevice.Message != reportedDevice.Message || existingDevice.HealthCheckTimeout != reportedDevice.HealthCheckTimeout {
				changedDevices = append(changedDevices, reportedDevice)
			}

			currentDriver.Devices[key] = reportedDevice
		}
		if ignoredDevices > 0 {
			logger.Error(nil, "Health report contains more devices than the cache keeps per driver, ignoring new devices beyond the limit", "driverName", driverName, "limit", maxDevicesPerDriver, "ignoredDevices", ignoredDevices)
		}

		// 阶段二：处理缓存中存在但本次未上报的设备。这些设备可能已被移除，
		// 或插件已停止监控。一旦其状态超时，就发出转为 "Unknown" 的通知并删除条目。
		// 缓存中不存在的设备本来就按 "Unknown" 读取（见 getHealthInfo），
		// 因此删除条目等价于永久保持 "Unknown"，同时让缓存规模受驱动持续上报的范围约束。
		deletedDevices := false
		for key, existingDevice := range currentDriver.Devices {
			if _, wasReported := reportedKeys[key]; !wasReported {
				// 优先使用设备自定义的健康检查超时，否则使用默认值。
				timeout := existingDevice.HealthCheckTimeout
				if timeout == 0 {
					timeout = DefaultHealthTimeout
				}

				if now.Sub(existingDevice.LastUpdated) > timeout {
					// 已经是 "Unknown" 的条目直接删除，无需通知，因为对读者而言没有变化。
					if existingDevice.Health != state.DeviceHealthStatusUnknown {
						existingDevice.Health = state.DeviceHealthStatusUnknown
						existingDevice.Message = ""
						existingDevice.LastUpdated = now
						changedDevices = append(changedDevices, existingDevice)
					}
					delete(currentDriver.Devices, key)
					deletedDevices = true
				}
			}
		}

		// 阶段三：只要状态有变化就持久化到 checkpoint 文件。
		if len(changedDevices) > 0 || deletedDevices {
			if err := cache.saveToCheckpointInternal(logger); err != nil {
				logger.Error(err, "Failed to save health checkpoint after update. Kubelet restart may lose the device health information.")
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return changedDevices, nil
}

// clearDriver 清除指定驱动的全部健康数据。
func (cache *healthInfoCache) clearDriver(logger klog.Logger, driverName string) error {
	return cache.withLock(func() error {
		// 整个驱动从缓存中移除，并立即持久化。
		delete(*cache.HealthInfo, driverName)
		return cache.saveToCheckpointInternal(logger)
	})
}
