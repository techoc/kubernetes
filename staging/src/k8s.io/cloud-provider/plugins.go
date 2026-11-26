/*
Copyright 2014 The Kubernetes Authors.

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

package cloudprovider

import (
	"fmt"
	"io"
	"os"
	"sync"

	"k8s.io/klog/v2"
)

// Factory 是返回 cloudprovider.Interface 的构造函数。config 参数向工厂提供
// 一个 io.Reader 用于加载特定配置；若未提供配置，该参数为 nil。
// 云厂商在各自包的 init 中注册 Factory，进程启动后即不可再注册。
type Factory func(config io.Reader) (Interface, error)

// providers 保存所有已注册的云提供商。
// providers 被启动阶段与运行期并发读取，必须由 providersMutex 保护。
var (
	providersMutex sync.Mutex
	providers      = make(map[string]Factory)
)

const externalCloudProvider = "external"

// RegisterCloudProvider 按名称注册一个 cloudprovider.Factory，预期在应用启动阶段完成。
// 重复注册属于编程错误，直接 Fatal 终止，避免运行时行为不确定。
func RegisterCloudProvider(name string, cloud Factory) {
	providersMutex.Lock()
	defer providersMutex.Unlock()
	if _, found := providers[name]; found {
		klog.Fatalf("Cloud provider %q was registered twice", name)
	}
	klog.V(1).Infof("Registered cloud provider %q", name)
	providers[name] = cloud
}

// IsCloudProvider 在 name 对应某个已注册的云提供商时返回 true。
// 只查询注册表，不校验 Factory 是否可成功初始化。
func IsCloudProvider(name string) bool {
	providersMutex.Lock()
	defer providersMutex.Unlock()
	_, found := providers[name]
	return found
}

// GetCloudProvider 创建指定名称的云提供商实例；名称未知时返回 nil。
// 错误返回值仅用于"名称已知但初始化失败"的情形。config 参数指向云提供商
// 配置文件的 io.Reader，无配置时为 nil。
// 未知 provider 返回 (nil, nil)，调用方需自行把 nil 实例视为“未注册”。
func GetCloudProvider(name string, config io.Reader) (Interface, error) {
	providersMutex.Lock()
	defer providersMutex.Unlock()
	f, found := providers[name]
	if !found {
		return nil, nil
	}
	return f(config)
}

// IsExternal 判断给定字符串是否为外部云提供商。
// "external" 表示云逻辑由进程外的 cloud-controller-manager 提供，本进程不初始化任何实现。
func IsExternal(name string) bool {
	return name == externalCloudProvider
}

// DisableWarningForProvider 记录云提供商被禁用的相关信息。
// 对非 external provider 输出迁移提示：内置云提供商已被移除，需改用外部 CCM。
func DisableWarningForProvider(providerName string) {
	if !IsExternal(providerName) {
		klog.Infof("INFO: Please make sure you are running an external cloud controller manager binary for provider %q."+
			"In-tree cloud providers are disabled. Refer to https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/cloud-provider/sample "+
			"for an example implementation.", providerName)
		klog.Warningf("WARNING: built-in cloud providers are disabled. Please set \"--cloud-provider=external\" and migrate to an external cloud controller manager for provider %q", providerName)
	}
}

// ErrorForDisabledProvider 返回按给定 provider 名称格式化的错误。
// 返回可操作的错误信息，明确指引用户设置 --cloud-provider=external。
func ErrorForDisabledProvider(providerName string) error {
	return fmt.Errorf("cloud provider %q was specified, but built-in cloud providers are disabled. Please set --cloud-provider=external and migrate to an external cloud provider", providerName)
}

// InitCloudProvider 创建指定名称的云提供商实例。
// 这是启动路径上唯一的初始化入口：负责打开配置文件、调用 Factory 并做基本校验。
func InitCloudProvider(name string, configFilePath string) (Interface, error) {
	var cloud Interface
	var err error

	// 空名称表示不使用云提供商，返回 nil 而非错误。
	if name == "" {
		return nil, nil
	}

	// external 模式下本进程不创建实现，由进程外 CCM 负责。
	if IsExternal(name) {
		klog.Info("External cloud provider specified")
		return nil, nil
	}

	if configFilePath != "" {
		// 配置文件由 Factory 自行解析，这里只负责打开并在返回前关闭。
		var config *os.File
		config, err = os.Open(configFilePath)
		if err != nil {
			klog.Fatalf("Couldn't open cloud provider configuration %s: %#v",
				configFilePath, err)
		}

		defer config.Close()
		cloud, err = GetCloudProvider(name, config)
	} else {
		// 显式传入 nil，插件才能用 == nil 判断；参见 golang.org/doc/faq 中
		// "Why is my nil error value not equal to nil?"。
		cloud, err = GetCloudProvider(name, nil)
	}

	// Factory 返回的初始化错误需要保留 provider 名称，方便定位配置问题。
	if err != nil {
		return nil, fmt.Errorf("could not init cloud provider %q: %v", name, err)
	}
	// 已知名称却返回 nil 说明 Factory 实现有误，按未知 provider 处理。
	if cloud == nil {
		return nil, fmt.Errorf("unknown cloud provider %q", name)
	}

	return cloud, nil
}
