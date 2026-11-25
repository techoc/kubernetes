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

package clientcmd

import (
	"io"
	"sync"

	"k8s.io/klog/v2"

	restclient "k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// DeferredLoadingClientConfig 是由客户端配置加载器支撑的 ClientConfig 接口实现。
// 它适用于加载规则在实例化之后仍可能变化、且需要确保使用最新规则的场景：
// 例如在解析发生之前把 flag 绑定到加载规则参数上，同时让调用方无需关心
// 值如何被修改，避免沿调用栈传递多余信息。
//
// 关键语义是“延迟”：构造时不读取任何文件，直到首次调用 ClientConfig/Namespace/RawConfig
// 才按当时的 loader 规则加载，并在进程内缓存结果。
type DeferredLoadingClientConfig struct {
	loader         ClientConfigLoader
	overrides      *ConfigOverrides
	fallbackReader io.Reader

	// clientConfig 是首次加载后的缓存；loadingLock 保证并发首调只加载一次。
	clientConfig ClientConfig
	loadingLock  sync.Mutex

	// 供测试使用
	icc InClusterConfig
}

// InClusterConfig 抽象出“客户端是否运行在集群内”的细节，供测试使用。
type InClusterConfig interface {
	ClientConfig
	Possible() bool
}

// NewNonInteractiveDeferredLoadingClientConfig 使用所传入的上下文名称创建 ClientConfig。
// 不允许交互式输入凭据，适用于非终端场景（如 daemon）。
func NewNonInteractiveDeferredLoadingClientConfig(loader ClientConfigLoader, overrides *ConfigOverrides) ClientConfig {
	return &DeferredLoadingClientConfig{loader: loader, overrides: overrides, icc: &inClusterClientConfig{overrides: overrides}}
}

// NewInteractiveDeferredLoadingClientConfig 使用所传入的上下文名称和回退认证读取器创建 ClientConfig。
// fallbackReader 用于在需要时向用户索取凭据（如证书口令或登录确认）。
func NewInteractiveDeferredLoadingClientConfig(loader ClientConfigLoader, overrides *ConfigOverrides, fallbackReader io.Reader) ClientConfig {
	return &DeferredLoadingClientConfig{loader: loader, overrides: overrides, icc: &inClusterClientConfig{overrides: overrides}, fallbackReader: fallbackReader}
}

// createClientConfig 惰性构建并缓存底层 ClientConfig；双检锁避免重复加载文件。
func (config *DeferredLoadingClientConfig) createClientConfig() (ClientConfig, error) {
	config.loadingLock.Lock()
	defer config.loadingLock.Unlock()

	if config.clientConfig != nil {
		return config.clientConfig, nil
	}
	mergedConfig, err := config.loader.Load()
	if err != nil {
		return nil, err
	}

	// 覆盖项中的 CurrentContext 优先于 kubeconfig 自身的 current-context。
	var currentContext string
	if config.overrides != nil {
		currentContext = config.overrides.CurrentContext
	}
	if config.fallbackReader != nil {
		config.clientConfig = NewInteractiveClientConfig(*mergedConfig, currentContext, config.overrides, config.fallbackReader, config.loader)
	} else {
		config.clientConfig = NewNonInteractiveClientConfig(*mergedConfig, currentContext, config.overrides, config.loader)
	}
	return config.clientConfig, nil
}

func (config *DeferredLoadingClientConfig) RawConfig() (clientcmdapi.Config, error) {
	mergedConfig, err := config.createClientConfig()
	if err != nil {
		return clientcmdapi.Config{}, err
	}

	return mergedConfig.RawConfig()
}

// ClientConfig 实现 ClientConfig 接口
func (config *DeferredLoadingClientConfig) ClientConfig() (*restclient.Config, error) {
	mergedClientConfig, err := config.createClientConfig()
	if err != nil {
		return nil, err
	}

	// 加载配置；只要错误非“空配置”、或内容不同于默认配置，就直接返回。
	mergedConfig, err := mergedClientConfig.ClientConfig()
	switch {
	case err != nil:
		if !IsEmptyConfig(err) {
			// 除空配置错误外，任何错误都直接返回
			return nil, err
		}
	case mergedConfig != nil:
		// 配置有效，但如果它与默认值相同，还应继续尝试集群内配置；
		// 仅“空配置”错误不致命，继续尝试集群内配置。
		if !config.loader.IsDefaultConfig(mergedConfig) {
			return mergedConfig, nil
		}
	}

	// 检查并使用集群内配置
	// 在 Pod 中运行时，ServiceAccount 挂载的配置优先级高于 kubeconfig 默认值。
	if config.icc.Possible() {
		//nolint:logcheck // A helper function like this should not log. But this is probably part of the the established client-go API and not worth changing.
		klog.V(4).Infof("Using in-cluster configuration")
		return config.icc.ClientConfig()
	}

	// 返回合并后的客户端配置结果
	return mergedConfig, err
}

// Namespace 实现 KubeConfig 接口
func (config *DeferredLoadingClientConfig) Namespace() (string, bool, error) {
	mergedKubeConfig, err := config.createClientConfig()
	if err != nil {
		return "", false, err
	}

	ns, overridden, err := mergedKubeConfig.Namespace()
	// 若出错且不是空配置错误、或合并配置显式定义了命名空间、或无法使用集群内配置，
	// 则立即返回
	if (err != nil && !IsEmptyConfig(err)) || overridden || !config.icc.Possible() {
		// 除空配置错误外，任何错误都直接返回
		return ns, overridden, err
	}

	if len(ns) > 0 {
		// 如果从 kubeconfig 拿到了非默认命名空间，直接使用
		if ns != "default" {
			return ns, false, nil
		}

		// 如果拿到的是默认命名空间，判断它是显式配置还是隐式缺省
		// “default”既可能是用户显式写的，也可能是缺省值；只有显式指定才应压过
		// ServiceAccount 命名空间。
		if raw, err := mergedKubeConfig.RawConfig(); err == nil {
			// 确定当前上下文
			currentContext := raw.CurrentContext
			if config.overrides != nil && len(config.overrides.CurrentContext) > 0 {
				currentContext = config.overrides.CurrentContext
			}
			if context := raw.Contexts[currentContext]; context != nil && len(context.Namespace) > 0 {
				return ns, false, nil
			}
		}
	}

	//nolint:logcheck // A helper function like this should not log. But this is probably part of the the established client-go API and not worth changing.
	klog.V(4).Infof("Using in-cluster namespace")

	// 允许使用 ServiceAccount token 目录中的命名空间。
	return config.icc.Namespace()
}

// ConfigAccess 实现 ClientConfig 接口
func (config *DeferredLoadingClientConfig) ConfigAccess() ConfigAccess {
	return config.loader
}
