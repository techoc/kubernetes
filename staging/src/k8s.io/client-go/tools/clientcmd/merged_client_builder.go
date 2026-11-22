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

// DeferredLoadingClientConfig is a ClientConfig interface that is backed by a client config loader.
// It is used in cases where the loading rules may change after you've instantiated them and you want to be sure that
// the most recent rules are used.  This is useful in cases where you bind flags to loading rule parameters before
// the parse happens and you want your calling code to be ignorant of how the values are being mutated to avoid
// passing extraneous information down a call stack
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

	// provided for testing
	icc InClusterConfig
}

// InClusterConfig abstracts details of whether the client is running in a cluster for testing.
type InClusterConfig interface {
	ClientConfig
	Possible() bool
}

// NewNonInteractiveDeferredLoadingClientConfig creates a ClientConfig using the passed context name
// 不允许交互式输入凭据，适用于非终端场景（如 daemon）。
func NewNonInteractiveDeferredLoadingClientConfig(loader ClientConfigLoader, overrides *ConfigOverrides) ClientConfig {
	return &DeferredLoadingClientConfig{loader: loader, overrides: overrides, icc: &inClusterClientConfig{overrides: overrides}}
}

// NewInteractiveDeferredLoadingClientConfig creates a ClientConfig using the passed context name and the fallback auth reader
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

// ClientConfig implements ClientConfig
func (config *DeferredLoadingClientConfig) ClientConfig() (*restclient.Config, error) {
	mergedClientConfig, err := config.createClientConfig()
	if err != nil {
		return nil, err
	}

	// load the configuration and return on non-empty errors and if the
	// content differs from the default config
	mergedConfig, err := mergedClientConfig.ClientConfig()
	switch {
	case err != nil:
		if !IsEmptyConfig(err) {
			// return on any error except empty config
			return nil, err
		}
	case mergedConfig != nil:
		// the configuration is valid, but if this is equal to the defaults we should try
		// in-cluster configuration
		// 仅“空配置”错误不致命，继续尝试集群内配置。
		if !config.loader.IsDefaultConfig(mergedConfig) {
			return mergedConfig, nil
		}
	}

	// check for in-cluster configuration and use it
	// 在 Pod 中运行时，ServiceAccount 挂载的配置优先级高于 kubeconfig 默认值。
	if config.icc.Possible() {
		//nolint:logcheck // A helper function like this should not log. But this is probably part of the the established client-go API and not worth changing.
		klog.V(4).Infof("Using in-cluster configuration")
		return config.icc.ClientConfig()
	}

	// return the result of the merged client config
	return mergedConfig, err
}

// Namespace implements KubeConfig
func (config *DeferredLoadingClientConfig) Namespace() (string, bool, error) {
	mergedKubeConfig, err := config.createClientConfig()
	if err != nil {
		return "", false, err
	}

	ns, overridden, err := mergedKubeConfig.Namespace()
	// if we get an error and it is not empty config, or if the merged config defined an explicit namespace, or
	// if in-cluster config is not possible, return immediately
	if (err != nil && !IsEmptyConfig(err)) || overridden || !config.icc.Possible() {
		// return on any error except empty config
		return ns, overridden, err
	}

	if len(ns) > 0 {
		// if we got a non-default namespace from the kubeconfig, use it
		if ns != "default" {
			return ns, false, nil
		}

		// if we got a default namespace, determine whether it was explicit or implicit
		// “default”既可能是用户显式写的，也可能是缺省值；只有显式指定才应压过
		// ServiceAccount 命名空间。
		if raw, err := mergedKubeConfig.RawConfig(); err == nil {
			// determine the current context
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

	// allow the namespace from the service account token directory to be used.
	return config.icc.Namespace()
}

// ConfigAccess implements ClientConfig
func (config *DeferredLoadingClientConfig) ConfigAccess() ConfigAccess {
	return config.loader
}
