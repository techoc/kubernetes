/*
Copyright 2018 The Kubernetes Authors.

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

package genericclioptions

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// TestConfigFlags 持有 clientConfig 及各 RESTClientGetter 相关接口的测试替身。
//
// TestConfigFlags 是 ConfigFlags 的测试替身：它不注册任何真实 flag，
// 而是由测试通过 With* 方法注入 clientConfig/discoveryClient/RESTMapper。
type TestConfigFlags struct {
	clientConfig    clientcmd.ClientConfig
	discoveryClient discovery.CachedDiscoveryInterface
	restMapper      meta.RESTMapper
}

// ToRawKubeConfigLoader 实现 RESTClientGetter；已设置时返回对应的 clientConfig。
//
// 未注入 clientConfig 时直接 panic，而不是返回零值：测试里这属于使用错误，
// 尽早暴露比让后续逻辑静默走到空命名空间更好。
func (f *TestConfigFlags) ToRawKubeConfigLoader() clientcmd.ClientConfig {
	if f.clientConfig == nil {
		panic("attempt to obtain a test RawKubeConfigLoader with no clientConfig specified")
	}
	return f.clientConfig
}

// ToRESTConfig 实现 RESTClientGetter。
// 依据 kubeconfig 路径、加载规则和标志覆盖返回 REST 配置；
// 调用前须先调用 AddFlags。
func (f *TestConfigFlags) ToRESTConfig() (*rest.Config, error) {
	return f.ToRawKubeConfigLoader().ClientConfig()
}

// ToDiscoveryClient 实现 RESTClientGetter，返回缓存的发现客户端。
func (f *TestConfigFlags) ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	// 未注入时返回 (nil, nil) 而非报错，调用方需自行判断是否为 nil；
	// 这与真实实现不同，测试中要避免依赖发现能力的路径。
	return f.discoveryClient, nil
}

// ToRESTMapper 实现 RESTClientGetter，返回 RESTMapper。
//
// 优先使用显式注入的 RESTMapper；否则用 discoveryClient 构造延迟发现 + 短名展开的 mapper。
func (f *TestConfigFlags) ToRESTMapper() (meta.RESTMapper, error) {
	if f.restMapper != nil {
		return f.restMapper, nil
	}
	if f.discoveryClient != nil {
		mapper := restmapper.NewDeferredDiscoveryRESTMapper(f.discoveryClient)
		expander := restmapper.NewShortcutExpander(mapper, f.discoveryClient, nil)
		return expander, nil
	}
	return nil, fmt.Errorf("no restmapper")
}

// WithClientConfig 设置 clientConfig 标志。
func (f *TestConfigFlags) WithClientConfig(clientConfig clientcmd.ClientConfig) *TestConfigFlags {
	f.clientConfig = clientConfig
	return f
}

// WithRESTMapper 设置 restMapper 标志。
func (f *TestConfigFlags) WithRESTMapper(mapper meta.RESTMapper) *TestConfigFlags {
	f.restMapper = mapper
	return f
}

// WithDiscoveryClient 设置 discoveryClient 标志。
func (f *TestConfigFlags) WithDiscoveryClient(c discovery.CachedDiscoveryInterface) *TestConfigFlags {
	f.discoveryClient = c
	return f
}

// WithNamespace 通过修改 delegate 和 namespace 来设置 clientConfig。
//
// 用包装器覆盖命名空间而不改动原始 clientConfig，便于在同一配置上跑多组用例。
// 若尚未设置 clientConfig 则 panic，因为无委托对象可包装。
func (f *TestConfigFlags) WithNamespace(ns string) *TestConfigFlags {
	if f.clientConfig == nil {
		panic("attempt to obtain a test RawKubeConfigLoader with no clientConfig specified")
	}
	f.clientConfig = &namespacedClientConfig{
		delegate:  f.clientConfig,
		namespace: ns,
	}
	return f
}

// NewTestConfigFlags 构造用于测试 ConfigFlags 的 TestConfigFlags。
func NewTestConfigFlags() *TestConfigFlags {
	return &TestConfigFlags{}
}

// namespacedClientConfig 只覆盖 Namespace()，其余方法全部委托给底层配置。
type namespacedClientConfig struct {
	delegate  clientcmd.ClientConfig
	namespace string
}

// Namespace 返回被覆盖的命名空间；第二个返回值表示命名空间是否被显式指定
// （长度大于 0），这会影响调用方是否回退到 kubeconfig 的默认命名空间。
func (c *namespacedClientConfig) Namespace() (string, bool, error) {
	return c.namespace, len(c.namespace) > 0, nil
}

func (c *namespacedClientConfig) RawConfig() (clientcmdapi.Config, error) {
	return c.delegate.RawConfig()
}
func (c *namespacedClientConfig) ClientConfig() (*rest.Config, error) {
	return c.delegate.ClientConfig()
}
func (c *namespacedClientConfig) ConfigAccess() clientcmd.ConfigAccess {
	return c.delegate.ConfigAccess()
}
