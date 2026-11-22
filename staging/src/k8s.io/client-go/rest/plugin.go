/*
Copyright 2016 The Kubernetes Authors.

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

package rest

import (
	"fmt"
	"net/http"
	"sync"

	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// AuthProvider 是云厂商认证插件的统一抽象。
// 插件在进程启动时通过 RegisterAuthProviderPlugin 注册，之后由 REST 客户端按名称实例化。
type AuthProvider interface {
	// WrapTransport allows the plugin to create a modified RoundTripper that
	// attaches authorization headers (or other info) to requests.
	WrapTransport(http.RoundTripper) http.RoundTripper
	// Login allows the plugin to initialize its configuration. It must not
	// require direct user interaction.
	Login() error
}

// Factory generates an AuthProvider plugin.
//
//	clusterAddress is the address of the current cluster.
//	config is the initial configuration for this plugin.
//	persister allows the plugin to save updated configuration.
//
// Factory 在每次构建客户端时被调用，因此不应在其中做阻塞式的交互登录。
type Factory func(clusterAddress string, config map[string]string, persister AuthProviderConfigPersister) (AuthProvider, error)

// AuthProviderConfigPersister allows a plugin to persist configuration info
// for just itself.
//
// 插件只能写入自己的配置节，避免互相覆盖 kubeconfig 中的其他内容。
type AuthProviderConfigPersister interface {
	Persist(map[string]string) error
}

// noopPersister 用于没有持久化需求的调用方（例如 kubeconfig 为只读时）。
type noopPersister struct{}

func (n *noopPersister) Persist(_ map[string]string) error {
	// no operation persister
	return nil
}

// All registered auth provider plugins.
//
// 注册表由 pluginsLock 保护：注册发生在 init 阶段，读取可能来自多个并发构建的客户端。
var pluginsLock sync.Mutex
var plugins = make(map[string]Factory)

// RegisterAuthProviderPlugin 注册一个认证插件，重名会返回错误而不是静默覆盖。
//
// 典型调用发生在插件包的 init() 中，因此这里刻意不打印日志（日志系统可能尚未初始化）。
func RegisterAuthProviderPlugin(name string, plugin Factory) error {
	pluginsLock.Lock()
	defer pluginsLock.Unlock()
	if _, found := plugins[name]; found {
		return fmt.Errorf("auth Provider Plugin %q was registered twice", name)
	}
	// RegisterAuthProviderPlugin gets called during the init phase before
	// logging is initialized and therefore should not emit logs. If you
	// need this message for debugging something, then uncomment it.
	// klog.V(4).Infof("Registered Auth Provider Plugin %q", name)
	plugins[name] = plugin
	return nil
}

// GetAuthProvider 依据 AuthProviderConfig.Name 查表并实例化插件。
// persister 为 nil 时退化为空实现，保证插件可以无条件调用 Persist。
func GetAuthProvider(clusterAddress string, apc *clientcmdapi.AuthProviderConfig, persister AuthProviderConfigPersister) (AuthProvider, error) {
	pluginsLock.Lock()
	defer pluginsLock.Unlock()
	p, ok := plugins[apc.Name]
	if !ok {
		return nil, fmt.Errorf("no Auth Provider found for name %q", apc.Name)
	}
	if persister == nil {
		persister = &noopPersister{}
	}
	return p(clusterAddress, apc.Config, persister)
}
