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

package apiserver

import (
	"k8s.io/apiserver/pkg/endpoints/discovery"
	genericapiserver "k8s.io/apiserver/pkg/server"
)

// completedConfig 保存完成配置后的内部状态。
type completedConfig struct {
	Generic genericapiserver.CompletedConfig
	*Extra
}

// CompletedConfig 内嵌私有指针，保证无法在本包之外实例化。
type CompletedConfig struct {
	*completedConfig
}

// Complete 完成配置，填充发现地址与默认的 peer 端点重建间隔。
func (c *Config) Complete() CompletedConfig {
	cfg := completedConfig{
		c.Generic.Complete(c.VersionedInformers),
		&c.Extra,
	}

	// 发现地址必须使用外部可访问的地址，而非内部监听地址。
	discoveryAddresses := discovery.DefaultAddresses{DefaultAddress: cfg.Generic.ExternalAddress}
	cfg.Generic.DiscoveryAddresses = discoveryAddresses

	// 用户未显式配置时回退到默认值，避免零值导致重建过于频繁。
	if cfg.Extra.PeerEndpointReconcileInterval == 0 {
		cfg.Extra.PeerEndpointReconcileInterval = DefaultPeerEndpointReconcileInterval
	}

	return CompletedConfig{&cfg}
}
