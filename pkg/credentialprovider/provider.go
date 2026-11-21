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

package credentialprovider

import (
	"os"
	"reflect"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// DockerConfigProvider 是已注册扩展需要实现的接口，用于生成 dockercfg 凭证。
type DockerConfigProvider interface {
	// Enabled 报告凭证提供者是否可用；实现可能阻塞，例如元数据服务不可达。
	Enabled() bool
	// Provide 返回 Docker 凭证配置；实现可能阻塞。
	// 镜像名作为参数供需要按镜像决定凭证的实现使用，忽略它也是安全的。
	Provide(image string) DockerConfig
}

// defaultDockerConfigProvider 直接读取 .dockercfg 文件。
type defaultDockerConfigProvider struct{}

// CachingDockerConfigProvider 包装另一个提供者，并在指定时长内缓存其凭证。
type CachingDockerConfigProvider struct {
	Provider DockerConfigProvider
	Lifetime time.Duration

	// ShouldCache 决定某份配置是否可缓存；为 nil 时全部缓存。
	ShouldCache func(DockerConfig) bool

	// 缓存状态。
	cacheDockerConfig DockerConfig
	expiration        time.Time
	mu                sync.Mutex
}

// Enabled 实现 DockerConfigProvider 接口。
func (d *defaultDockerConfigProvider) Enabled() bool {
	return true
}

// Provide 实现 DockerConfigProvider 接口。
func (d *defaultDockerConfigProvider) Provide(image string) DockerConfig {
	// 标准 Docker 凭证来自 .dockercfg。
	if cfg, err := ReadDockerConfigFile(); err == nil {
		return cfg
	} else if !os.IsNotExist(err) {
		klog.V(2).Infof("Docker config file not found: %v", err)
	}
	return DockerConfig{}
}

// Enabled 实现 DockerConfigProvider 接口。
func (d *CachingDockerConfigProvider) Enabled() bool {
	return d.Provider.Enabled()
}

// Provide 实现 DockerConfigProvider 接口。
func (d *CachingDockerConfigProvider) Provide(image string) DockerConfig {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 缓存未过期时直接返回，避免重复查询底层提供者。
	if time.Now().Before(d.expiration) {
		return d.cacheDockerConfig
	}

	klog.V(2).Infof("Refreshing cache for provider: %v", reflect.TypeOf(d.Provider).String())
	config := d.Provider.Provide(image)
	if d.ShouldCache == nil || d.ShouldCache(config) {
		d.cacheDockerConfig = config
		d.expiration = time.Now().Add(d.Lifetime)
	}
	return config
}

// NewDefaultDockerKeyring 创建使用 .dockercfg 中默认凭证的 keyring。
func NewDefaultDockerKeyring() DockerKeyring {
	return &providersDockerKeyring{
		Providers: []DockerConfigProvider{
			&CachingDockerConfigProvider{
				Provider: &defaultDockerConfigProvider{},
				Lifetime: 5 * time.Minute,
			},
		},
	}
}
