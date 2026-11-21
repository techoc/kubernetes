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

package plugin

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/credentialprovider"
	"k8s.io/kubernetes/pkg/features"
)

// dockerConfigProviderWithCoordinates 在返回凭证的同时给出来源的 ServiceAccount 坐标。
type dockerConfigProviderWithCoordinates interface {
	// provideWithCoordinates 返回镜像的 Docker 配置及 ServiceAccount 坐标。
	provideWithCoordinates(image string) (credentialprovider.DockerConfig, *credentialprovider.ServiceAccountCoordinates)
}

// provider 把插件名与其实现关联起来。
type provider struct {
	name string
	impl *pluginProvider
}

var providersMutex sync.RWMutex
var providers = make([]provider, 0)
var seenProviderNames = sets.NewString()

// registerCredentialProviderPlugin 注册插件实现；重名注册直接 panic。
func registerCredentialProviderPlugin(name string, p *pluginProvider) {
	providersMutex.Lock()
	defer providersMutex.Unlock()

	// 重名意味着配置冲突，继续运行会导致凭证来源不明。
	if seenProviderNames.Has(name) {
		panic(fmt.Sprintf("Credential provider %q was registered twice", name))
	}
	seenProviderNames.Insert(name)

	providers = append(providers, provider{name, p})
	klog.V(4).InfoS("Registered credential provider", "provider", name)
}

// externalCredentialProviderKeyring 汇总所有外部凭证插件。
type externalCredentialProviderKeyring struct {
	providers []dockerConfigProviderWithCoordinates
}

// NewExternalCredentialProviderDockerKeyring 为指定 Pod 创建外部插件 keyring。
func NewExternalCredentialProviderDockerKeyring(podNamespace, podName, podUID, serviceAccountName string) credentialprovider.DockerKeyring {
	providersMutex.RLock()
	defer providersMutex.RUnlock()

	keyring := &externalCredentialProviderKeyring{
		providers: make([]dockerConfigProviderWithCoordinates, 0, len(providers)),
	}

	// 每个 Pod 都生成独立的 provider，以便按 Pod 的 ServiceAccount 生成绑定令牌。
	for _, p := range providers {
		pp := &perPodPluginProvider{
			provider: p.impl,
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.KubeletServiceAccountTokenForCredentialProviders) {
			klog.V(4).InfoS("Generating per pod credential provider", "provider", p.name, "podName", podName, "podNamespace", podNamespace, "podUID", podUID, "serviceAccountName", serviceAccountName)

			pp.podNamespace = podNamespace
			pp.podName = podName
			pp.podUID = types.UID(podUID)
			pp.serviceAccountName = serviceAccountName
		} else {
			klog.V(4).InfoS("Generating credential provider", "provider", p.name)
		}

		keyring.providers = append(keyring.providers, pp)
	}

	return keyring
}

// Lookup 逐个调用外部插件取凭证，再交由 BasicDockerKeyring 匹配镜像。
func (k *externalCredentialProviderKeyring) Lookup(image string) ([]credentialprovider.TrackedAuthConfig, bool) {
	keyring := &credentialprovider.BasicDockerKeyring{}

	for _, p := range k.providers {
		dockerConfig, saCoords := p.provideWithCoordinates(image)
		// SA 坐标用于标记凭证来源，供后续 Secret/SA 校验使用。
		if saCoords != nil {
			keyring.Add(&credentialprovider.CredentialSource{ServiceAccount: saCoords}, dockerConfig)
		} else {
			keyring.Add(nil, dockerConfig)
		}
	}

	return keyring.Lookup(image)
}
