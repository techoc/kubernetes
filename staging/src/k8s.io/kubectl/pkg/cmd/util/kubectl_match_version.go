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

package util

import (
	"sync"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubectl/pkg/scheme"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/component-base/version"
)

const (
	flagMatchBinaryVersion = "match-server-version"
)

// MatchVersionFlags 用于设置"匹配服务端版本"的功能。
type MatchVersionFlags struct {
	Delegate genericclioptions.RESTClientGetter

	RequireMatchedServerVersion bool
	checkServerVersion          sync.Once
	matchesServerVersionErr     error
}

var _ genericclioptions.RESTClientGetter = &MatchVersionFlags{}

func (f *MatchVersionFlags) checkMatchingServerVersion() error {
	f.checkServerVersion.Do(func() {
		if !f.RequireMatchedServerVersion {
			return
		}
		discoveryClient, err := f.Delegate.ToDiscoveryClient()
		if err != nil {
			f.matchesServerVersionErr = err
			return
		}
		f.matchesServerVersionErr = discovery.MatchesServerVersion(version.Get(), discoveryClient)
	})

	return f.matchesServerVersionErr
}

// ToRESTConfig 实现 RESTClientGetter。
// 根据给定的 .kubeconfig 文件路径、加载规则和配置 flag 覆盖项
// 返回 REST client 配置。
// 假定 AddFlags 方法已被调用。
func (f *MatchVersionFlags) ToRESTConfig() (*rest.Config, error) {
	if err := f.checkMatchingServerVersion(); err != nil {
		return nil, err
	}
	clientConfig, err := f.Delegate.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	// TODO 我们本不该这么做，这看起来像是哪里出了问题。
	setKubernetesDefaults(clientConfig)
	return clientConfig, nil
}

func (f *MatchVersionFlags) ToRawKubeConfigLoader() clientcmd.ClientConfig {
	return f.Delegate.ToRawKubeConfigLoader()
}

func (f *MatchVersionFlags) ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	if err := f.checkMatchingServerVersion(); err != nil {
		return nil, err
	}
	return f.Delegate.ToDiscoveryClient()
}

// ToRESTMapper 返回一个 mapper。
func (f *MatchVersionFlags) ToRESTMapper() (meta.RESTMapper, error) {
	if err := f.checkMatchingServerVersion(); err != nil {
		return nil, err
	}
	return f.Delegate.ToRESTMapper()
}

func (f *MatchVersionFlags) AddFlags(flags *pflag.FlagSet) {
	flags.BoolVar(&f.RequireMatchedServerVersion, flagMatchBinaryVersion, f.RequireMatchedServerVersion, "Require server version to match client version")
}

func NewMatchVersionFlags(delegate genericclioptions.RESTClientGetter) *MatchVersionFlags {
	return &MatchVersionFlags{
		Delegate: delegate,
	}
}

// setKubernetesDefaults 为给定的 client 配置设置访问 Kubernetes API 所需的默认值，
// 如果其中任何默认值无法设置或无效，则返回错误。
// TODO 这并非我们想要的做法。每个 clientset 都应自行按需设置默认值。
func setKubernetesDefaults(config *rest.Config) error {
	// TODO 移除这个 hack。它只是为了让 GetOptions 能够被序列化。
	config.GroupVersion = &schema.GroupVersion{Group: "", Version: "v1"}

	if config.APIPath == "" {
		config.APIPath = "/api"
	}
	if config.NegotiatedSerializer == nil {
		// 该 codec factory 确保资源不会被转换，因此资源不会
		// 经由 internal 版本来回转换。客户端不会执行
		// defaulting。
		config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	}
	return rest.SetKubernetesDefaults(config)
}
