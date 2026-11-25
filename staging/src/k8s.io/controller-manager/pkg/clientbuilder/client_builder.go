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

package clientbuilder

import (
	"k8s.io/client-go/discovery"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// ControllerClientBuilder 允许为各控制器获取 client 与 config。
// 注意 staging/src/k8s.io/cloud-provider/cloud.go 中也有一份拷贝；
// TODO: Extract this into a separate controller utilities repo (issues/68947)
// ControllerClientBuilder 定义“为某个控制器创建客户端/配置”的统一入口。
// 每个控制器用自己名字调用，得到的客户端 User-Agent 会带上该名字，便于在 apiserver 侧追踪。
// 注意：cloud-provider 仓库中有一份拷贝，改动时需同步（issue #68947）。
type ControllerClientBuilder interface {
	Config(name string) (*restclient.Config, error)
	// 返回带控制器身份的 REST 配置。
	ConfigOrDie(name string) *restclient.Config
	// 同上，但出错时直接 Fatal 终止进程。
	Client(name string) (clientset.Interface, error)
	// 基于配置创建 Kubernetes clientset。
	ClientOrDie(name string) clientset.Interface
	DiscoveryClient(name string) (discovery.DiscoveryInterface, error)
	// 创建发现客户端，内部会调高 Burst 以应对大量发现请求。
	DiscoveryClientOrDie(name string) discovery.DiscoveryInterface
}

// SimpleControllerClientBuilder 返回一个固定客户端、但 User-Agent 各不相同的构建器。
// 最简单的实现：所有控制器共用一份基础配置，仅 User-Agent 不同。
type SimpleControllerClientBuilder struct {
	// ClientConfig 是被克隆的骨架配置，作为每个控制器客户端的基础。
	// 作为模板的基础配置，每次为控制器生成客户端时都会克隆一份。
	ClientConfig *restclient.Config
}

// Config 返回固定客户端对应的 client config。
// 克隆模板配置并加上以控制器名命名的 User-Agent。
// 使用值拷贝而不是直接修改模板，避免控制器之间互相污染。
func (b SimpleControllerClientBuilder) Config(name string) (*restclient.Config, error) {
	clientConfig := *b.ClientConfig
	// 浅拷贝一份，后续 AddUserAgent 只作用于副本。
	return restclient.AddUserAgent(&clientConfig, name), nil
}

// ConfigOrDie 在之前的 config 函数没有错误时返回 client config；
// 若获取客户端出错，会记录错误并终止所在进程。
// Config 的“不会失败”版本；失败时记录日志并退出进程。
func (b SimpleControllerClientBuilder) ConfigOrDie(name string) *restclient.Config {
	clientConfig, err := b.Config(name)
	if err != nil {
		klog.Fatal(err)
	}
	return clientConfig
}

// Client 返回由 ClientBuilder 构造的 clientset.Interface。
// 先取配置再构造 clientset，两步都可能失败，因此都向上返回错误。
func (b SimpleControllerClientBuilder) Client(name string) (clientset.Interface, error) {
	clientConfig, err := b.Config(name)
	if err != nil {
		return nil, err
	}
	return clientset.NewForConfig(clientConfig)
}

// ClientOrDie 返回由 ClientBuilder 构造、不会报错的 clientset.Interface；
// 若获取客户端出错，会记录错误并终止所在进程。
// Client 的“不会失败”版本。
func (b SimpleControllerClientBuilder) ClientOrDie(name string) clientset.Interface {
	client, err := b.Client(name)
	if err != nil {
		klog.Fatal(err)
	}
	return client
}

// DiscoveryClient 返回由 ClientBuilder 构造的 discovery.DiscoveryInterface。
// Discovery 比较特殊：为了应对大量的发现请求，会人为把 burst 调得很高。
// 发现客户端的特殊之处：请求量大但频率低，需要临时调高 Burst/QPS。
func (b SimpleControllerClientBuilder) DiscoveryClient(name string) (discovery.DiscoveryInterface, error) {
	clientConfig, err := b.Config(name)
	if err != nil {
		return nil, err
	}
	// Discovery 会以较低的频率发出大量请求；调高 burst 能让突发请求成功，并在几秒内完成令牌回填。
	// 允许短时间内堆积大量发现请求，随后几秒内即可补充令牌。
	clientConfig.Burst = 200
	clientConfig.QPS = 20
	return clientset.NewForConfig(clientConfig)
}

// DiscoveryClientOrDie 返回由 ClientBuilder 构造、不会报错的 discovery.DiscoveryInterface。
// Discovery 比较特殊：为了应对大量的发现请求，会人为把 burst 调得很高；
// 若获取客户端出错，会记录错误并终止所在进程。
// DiscoveryClient 的“不会失败”版本。
func (b SimpleControllerClientBuilder) DiscoveryClientOrDie(name string) discovery.DiscoveryInterface {
	client, err := b.DiscoveryClient(name)
	if err != nil {
		klog.Fatal(err)
	}
	return client
}
