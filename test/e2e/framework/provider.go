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

package framework

import (
	"context"
	"fmt"
	"os"
	"sync"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
)

// Factory 是操作 provider 特定行为的函数。
type Factory func() (ProviderInterface, error)

var (
	providers = make(map[string]Factory)
	mutex     sync.Mutex
)

// RegisterProvider 应在应用初始化期间调用，
// 通常由 provider 包中的 init 函数调用。
func RegisterProvider(name string, factory Factory) {
	mutex.Lock()
	defer mutex.Unlock()
	if _, ok := providers[name]; ok {
		panic(fmt.Sprintf("provider %s already registered", name))
	}
	providers[name] = factory
}

// GetProviders 返回所有当前注册的 provider 名称。
func GetProviders() []string {
	mutex.Lock()
	defer mutex.Unlock()
	var providerNames []string
	for name := range providers {
		providerNames = append(providerNames, name)
	}
	return providerNames
}

func init() {
	// "local" 或 "skeleton" 始终可用。
	RegisterProvider("local", func() (ProviderInterface, error) {
		return NullProvider{}, nil
	})
	RegisterProvider("skeleton", func() (ProviderInterface, error) {
		return NullProvider{}, nil
	})
	// 过去允许空字符串，但现在
	// 已不是合法值。
}

// SetupProviderConfig 校验选中的 provider 并为之创建
// 一个接口实例。
func SetupProviderConfig(providerName string) (ProviderInterface, error) {
	var err error

	mutex.Lock()
	defer mutex.Unlock()
	factory, ok := providers[providerName]
	if !ok {
		return nil, fmt.Errorf("The provider %s is unknown: %w", providerName, os.ErrNotExist)
	}
	provider, err := factory()

	return provider, err
}

// ProviderInterface 包含某些 provider 特定功能的实现。
type ProviderInterface interface {
	FrameworkBeforeEach(f *Framework)
	FrameworkAfterEach(f *Framework)

	ResizeGroup(group string, size int32) error
	GetGroupNodes(group string) ([]string, error)
	GroupSize(group string) (int, error)

	DeleteNode(node *v1.Node) error

	CreatePD(zone string) (string, error)
	DeletePD(pdName string) error
	CreateShare() (string, string, string, error)
	DeleteShare(accountName, shareName string) error

	CreatePVSource(ctx context.Context, zone, diskName string) (*v1.PersistentVolumeSource, error)
	DeletePVSource(ctx context.Context, pvSource *v1.PersistentVolumeSource) error

	CleanupServiceResources(ctx context.Context, c clientset.Interface, loadBalancerName, region, zone string)

	EnsureLoadBalancerResourcesDeleted(ctx context.Context, ip, portRange string) error
	LoadBalancerSrcRanges() []string
	EnableAndDisableInternalLB() (enable, disable func(svc *v1.Service))
}

// NullProvider 是 ProviderInterface 的默认实现，
// 不执行任何操作。
type NullProvider struct{}

// FrameworkBeforeEach 是执行 BeforeEach 的基础实现。
func (n NullProvider) FrameworkBeforeEach(f *Framework) {}

// FrameworkAfterEach 是执行 AfterEach 的基础实现。
func (n NullProvider) FrameworkAfterEach(f *Framework) {}

// ResizeGroup 是调整 group 大小的基础实现。
func (n NullProvider) ResizeGroup(string, int32) error {
	return fmt.Errorf("Provider does not support InstanceGroups")
}

// GetGroupNodes 是返回 group 节点的基础实现。
func (n NullProvider) GetGroupNodes(group string) ([]string, error) {
	return nil, fmt.Errorf("provider does not support InstanceGroups")
}

// GroupSize 返回实例 group 的大小
func (n NullProvider) GroupSize(group string) (int, error) {
	return -1, fmt.Errorf("provider does not support InstanceGroups")
}

// DeleteNode 是删除节点的基础实现。
func (n NullProvider) DeleteNode(node *v1.Node) error {
	return fmt.Errorf("provider does not support DeleteNode")
}

func (n NullProvider) CreateShare() (string, string, string, error) {
	return "", "", "", fmt.Errorf("provider does not support volume creation")
}

func (n NullProvider) DeleteShare(accountName, shareName string) error {
	return fmt.Errorf("provider does not support volume deletion")
}

// CreatePD 是创建 PD 的基础实现。
func (n NullProvider) CreatePD(zone string) (string, error) {
	return "", fmt.Errorf("provider does not support volume creation")
}

// DeletePD 是删除 PD 的基础实现。
func (n NullProvider) DeletePD(pdName string) error {
	return fmt.Errorf("provider does not support volume deletion")
}

// CreatePVSource 是创建 PV source 的基础实现。
func (n NullProvider) CreatePVSource(ctx context.Context, zone, diskName string) (*v1.PersistentVolumeSource, error) {
	return nil, fmt.Errorf("Provider not supported")
}

// DeletePVSource 是删除 PV source 的基础实现。
func (n NullProvider) DeletePVSource(ctx context.Context, pvSource *v1.PersistentVolumeSource) error {
	return fmt.Errorf("Provider not supported")
}

// CleanupServiceResources 是清理 Service 资源的基础实现。
func (n NullProvider) CleanupServiceResources(ctx context.Context, c clientset.Interface, loadBalancerName, region, zone string) {
}

// EnsureLoadBalancerResourcesDeleted 是确保 load balancer 被删除的基础实现。
func (n NullProvider) EnsureLoadBalancerResourcesDeleted(ctx context.Context, ip, portRange string) error {
	return nil
}

// LoadBalancerSrcRanges 是返回 load balancer 所用 IP 范围的基础实现。
func (n NullProvider) LoadBalancerSrcRanges() []string {
	return nil
}

// EnableAndDisableInternalLB 是返回启用/禁用 internal LB 函数的基础实现。
func (n NullProvider) EnableAndDisableInternalLB() (enable, disable func(svc *v1.Service)) {
	nop := func(svc *v1.Service) {}
	return nop, nop
}

var _ ProviderInterface = NullProvider{}
