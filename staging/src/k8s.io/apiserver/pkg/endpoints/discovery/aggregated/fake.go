/*
Copyright 2022 The Kubernetes Authors.

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

package aggregated

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/emicklei/go-restful/v3"
	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/wait"
)

// FakeResourceManager 是 ResourceManager 的测试替身：记录调用并可与预期动作比对。
// 仅用于测试，不实现真实的 discovery 文档维护。
type FakeResourceManager interface {
	ResourceManager
	// Expect 返回用于描述预期动作的 manager。
	Expect() ResourceManager

	HasExpectedNumberActions() bool
	Validate() error
	WaitForActions(ctx context.Context, timeout time.Duration) error
}

// NewFakeResourceManager 创建空的 fake manager。
func NewFakeResourceManager() FakeResourceManager {
	return &fakeResourceManager{}
}

// fakeResourceManager 是一个带有辅助函数的 resource manager，用于检查实际动作
// 是否与预期一致，仅供测试使用
type fakeResourceManager struct {
	recorderResourceManager
	expect recorderResourceManager
}

// recorderResourceManager 不维护 discovery 文档，只记录对其接口方法的调用，供测试使用
type recorderResourceManager struct {
	lock    sync.RWMutex
	Actions []recorderResourceManagerAction
}

var _ ResourceManager = &fakeResourceManager{}
var _ ResourceManager = &recorderResourceManager{}

// recorderResourceManagerAction 保存对 resource manager 一次调用的记录
type recorderResourceManagerAction struct {
	Type    string
	Group   string
	Version string
	Value   interface{}
}

// Expect 返回预期动作记录器；调用方在其上调用同样的方法即可声明期望。
func (f *fakeResourceManager) Expect() ResourceManager {
	return &f.expect
}

// HasExpectedNumberActions 判断实际动作数是否已达到预期数（允许更多），
// 供 WaitForActions 轮询使用。这里同时持有两个锁，避免并发写入。
func (f *fakeResourceManager) HasExpectedNumberActions() bool {
	f.lock.RLock()
	defer f.lock.RUnlock()

	f.expect.lock.RLock()
	defer f.expect.lock.RUnlock()

	return len(f.Actions) >= len(f.expect.Actions)
}

// Validate 逐项比对预期动作与实际动作，失败时返回可读的 diff。
func (f *fakeResourceManager) Validate() error {
	f.lock.RLock()
	defer f.lock.RUnlock()

	f.expect.lock.RLock()
	defer f.expect.lock.RUnlock()

	if !reflect.DeepEqual(f.expect.Actions, f.Actions) {
		return errors.New(diff.Diff(f.expect.Actions, f.Actions))
	}
	return nil
}

// WaitForActions 以 100ms 间隔轮询，直到动作数量满足预期并校验通过，
// 或超时/ctx 取消。轮询是因为被测组件可能异步地驱动 manager。
func (f *fakeResourceManager) WaitForActions(ctx context.Context, timeout time.Duration) error {
	err := wait.PollImmediateWithContext(
		ctx,
		100*time.Millisecond, // 每 100ms 轮询一次
		timeout,              // 超过 timeout 后放弃
		func(ctx context.Context) (done bool, err error) {
			if f.HasExpectedNumberActions() {
				return true, f.Validate()
			}
			return false, nil
		})
	return err
}

// SetGroupVersionPriority 记录一次优先级设置。注意 Value 存的是 versionpriority，
// grouppriority 未记录，这是既有测试约定。
func (f *recorderResourceManager) SetGroupVersionPriority(gv metav1.GroupVersion, grouppriority, versionpriority int) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.Actions = append(f.Actions, recorderResourceManagerAction{
		Type:    "SetGroupVersionPriority",
		Group:   gv.Group,
		Version: gv.Version,
		Value:   versionpriority,
	})
}

// SetPeerDiscoveryProvider 记录 peer discovery provider 设置。
func (f *recorderResourceManager) SetPeerDiscoveryProvider(provider PeerDiscoveryProvider) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.Actions = append(f.Actions, recorderResourceManagerAction{
		Type:    "SetPeerDiscoveryProvider",
		Group:   "",
		Version: "",
		Value:   provider,
	})
}

// AddGroupVersion 记录新增组版本。
func (f *recorderResourceManager) AddGroupVersion(groupName string, value apidiscoveryv2.APIVersionDiscovery) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.Actions = append(f.Actions, recorderResourceManagerAction{
		Type:  "AddGroupVersion",
		Group: groupName,
		Value: value,
	})
}

// RemoveGroup 记录移除整个 API 组。
func (f *recorderResourceManager) RemoveGroup(groupName string) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.Actions = append(f.Actions, recorderResourceManagerAction{
		Type:  "RemoveGroup",
		Group: groupName,
	})

}

// RemoveGroupVersion 记录移除单个组版本。
func (f *recorderResourceManager) RemoveGroupVersion(gv metav1.GroupVersion) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.Actions = append(f.Actions, recorderResourceManagerAction{
		Type:    "RemoveGroupVersion",
		Group:   gv.Group,
		Version: gv.Version,
	})

}

// SetGroups 记录一次性替换全部组。
func (f *recorderResourceManager) SetGroups(values []apidiscoveryv2.APIGroupDiscovery) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.Actions = append(f.Actions, recorderResourceManagerAction{
		Type:  "SetGroups",
		Value: values,
	})
}

// WebService 未实现：测试替身不提供 HTTP 路由。
func (f *recorderResourceManager) WebService() *restful.WebService {
	panic("unimplemented")
}

// ServeHTTP 未实现：测试替身不直接对外提供 discovery 端点。
func (f *recorderResourceManager) ServeHTTP(http.ResponseWriter, *http.Request) {
	panic("unimplemented")
}

// WithSource 未实现：测试替身不依赖数据源。
func (f *recorderResourceManager) WithSource(source Source) ResourceManager {
	panic("unimplemented")
}

// AddInvalidationCallback 未实现：测试替身没有缓存失效逻辑。
func (f *recorderResourceManager) AddInvalidationCallback(callback func()) {
	panic("unimplemented")
}
