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

package registrytest

import (
	"context"
	"sync"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// ServiceRegistry 是用于测试的内存 Service 存储模拟，记录最近一次读取、创建、更新和删除的对象信息。
type ServiceRegistry struct {
	mu      sync.Mutex
	List    api.ServiceList
	Service *api.Service
	Updates []api.Service
	Err     error

	DeletedID string
	GottenID  string
	UpdatedID string
}

// SetError 设置后续 Service 操作统一返回的模拟错误。
func (r *ServiceRegistry) SetError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Err = err
}

// ListServices 按请求上下文中的 namespace 过滤 Service，并复制列表元数据。
func (r *ServiceRegistry) ListServices(ctx context.Context, options *metainternalversion.ListOptions) (*api.ServiceList, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ns, _ := genericapirequest.NamespaceFrom(ctx)

	// 将内部 ServiceList 的 TypeMeta 和 ListMeta 复制到返回列表。
	res := new(api.ServiceList)
	res.TypeMeta = r.List.TypeMeta
	res.ListMeta = r.List.ListMeta

	if ns != metav1.NamespaceAll {
		for _, service := range r.List.Items {
			if ns == service.Namespace {
				res.Items = append(res.Items, service)
			}
		}
	} else {
		res.Items = append([]api.Service{}, r.List.Items...)
	}

	return res, r.Err
}

// CreateService 记录新 Service，并将其追加到模拟列表。
func (r *ServiceRegistry) CreateService(ctx context.Context, svc *api.Service, createValidation rest.ValidateObjectFunc) (*api.Service, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Service = svc.DeepCopy()

	r.List.Items = append(r.List.Items, *svc)
	return svc, r.Err
}

// GetService 记录被读取的 Service 名称，并返回当前模拟对象。
func (r *ServiceRegistry) GetService(ctx context.Context, id string, options *metav1.GetOptions) (*api.Service, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.GottenID = id
	return r.Service, r.Err
}

// DeleteService 记录被删除的 Service 名称，并清空当前模拟对象。
func (r *ServiceRegistry) DeleteService(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.DeletedID = id
	r.Service = nil
	return r.Err
}

// UpdateService 记录更新的 Service 名称和对象，并替换当前模拟对象。
func (r *ServiceRegistry) UpdateService(ctx context.Context, svc *api.Service, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc) (*api.Service, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.UpdatedID = svc.Name
	*r.Service = *svc
	r.Updates = append(r.Updates, *svc)
	return svc, r.Err
}

// WatchServices 当前不产生事件流，仅返回预设的模拟错误。
func (r *ServiceRegistry) WatchServices(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return nil, r.Err
}
