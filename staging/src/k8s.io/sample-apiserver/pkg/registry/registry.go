/*
Copyright 2017 The Kubernetes Authors.

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

package registry

import (
	"fmt"

	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
)

// REST implements a RESTStorage for API services against etcd
// 内嵌 Store 直接复用标准 CRUD，示例没有自定义子资源。
type REST struct {
	*genericregistry.Store
}

// RESTInPeace is just a simple function that panics on error.
// Otherwise returns the given storage object. It is meant to be
// a wrapper for wardle registries.
// 存储注册失败属于启动期不可恢复错误，就地 panic 比带着坏存储继续跑更安全。
func RESTInPeace(storage *REST, err error) *REST {
	// 包一层错误说明，堆栈里能看出是哪个资源的存储没建起来。
	if err != nil {
		err = fmt.Errorf("unable to create REST storage for a resource due to %v, will die", err)
		panic(err)
	}
	return storage
}
