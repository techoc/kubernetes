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

package registrytest

import (
	"fmt"

	"k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
)

// ValidateStorageStrategies 检查存储映射中的 genericregistry.Store 是否都配置了创建、更新和删除策略。
func ValidateStorageStrategies(storageMap map[string]rest.Storage) []error {
	errs := []error{}

	for k, storage := range storageMap {
		switch t := storage.(type) {
		case registry.GenericStore:
			// 当前所有通用 Store 都应配置创建、更新和删除策略；缺少任意策略都会被记录为错误。
			if t.GetCreateStrategy() == nil {
				errs = append(errs, fmt.Errorf("store for type [%v] does not have a CreateStrategy", k))
			}
			if t.GetUpdateStrategy() == nil {
				errs = append(errs, fmt.Errorf("store for type [%v] does not have an UpdateStrategy", k))
			}
			if t.GetDeleteStrategy() == nil {
				errs = append(errs, fmt.Errorf("store for type [%v] does not have a DeleteStrategy", k))
			}
		}
	}

	return errs
}
