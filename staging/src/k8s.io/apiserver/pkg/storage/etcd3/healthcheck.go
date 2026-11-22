/*
Copyright 2015 The Kubernetes Authors.

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

package etcd3

import (
	"encoding/json"
	"fmt"
)

// etcdHealth encodes data returned from etcd /healthz handler.
type etcdHealth struct {
	// Note this has to be public so the json library can modify it.
	Health string `json:"health"`
}

// EtcdHealthCheck decodes data returned from etcd /healthz handler.
// Deprecated: Validate health by passing storagebackend.Config directly to storagefactory.CreateProber.
// 已弃用：请把 storagebackend.Config 直接传给 storagefactory.CreateProber 来探测健康状态。
func EtcdHealthCheck(data []byte) error {
	obj := etcdHealth{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	// etcd 的 /healthz 用字符串 "true"/"false" 而不是 JSON 布尔值。
	if obj.Health != "true" {
		return fmt.Errorf("Unhealthy status: %s", obj.Health)
	}
	return nil
}
