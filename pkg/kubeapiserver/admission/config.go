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

package admission

import (
	"k8s.io/apiserver/pkg/admission"
)

// Config 持有初始化准入插件所需的配置。
type Config struct{}

// New 构建准入插件所需的初始化器及启动钩子。
func (c *Config) New() ([]admission.PluginInitializer, error) {
	return []admission.PluginInitializer{NewPluginInitializer()}, nil
}
