/*
Copyright The Kubernetes Authors.

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

// +k8s:deepcopy-gen=package
// +k8s:conversion-gen=k8s.io/apiserver/pkg/admission/plugin/policy/config/apis/policyconfig

// v1 是策略类准入插件配置的稳定版本。转换目标（conversion-gen）指向无版本号的
// policyconfig 包，因此任何字段调整都必须保持二者可双向无损转换。
package v1
