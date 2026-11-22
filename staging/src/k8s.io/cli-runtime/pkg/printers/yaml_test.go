/*
Copyright 2021 The Kubernetes Authors.

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

package printers

import (
	"testing"

	"sigs.k8s.io/yaml"

	"k8s.io/client-go/kubernetes/scheme"
)

// TestYAMLPrinter 复用 testPrinter 的往返断言，仅把反序列化器换成 YAML。
func TestYAMLPrinter(t *testing.T) {
	testPrinter(t, NewTypeSetter(scheme.Scheme).ToPrinter(&YAMLPrinter{}), yamlUnmarshal)
}

// yamlUnmarshal 是适配 sigs.k8s.io/yaml 的 Unmarshal 签名的小包装。
func yamlUnmarshal(data []byte, v interface{}) error {
	return yaml.Unmarshal(data, v)
}
