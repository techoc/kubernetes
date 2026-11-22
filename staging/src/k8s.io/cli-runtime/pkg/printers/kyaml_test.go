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

// TestKYAMLPrinter 直接复用 testPrinter 的用例集，只是把 Printer 换成 KYAMLPrinter。
func TestKYAMLPrinter(t *testing.T) {
	testPrinter(t, NewTypeSetter(scheme.Scheme).ToPrinter(&KYAMLPrinter{}), kyamlUnmarshal)
}

// kyamlUnmarshal 作为 testPrinter 的解析器：用 sigs.k8s.io/yaml 校验输出可被重新读取。
func kyamlUnmarshal(data []byte, v interface{}) error {
	return yaml.Unmarshal(data, v)
}
