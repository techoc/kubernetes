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

package scheme

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	componentconfigtesting "k8s.io/component-base/config/testing"
	"k8s.io/kubernetes/pkg/kubelet/apis/config/fuzzer"
)

// TestRoundTripFuzzing 用 fuzz 数据验证 kubeletconfig 各版本的序列化往返一致性。
func TestRoundTripFuzzing(t *testing.T) {
	scheme, _, err := NewSchemeAndCodecs()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	roundtrip.RoundTripTestForScheme(t, scheme, fuzzer.Funcs)
}

// TestRoundTripYAML 验证 YAML 编解码的往返一致性。
func TestRoundTripYAML(t *testing.T) {
	scheme, codec, err := NewSchemeAndCodecs()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	componentconfigtesting.RoundTripTest(t, scheme, *codec)
}

// TestDefaultsYAML 验证默认值应用逻辑。
func TestDefaultsYAML(t *testing.T) {
	scheme, codec, err := NewSchemeAndCodecs()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	componentconfigtesting.DefaultingTest(t, scheme, *codec)
}
