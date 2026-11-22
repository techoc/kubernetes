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

package initializer

import (
	"net/url"
	"testing"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/util/webhook"
)

// doNothingAdmission 只满足 admission.Interface 的最小实现，用于让
// serviceWanter 嵌入而不必重复实现 Handles。
type doNothingAdmission struct{}

func (doNothingAdmission) Handles(o admission.Operation) bool { return false }

// fakeServiceResolver 是一个恒返回 nil 的解析器，仅用于身份比较。
type fakeServiceResolver struct{}

func (f *fakeServiceResolver) ResolveEndpoint(namespace, name string, port int32) (*url.URL, error) {
	return nil, nil
}

// serviceWanter 同时实现 WantsServiceResolver，记录注入进来的解析器。
type serviceWanter struct {
	doNothingAdmission
	got ServiceResolver
}

func (s *serviceWanter) SetServiceResolver(sr webhook.ServiceResolver) { s.got = sr }

// TestWantsServiceResolver 验证初始化器会通过 WantsServiceResolver 接口把
// 同一个解析器实例注入给插件（而不是复制或包装）。
func TestWantsServiceResolver(t *testing.T) {
	sw := &serviceWanter{}
	fsr := &fakeServiceResolver{}
	i := NewPluginInitializer(nil, fsr)
	i.Initialize(sw)
	if got, ok := sw.got.(*fakeServiceResolver); !ok || got != fsr {
		t.Errorf("plumbing fail - %v %v#", ok, got)
	}
}
