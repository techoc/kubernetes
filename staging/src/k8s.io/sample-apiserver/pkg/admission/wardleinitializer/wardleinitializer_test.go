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

package wardleinitializer_test

import (
	"context"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/sample-apiserver/pkg/admission/wardleinitializer"
	"k8s.io/sample-apiserver/pkg/generated/clientset/versioned/fake"
	informers "k8s.io/sample-apiserver/pkg/generated/informers/externalversions"
)

// TestWantsInternalWardleInformerFactory ensures that the informer factory is injected
// when the WantsInternalWardleInformerFactory interface is implemented by a plugin.
// 用测试桩断言注入的是同一个 factory 实例，说明接口断言分支确实走到了。
func TestWantsInternalWardleInformerFactory(t *testing.T) {
	cs := &fake.Clientset{}
	sf := informers.NewSharedInformerFactory(cs, time.Duration(1)*time.Second)
	target := wardleinitializer.New(sf)

	wantWardleInformerFactory := &wantInternalWardleInformerFactory{}
	target.Initialize(wantWardleInformerFactory)
	if wantWardleInformerFactory.sf != sf {
		t.Errorf("expected informer factory to be initialized")
	}
}

// wantInternalWardleInformerFactory is a test stub that fulfills the WantsInternalWardleInformerFactory interface
// 只关心注入回调，其余准入方法做成空实现，编译期断言保证接口没漂移。
type wantInternalWardleInformerFactory struct {
	sf informers.SharedInformerFactory
}

// 记下传入的 factory，测试再和原始实例做指针比较。
func (f *wantInternalWardleInformerFactory) SetInternalWardleInformerFactory(sf informers.SharedInformerFactory) {
	f.sf = sf
}

// 桩插件不参与实际准入，返回 nil 表示不拦截。
func (f *wantInternalWardleInformerFactory) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	return nil
}
func (f *wantInternalWardleInformerFactory) Handles(o admission.Operation) bool { return false }
func (f *wantInternalWardleInformerFactory) ValidateInitialization() error      { return nil }

// 两组断言让接口签名变更在编译时暴露，而不是等到运行时才 panic。
var _ admission.Interface = &wantInternalWardleInformerFactory{}
var _ wardleinitializer.WantsInternalWardleInformerFactory = &wantInternalWardleInformerFactory{}
