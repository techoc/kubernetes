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

package banflunder_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/sample-apiserver/pkg/admission/plugin/banflunder"
	"k8s.io/sample-apiserver/pkg/admission/wardleinitializer"
	wardle "k8s.io/sample-apiserver/pkg/apis/wardle/v1alpha1"
	"k8s.io/sample-apiserver/pkg/generated/clientset/versioned/fake"
	informers "k8s.io/sample-apiserver/pkg/generated/informers/externalversions"
)

// TestBanflunderAdmissionPlugin 测试禁用 Flunder 名字的准入插件。
// 用表格驱动的方式覆盖“命中禁用名单 / 未命中 / 资源类型不符”三条路径，
// 重点验证插件只在 Kind 确实是 Flunder 时才拦截。
func TestBanflunderAdmissionPlugin(t *testing.T) {
	// 每个场景各自准备一份假客户端返回的 Fischer 列表以及待准入的 Flunder，
	// admissionMustFail 表示期望插件拒绝该请求。
	var scenarios = []struct {
		informersOutput        wardle.FischerList
		admissionInput         wardle.Flunder
		admissionInputKind     schema.GroupVersionKind
		admissionInputResource schema.GroupVersionResource
		admissionMustFail      bool
	}{
		// 场景 1：名字在禁用名单中的 Flunder 必须被拒绝。
		{
			informersOutput: wardle.FischerList{
				Items: []wardle.Fischer{
					{DisallowedFlunders: []string{"badname"}},
				},
			},
			admissionInput: wardle.Flunder{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "badname",
					Namespace: "",
				},
			},
			admissionInputKind:     wardle.SchemeGroupVersion.WithKind("Flunder").GroupKind().WithVersion("version"),
			admissionInputResource: wardle.Resource("flunders").WithVersion("version"),
			admissionMustFail:      true,
		},
		// 场景 2：名字不在禁用名单中的 Flunder 必须被放行。
		{
			informersOutput: wardle.FischerList{
				Items: []wardle.Fischer{
					{DisallowedFlunders: []string{"badname"}},
				},
			},
			admissionInput: wardle.Flunder{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "goodname",
					Namespace: "",
				},
			},
			admissionInputKind:     wardle.SchemeGroupVersion.WithKind("Flunder").GroupKind().WithVersion("version"),
			admissionInputResource: wardle.Resource("flunders").WithVersion("version"),
			admissionMustFail:      false,
		},
		// 场景 3：即使名字在禁用名单中，非 Flunder 请求也应整体放行。
		{
			informersOutput: wardle.FischerList{
				Items: []wardle.Fischer{
					{DisallowedFlunders: []string{"badname"}},
				},
			},
			admissionInput: wardle.Flunder{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "badname",
					Namespace: "",
				},
			},
			admissionInputKind:     wardle.SchemeGroupVersion.WithKind("NotFlunder").GroupKind().WithVersion("version"),
			admissionInputResource: wardle.Resource("notflunders").WithVersion("version"),
			admissionMustFail:      false,
		},
	}

	for index, scenario := range scenarios {
		// 用闭包包住每个场景，便于 defer 关闭 informer 的 stop channel，避免场景间互相污染。
		func() {
			// 让 fake 客户端的 list 总是返回该场景预设的 Fischer，插件据此构建缓存。
			cs := &fake.Clientset{}
			cs.AddReactor("list", "fischers", func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, &scenario.informersOutput, nil
			})
			informersFactory := informers.NewSharedInformerFactory(cs, 5*time.Minute)

			target, err := banflunder.New()
			if err != nil {
				t.Fatalf("scenario %d: failed to create banflunder admission plugin due to = %v", index, err)
			}

			targetInitializer := wardleinitializer.New(informersFactory)
			targetInitializer.Initialize(target)

			err = admission.ValidateInitialization(target)
			if err != nil {
				t.Fatalf("scenario %d: failed to initialize banflunder admission plugin due to =%v", index, err)
			}

			stop := make(chan struct{})
			defer close(stop)
			// 必须先等缓存同步完成，再调用 Admit，否则插件看到的禁用名单可能还是空的。
			informersFactory.Start(stop)
			informersFactory.WaitForCacheSync(stop)

			// 构造一个 CREATE 请求，Kind/Resource 由场景指定，用来验证插件的类型过滤。
			err = target.Admit(context.TODO(), admission.NewAttributesRecord(
				&scenario.admissionInput,
				nil,
				scenario.admissionInputKind,
				scenario.admissionInput.ObjectMeta.Namespace,
				"",
				scenario.admissionInputResource,
				"",
				admission.Create,
				&metav1.CreateOptions{},
				false,
				nil),
				nil,
			)

			// 只在期望失败时要求报错，其余场景要求零错误，避免“误杀”通过。
			if scenario.admissionMustFail && err == nil {
				t.Errorf("scenario %d: expected an error but got nothing", index)
			}

			if !scenario.admissionMustFail && err != nil {
				t.Errorf("scenario %d: banflunder admission plugin returned unexpected error = %v", index, err)
			}
		}()
	}
}
