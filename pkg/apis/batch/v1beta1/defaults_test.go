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

package v1beta1_test

import (
	"reflect"
	"testing"

	batchv1beta1 "k8s.io/api/batch/v1beta1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	_ "k8s.io/kubernetes/pkg/apis/batch/install"
	. "k8s.io/kubernetes/pkg/apis/batch/v1beta1"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/utils/ptr"
)

// TestSetDefaultCronJob 验证 CronJob 经过编解码与转换的 roundTrip 流程后，
// 缺省字段会被正确地填充为默认值（并发策略 AllowConcurrent、不暂停、
// 成功/失败历史任务保留数分别为 3 和 1），而用户已显式设置的字段
// 不会被默认值覆盖。采用表驱动测试把"空对象"和"已设字段"两种场景放在一起比较。
func TestSetDefaultCronJob(t *testing.T) {
	tests := map[string]struct {
		original *batchv1beta1.CronJob
		expected *batchv1beta1.CronJob
	}{
		// 用例一：空对象的所有相关字段都应被填上默认值，
		// 这是默认化逻辑最常见也最容易遗漏字段的场景。
		"empty batchv1beta1.CronJob should default batchv1beta1.ConcurrencyPolicy and Suspend": {
			original: &batchv1beta1.CronJob{},
			expected: &batchv1beta1.CronJob{
				Spec: batchv1beta1.CronJobSpec{
					ConcurrencyPolicy:          batchv1beta1.AllowConcurrent,
					Suspend:                    newBool(false),
					SuccessfulJobsHistoryLimit: ptr.To[int32](3),
					FailedJobsHistoryLimit:     ptr.To[int32](1),
				},
			},
		},
		// 用例二：显式设置的非默认值（包括 ForbidConcurrent、true 以及 5）
		// 必须在 roundTrip 后原样保留，否则默认化函数会错误地覆盖用户意图。
		"set fields should not be defaulted": {
			original: &batchv1beta1.CronJob{
				Spec: batchv1beta1.CronJobSpec{
					ConcurrencyPolicy:          batchv1beta1.ForbidConcurrent,
					Suspend:                    newBool(true),
					SuccessfulJobsHistoryLimit: ptr.To[int32](5),
					FailedJobsHistoryLimit:     ptr.To[int32](5),
				},
			},
			expected: &batchv1beta1.CronJob{
				Spec: batchv1beta1.CronJobSpec{
					ConcurrencyPolicy:          batchv1beta1.ForbidConcurrent,
					Suspend:                    newBool(true),
					SuccessfulJobsHistoryLimit: ptr.To[int32](5),
					FailedJobsHistoryLimit:     ptr.To[int32](5),
				},
			},
		},
	}

	for name, test := range tests {
		original := test.original
		expected := test.expected
		// 默认值是在编解码/转换路径中注入的，必须走一遍 roundTrip 才能观察；
		// 直接调用默认化函数会漏掉注册环节是否正确接线的问题。
		obj2 := roundTrip(t, runtime.Object(original))
		actual, ok := obj2.(*batchv1beta1.CronJob)
		if !ok {
			// 类型不符说明转换返回了错误的对象，后续字段断言都已无意义，立即终止。
			t.Errorf("%s: unexpected object: %v", name, actual)
			t.FailNow()
		}
		// 逐个字段断言（而不是整体 DeepEqual），让失败信息直接指向具体是哪个默认值出错。
		if actual.Spec.ConcurrencyPolicy != expected.Spec.ConcurrencyPolicy {
			t.Errorf("%s: got different concurrencyPolicy than expected: %v %v", name, actual.Spec.ConcurrencyPolicy, expected.Spec.ConcurrencyPolicy)
		}
		if *actual.Spec.Suspend != *expected.Spec.Suspend {
			t.Errorf("%s: got different suspend than expected: %v %v", name, *actual.Spec.Suspend, *expected.Spec.Suspend)
		}
		if *actual.Spec.SuccessfulJobsHistoryLimit != *expected.Spec.SuccessfulJobsHistoryLimit {
			t.Errorf("%s: got different successfulJobsHistoryLimit than expected: %v %v", name, *actual.Spec.SuccessfulJobsHistoryLimit, *expected.Spec.SuccessfulJobsHistoryLimit)
		}
		if *actual.Spec.FailedJobsHistoryLimit != *expected.Spec.FailedJobsHistoryLimit {
			t.Errorf("%s: got different failedJobsHistoryLimit than expected: %v %v", name, *actual.Spec.FailedJobsHistoryLimit, *expected.Spec.FailedJobsHistoryLimit)
		}
	}
}

// roundTrip 模拟 API 请求中对象经历的完整路径：用 legacy codec 编码为序列化数据，
// 再用 universal decoder 解码（这一步会触发默认化），最后通过 scheme 转换回目标类型。
// 默认函数正是在这条路径上生效的，所以测试默认值必须走完整个往返。
func roundTrip(t *testing.T, obj runtime.Object) runtime.Object {
	data, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(SchemeGroupVersion), obj)
	if err != nil {
		// 编码失败时把原对象一起打印出来，便于定位是哪个字段不受 codec 支持。
		t.Errorf("%v\n %#v", err, obj)
		return nil
	}
	obj2, err := runtime.Decode(legacyscheme.Codecs.UniversalDecoder(), data)
	if err != nil {
		// 解码失败时同时打印编码后的数据，方便判断是序列化格式问题还是默认值注入问题。
		t.Errorf("%v\nData: %s\nSource: %#v", err, string(data), obj)
		return nil
	}
	// 用反射创建一个与输入相同具体类型的空对象作为转换目标，避免把内部版本对象
	// 直接当成外部版本对象返回，从而确保看到的默认值确实经过了转换链。
	obj3 := reflect.New(reflect.TypeOf(obj).Elem()).Interface().(runtime.Object)
	err = legacyscheme.Scheme.Convert(obj2, obj3, nil)
	if err != nil {
		t.Errorf("%v\nSource: %#v", err, obj2)
		return nil
	}
	return obj3
}

// newBool 返回一个指向给定布尔值的指针，方便在测试用例中构造 *bool 字段。
func newBool(val bool) *bool {
	// 取地址而非使用包级共享变量，防止不同用例之间共享指针造成意外串改。
	p := new(bool)
	*p = val
	return p
}
