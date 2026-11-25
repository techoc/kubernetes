/*
Copyright 2019 The Kubernetes Authors.

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

package webhook

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/admissionregistration/v1"
	"sigs.k8s.io/randfill"
)

// TestMutatingWebhookAccessor 验证 MutatingWebhookAccessor 在两个方向上的
// 保真度：一是 GetMutatingWebhook 返回原始对象，二是通过各 Get* 方法
// 重建出的 webhook 与原对象完全一致。使用 randfill 随机填充 100 轮，
// 以便覆盖 nil/非 nil 字段组合，防止新增字段后访问器被遗漏。
func TestMutatingWebhookAccessor(t *testing.T) {
	f := randfill.New()
	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("Run %d/100", i), func(t *testing.T) {
			orig := &v1.MutatingWebhook{}
			f.Fill(orig)

			// ReinvocationPolicy 只属于 MutatingWebhook，访问器接口不暴露它，
			// 因此先清零，否则往返比较会误报差异。
			orig.ReinvocationPolicy = nil

			uid := fmt.Sprintf("test.configuration.admission/%s/0", orig.Name)
			accessor := NewMutatingWebhookAccessor(uid, "test.configuration.admission", orig)
			// UID 用于在指标与日志中唯一定位 hook，必须原样返回。
			if uid != accessor.GetUID() {
				t.Errorf("expected GetUID to return %s, but got %s", accessor.GetUID(), uid)
			}
			m, ok := accessor.GetMutatingWebhook()
			if !ok {
				t.Errorf("expected GetMutatingWebhook to return ok for mutating webhook accessor")
			}
			if !reflect.DeepEqual(orig, m) {
				t.Errorf("expected GetMutatingWebhook to return original webhook, diff:\n%s", cmp.Diff(orig, m))
			}
			// 变更型访问器不应被当作校验型使用，避免调度器把 hook 放错阶段。
			if _, ok := accessor.GetValidatingWebhook(); ok {
				t.Errorf("expected GetValidatingWebhook to be nil for mutating webhook accessor")
			}
			// 逐字段重建，任何访问器遗漏的字段都会在这里暴露。
			copy := &v1.MutatingWebhook{
				Name:                    accessor.GetName(),
				ClientConfig:            accessor.GetClientConfig(),
				Rules:                   accessor.GetRules(),
				FailurePolicy:           accessor.GetFailurePolicy(),
				MatchPolicy:             accessor.GetMatchPolicy(),
				NamespaceSelector:       accessor.GetNamespaceSelector(),
				ObjectSelector:          accessor.GetObjectSelector(),
				SideEffects:             accessor.GetSideEffects(),
				TimeoutSeconds:          accessor.GetTimeoutSeconds(),
				AdmissionReviewVersions: accessor.GetAdmissionReviewVersions(),
				MatchConditions:         accessor.GetMatchConditions(),
			}
			if !reflect.DeepEqual(orig, copy) {
				t.Errorf("expected mutatingWebhook to round trip through WebhookAccessor, diff:\n%s", cmp.Diff(orig, copy))
			}
		})
	}
}

// TestValidatingWebhookAccessor 与变更型用例对称，验证校验型访问器不会
// 被误当作变更型使用，且所有字段可无损往返。
func TestValidatingWebhookAccessor(t *testing.T) {
	f := randfill.New()
	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("Run %d/100", i), func(t *testing.T) {
			orig := &v1.ValidatingWebhook{}
			f.Fill(orig)
			uid := fmt.Sprintf("test.configuration.admission/%s/0", orig.Name)
			accessor := NewValidatingWebhookAccessor(uid, "test.configuration.admission", orig)
			if uid != accessor.GetUID() {
				t.Errorf("expected GetUID to return %s, but got %s", accessor.GetUID(), uid)
			}
			m, ok := accessor.GetValidatingWebhook()
			if !ok {
				t.Errorf("expected GetValidatingWebhook to return ok for validating webhook accessor")
			}
			if !reflect.DeepEqual(orig, m) {
				t.Errorf("expected GetValidatingWebhook to return original webhook, diff:\n%s", cmp.Diff(orig, m))
			}
			// 校验型访问器暴露变更型对象会破坏阶段隔离。
			if _, ok := accessor.GetMutatingWebhook(); ok {
				t.Errorf("expected GetMutatingWebhook to be nil for validating webhook accessor")
			}
			copy := &v1.ValidatingWebhook{
				Name:                    accessor.GetName(),
				ClientConfig:            accessor.GetClientConfig(),
				Rules:                   accessor.GetRules(),
				FailurePolicy:           accessor.GetFailurePolicy(),
				MatchPolicy:             accessor.GetMatchPolicy(),
				NamespaceSelector:       accessor.GetNamespaceSelector(),
				ObjectSelector:          accessor.GetObjectSelector(),
				SideEffects:             accessor.GetSideEffects(),
				TimeoutSeconds:          accessor.GetTimeoutSeconds(),
				AdmissionReviewVersions: accessor.GetAdmissionReviewVersions(),
				MatchConditions:         accessor.GetMatchConditions(),
			}
			if !reflect.DeepEqual(orig, copy) {
				t.Errorf("expected validatingWebhook to round trip through WebhookAccessor, diff:\n%s", cmp.Diff(orig, copy))
			}
		})
	}
}
