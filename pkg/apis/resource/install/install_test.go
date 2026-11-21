/*
Copyright 2022 The Kubernetes Authors.

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

package install

import (
	"encoding/json"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	internal "k8s.io/kubernetes/pkg/apis/resource"
)

// TestResourceVersioner 验证单对象与列表的 resourceVersion 都能通过 accessor 读取。
func TestResourceVersioner(t *testing.T) {
	claim := internal.ResourceClaim{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "10"}}
	version, err := meta.NewAccessor().ResourceVersion(&claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if version != "10" {
		t.Errorf("unexpected version %v", version)
	}

	// 列表的 resourceVersion 与单对象语义相同，因此一并验证。
	claimList := internal.ResourceClaimList{ListMeta: metav1.ListMeta{ResourceVersion: "10"}}
	version, err = meta.NewAccessor().ResourceVersion(&claimList)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if version != "10" {
		t.Errorf("unexpected version %v", version)
	}
}

// TestCodec 验证内部类型编码为 v1beta2 后带有正确的 TypeMeta。
func TestCodec(t *testing.T) {
	claim := internal.ResourceClaim{}
	data, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(schema.GroupVersion{Group: "resource.k8s.io", Version: "v1beta2"}), &claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 通过 JSON 反序列化检查编码结果中的 APIVersion 与 Kind。
	other := internal.ResourceClaim{}
	if err := json.Unmarshal(data, &other); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if other.APIVersion != "resource.k8s.io/v1beta2" || other.Kind != "ResourceClaim" {
		t.Errorf("unexpected unmarshalled object %#v", other)
	}
}

// TestUnversioned 校验 metav1.Status 被 scheme 识别为无版本对象。
func TestUnversioned(t *testing.T) {
	for _, obj := range []runtime.Object{
		&metav1.Status{},
	} {
		if unversioned, ok := legacyscheme.Scheme.IsUnversioned(obj); !unversioned || !ok {
			t.Errorf("%v is expected to be unversioned", reflect.TypeOf(obj))
		}
	}
}
