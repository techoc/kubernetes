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

package plugins

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
)

// TestTranslateCinderInTreeStorageClassToCSI 验证 Cinder StorageClass 的翻译：
// 普通参数与 availability 原样保留，fstype 改名，AllowedTopologies 的 zone 键改用 CSI 拓扑键。
func TestTranslateCinderInTreeStorageClassToCSI(t *testing.T) {
	translator := NewOpenStackCinderCSITranslator()
	logger, _ := ktesting.NewTestContext(t)

	cases := []struct {
		name   string
		sc     *storage.StorageClass
		expSc  *storage.StorageClass
		expErr bool
	}{
		{
			// 普通参数应原样保留。
			name:  "translate normal",
			sc:    NewStorageClass(map[string]string{"foo": "bar"}, nil),
			expSc: NewStorageClass(map[string]string{"foo": "bar"}, nil),
		},
		{
			// 空参数表转换后仍为空参数表。
			name:  "translate empty map",
			sc:    NewStorageClass(map[string]string{}, nil),
			expSc: NewStorageClass(map[string]string{}, nil),
		},

		{
			// fstype 键应改名为 CSI 的 fstype 参数键。
			name:  "translate with fstype",
			sc:    NewStorageClass(map[string]string{"fstype": "ext3"}, nil),
			expSc: NewStorageClass(map[string]string{"csi.storage.k8s.io/fstype": "ext3"}, nil),
		},
		{
			// availability 是驱动原生支持的参数，不应被转成 AllowedTopologies。
			name:  "translate with topology in parameters (no translation expected)",
			sc:    NewStorageClass(map[string]string{"availability": "nova"}, nil),
			expSc: NewStorageClass(map[string]string{"availability": "nova"}, nil),
		},
		{
			// AllowedTopologies 中的 zone 键应替换为 Cinder CSI 的拓扑键。
			name:  "translate with topology",
			sc:    NewStorageClass(map[string]string{}, generateToplogySelectors(v1.LabelFailureDomainBetaZone, []string{"nova"})),
			expSc: NewStorageClass(map[string]string{}, generateToplogySelectors(CinderTopologyKey, []string{"nova"})),
		},
	}

	for _, tc := range cases {
		t.Logf("Testing %v", tc.name)
		// 逐例执行转换，并校验错误预期与结果深比较。
		got, err := translator.TranslateInTreeStorageClassToCSI(logger, tc.sc)
		if err != nil && !tc.expErr {
			t.Errorf("Did not expect error but got: %v", err)
		}

		if err == nil && tc.expErr {
			t.Errorf("Expected error, but did not get one.")
		}

		if !reflect.DeepEqual(got, tc.expSc) {
			t.Errorf("Got parameters: %v, expected: %v", got, tc.expSc)
		}

	}
}
