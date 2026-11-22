/*
Copyright 2016 The Kubernetes Authors.

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

package labels

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestCloneAndAddLabel 验证 CloneAndAddLabel 不会改动原 map，
// 并顺带验证 CloneAndRemoveLabel 能还原为原始标签集。
func TestCloneAndAddLabel(t *testing.T) {
	labels := map[string]string{
		"foo1": "bar1",
		"foo2": "bar2",
		"foo3": "bar3",
	}

	cases := []struct {
		labels     map[string]string
		labelKey   string
		labelValue string
		want       map[string]string
	}{
		{
			labels: labels,
			want:   labels,
		},
		{
			labels:     labels,
			labelKey:   "foo4",
			labelValue: "42",
			want: map[string]string{
				"foo1": "bar1",
				"foo2": "bar2",
				"foo3": "bar3",
				"foo4": "42",
			},
		},
	}

	for _, tc := range cases {
		got := CloneAndAddLabel(tc.labels, tc.labelKey, tc.labelValue)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("[Add] got %v, want %v", got, tc.want)
		}
		// 再验证反向操作能回到原标签集。
		got_rm := CloneAndRemoveLabel(got, tc.labelKey)
		if !reflect.DeepEqual(got_rm, tc.labels) {
			t.Errorf("[RM] got %v, want %v", got_rm, tc.labels)
		}
	}
}

// TestAddLabel 验证 AddLabel 在空值、已有标签和 nil map 情况下的行为。
func TestAddLabel(t *testing.T) {
	labels := map[string]string{
		"foo1": "bar1",
		"foo2": "bar2",
		"foo3": "bar3",
	}

	cases := []struct {
		labels     map[string]string
		labelKey   string
		labelValue string
		want       map[string]string
	}{
		{
			labels: labels,
			want:   labels,
		},
		{
			labels:     labels,
			labelKey:   "foo4",
			labelValue: "food",
			want: map[string]string{
				"foo1": "bar1",
				"foo2": "bar2",
				"foo3": "bar3",
				"foo4": "food",
			},
		},
		{
			labels:     nil,
			labelKey:   "foo4",
			labelValue: "food",
			want: map[string]string{
				"foo4": "food",
			},
		},
	}

	for _, tc := range cases {
		got := AddLabel(tc.labels, tc.labelKey, tc.labelValue)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("got %v, want %v", got, tc.want)
		}
	}
}

// TestCloneSelectorAndAddLabel 验证克隆 LabelSelector 后添加标签不会影响原始 selector，
// 并覆盖 nil MatchLabels 的情况。
func TestCloneSelectorAndAddLabel(t *testing.T) {
	labels := map[string]string{
		"foo1": "bar1",
		"foo2": "bar2",
		"foo3": "bar3",
	}

	cases := []struct {
		labels     map[string]string
		labelKey   string
		labelValue string
		want       map[string]string
	}{
		{
			labels: labels,
			want:   labels,
		},
		{
			labels:     labels,
			labelKey:   "foo4",
			labelValue: "89",
			want: map[string]string{
				"foo1": "bar1",
				"foo2": "bar2",
				"foo3": "bar3",
				"foo4": "89",
			},
		},
		{
			labels:     nil,
			labelKey:   "foo4",
			labelValue: "12",
			want: map[string]string{
				"foo4": "12",
			},
		},
	}

	for _, tc := range cases {
		ls_in := metav1.LabelSelector{MatchLabels: tc.labels}
		ls_out := metav1.LabelSelector{MatchLabels: tc.want}

		got := CloneSelectorAndAddLabel(&ls_in, tc.labelKey, tc.labelValue)
		if !reflect.DeepEqual(got, &ls_out) {
			t.Errorf("got %v, want %v", got, tc.want)
		}
	}
}

// TestAddLabelToSelector 验证 AddLabelToSelector 原地修改 selector 的 MatchLabels，
// 包括 nil map 场景。
func TestAddLabelToSelector(t *testing.T) {
	labels := map[string]string{
		"foo1": "bar1",
		"foo2": "bar2",
		"foo3": "bar3",
	}

	cases := []struct {
		labels     map[string]string
		labelKey   string
		labelValue string
		want       map[string]string
	}{
		{
			labels: labels,
			want:   labels,
		},
		{
			labels:     labels,
			labelKey:   "foo4",
			labelValue: "89",
			want: map[string]string{
				"foo1": "bar1",
				"foo2": "bar2",
				"foo3": "bar3",
				"foo4": "89",
			},
		},
		{
			labels:     nil,
			labelKey:   "foo4",
			labelValue: "12",
			want: map[string]string{
				"foo4": "12",
			},
		},
	}

	for _, tc := range cases {
		ls_in := metav1.LabelSelector{MatchLabels: tc.labels}
		ls_out := metav1.LabelSelector{MatchLabels: tc.want}

		got := AddLabelToSelector(&ls_in, tc.labelKey, tc.labelValue)
		if !reflect.DeepEqual(got, &ls_out) {
			t.Errorf("got %v, want %v", got, tc.want)
		}
	}
}
