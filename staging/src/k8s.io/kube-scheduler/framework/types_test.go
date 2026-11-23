/*
Copyright 2025 The Kubernetes Authors.

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

package framework

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// hostPortInfoParam 只是一组测试输入，字段顺序刻意和 Add/Remove 的参数顺序不同，
// 这样每个用例都得显式写出协议、IP、端口，避免位置参数读起来不知所以。
type hostPortInfoParam struct {
	protocol, ip string
	port         int32
}

// 端口 0 和负数是非法值，这里也塞进输入：它们不该进 map，
// 否则冲突检查会对永远用不上的端口报警。
func TestHostPortInfo_AddRemove(t *testing.T) {
	tests := []struct {
		desc    string
		added   []hostPortInfoParam
		removed []hostPortInfoParam
		length  int
	}{
		{
			desc: "normal add case",
			added: []hostPortInfoParam{
				{"TCP", "127.0.0.1", 79},
				{"UDP", "127.0.0.1", 80},
				{"TCP", "127.0.0.1", 81},
				{"TCP", "127.0.0.1", 82},
				// this might not make sense in real case, but the struct doesn't forbid it.
				{"TCP", "0.0.0.0", 79},
				{"UDP", "0.0.0.0", 80},
				{"TCP", "0.0.0.0", 81},
				{"TCP", "0.0.0.0", 82},
				{"TCP", "0.0.0.0", 0},
				{"TCP", "0.0.0.0", -1},
			},
			length: 8,
		},
		{
			desc: "empty ip and protocol add should work",
			added: []hostPortInfoParam{
				{"", "127.0.0.1", 79},
				{"UDP", "127.0.0.1", 80},
				{"", "127.0.0.1", 81},
				{"", "127.0.0.1", 82},
				{"", "", 79},
				{"UDP", "", 80},
				{"", "", 81},
				{"", "", 82},
				{"", "", 0},
				{"", "", -1},
			},
			length: 8,
		},
		{
			desc: "normal remove case",
			added: []hostPortInfoParam{
				{"TCP", "127.0.0.1", 79},
				{"UDP", "127.0.0.1", 80},
				{"TCP", "127.0.0.1", 81},
				{"TCP", "127.0.0.1", 82},
				{"TCP", "0.0.0.0", 79},
				{"UDP", "0.0.0.0", 80},
				{"TCP", "0.0.0.0", 81},
				{"TCP", "0.0.0.0", 82},
			},
			removed: []hostPortInfoParam{
				{"TCP", "127.0.0.1", 79},
				{"UDP", "127.0.0.1", 80},
				{"TCP", "127.0.0.1", 81},
				{"TCP", "127.0.0.1", 82},
				{"TCP", "0.0.0.0", 79},
				{"UDP", "0.0.0.0", 80},
				{"TCP", "0.0.0.0", 81},
				{"TCP", "0.0.0.0", 82},
			},
			length: 0,
		},
		{
			desc: "empty ip and protocol remove should work",
			added: []hostPortInfoParam{
				{"TCP", "127.0.0.1", 79},
				{"UDP", "127.0.0.1", 80},
				{"TCP", "127.0.0.1", 81},
				{"TCP", "127.0.0.1", 82},
				{"TCP", "0.0.0.0", 79},
				{"UDP", "0.0.0.0", 80},
				{"TCP", "0.0.0.0", 81},
				{"TCP", "0.0.0.0", 82},
			},
			removed: []hostPortInfoParam{
				{"", "127.0.0.1", 79},
				{"", "127.0.0.1", 81},
				{"", "127.0.0.1", 82},
				{"UDP", "127.0.0.1", 80},
				{"", "", 79},
				{"", "", 81},
				{"", "", 82},
				{"UDP", "", 80},
			},
			length: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			hp := make(HostPortInfo)
			for _, param := range test.added {
				hp.Add(param.ip, param.protocol, param.port)
			}
			for _, param := range test.removed {
				hp.Remove(param.ip, param.protocol, param.port)
			}
			if hp.Len() != test.length {
				t.Errorf("%v failed: expect length %d; got %d", test.desc, test.length, hp.Len())
				t.Error(hp)
			}
		})
	}
}

// 空 IP/协议代表“通配”，因此要同时覆盖两个方向：
// 通配检查能命中已登记的具体项，具体检查也能命中已登记的通配项。
func TestHostPortInfo_Check(t *testing.T) {
	tests := []struct {
		desc   string
		added  []hostPortInfoParam
		check  hostPortInfoParam
		expect bool
	}{
		{
			desc: "empty check should check 0.0.0.0 and TCP",
			added: []hostPortInfoParam{
				{"TCP", "127.0.0.1", 80},
			},
			check:  hostPortInfoParam{"", "", 81},
			expect: false,
		},
		{
			desc: "empty check should check 0.0.0.0 and TCP (conflicted)",
			added: []hostPortInfoParam{
				{"TCP", "127.0.0.1", 80},
			},
			check:  hostPortInfoParam{"", "", 80},
			expect: true,
		},
		{
			desc: "empty port check should pass",
			added: []hostPortInfoParam{
				{"TCP", "127.0.0.1", 80},
			},
			check:  hostPortInfoParam{"", "", 0},
			expect: false,
		},
		{
			desc: "0.0.0.0 should check all registered IPs",
			added: []hostPortInfoParam{
				{"TCP", "127.0.0.1", 80},
			},
			check:  hostPortInfoParam{"TCP", "0.0.0.0", 80},
			expect: true,
		},
		{
			desc: "0.0.0.0 with different protocol should be allowed",
			added: []hostPortInfoParam{
				{"UDP", "127.0.0.1", 80},
			},
			check:  hostPortInfoParam{"TCP", "0.0.0.0", 80},
			expect: false,
		},
		{
			desc: "0.0.0.0 with different port should be allowed",
			added: []hostPortInfoParam{
				{"TCP", "127.0.0.1", 79},
				{"TCP", "127.0.0.1", 81},
				{"TCP", "127.0.0.1", 82},
			},
			check:  hostPortInfoParam{"TCP", "0.0.0.0", 80},
			expect: false,
		},
		{
			desc: "normal ip should check all registered 0.0.0.0",
			added: []hostPortInfoParam{
				{"TCP", "0.0.0.0", 80},
			},
			check:  hostPortInfoParam{"TCP", "127.0.0.1", 80},
			expect: true,
		},
		{
			desc: "normal ip with different port/protocol should be allowed (0.0.0.0)",
			added: []hostPortInfoParam{
				{"TCP", "0.0.0.0", 79},
				{"UDP", "0.0.0.0", 80},
				{"TCP", "0.0.0.0", 81},
				{"TCP", "0.0.0.0", 82},
			},
			check:  hostPortInfoParam{"TCP", "127.0.0.1", 80},
			expect: false,
		},
		{
			desc: "normal ip with different port/protocol should be allowed",
			added: []hostPortInfoParam{
				{"TCP", "127.0.0.1", 79},
				{"UDP", "127.0.0.1", 80},
				{"TCP", "127.0.0.1", 81},
				{"TCP", "127.0.0.1", 82},
			},
			check:  hostPortInfoParam{"TCP", "127.0.0.1", 80},
			expect: false,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			hp := make(HostPortInfo)
			for _, param := range test.added {
				hp.Add(param.ip, param.protocol, param.port)
			}
			if hp.CheckConflict(test.check.ip, test.check.protocol, test.check.port) != test.expect {
				t.Errorf("expected %t; got %t", test.expect, !test.expect)
			}
		})
	}
}

// namespace 为空表示 Pod 自己的命名空间，namespaceSelector 存在时则由标签选出，
// 两者都为空时的默认行为最容易搞错，所以单列一个用例钉住。
func TestGetNamespacesFromPodAffinityTerm(t *testing.T) {
	tests := []struct {
		name string
		term *v1.PodAffinityTerm
		want sets.Set[string]
	}{
		{
			name: "podAffinityTerm_namespace_empty",
			term: &v1.PodAffinityTerm{},
			want: sets.Set[string]{metav1.NamespaceDefault: sets.Empty{}},
		},
		{
			name: "podAffinityTerm_namespace_not_empty",
			term: &v1.PodAffinityTerm{
				Namespaces: []string{metav1.NamespacePublic, metav1.NamespaceSystem},
			},
			want: sets.New(metav1.NamespacePublic, metav1.NamespaceSystem),
		},
		{
			name: "podAffinityTerm_namespace_selector_not_nil",
			term: &v1.PodAffinityTerm{
				NamespaceSelector: &metav1.LabelSelector{},
			},
			want: sets.Set[string]{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := getNamespacesFromPodAffinityTerm(&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "topologies_pod",
					Namespace: metav1.NamespaceDefault,
				},
			}, test.term)
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("Unexpected namespaces (-want, +got):\n%s", diff)
			}
		})
	}
}
