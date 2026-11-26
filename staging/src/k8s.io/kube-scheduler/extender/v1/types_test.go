/*
Copyright 2020 The Kubernetes Authors.

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

package v1

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestCompatibility 验证 extender/v1 中的类型能成功编码为 JSON 再解码回来，
// 包括字段名小写的情况；这些类型依赖 JSON tag，必须锁住行为一致性。
// @TODO(88634): 这些类型的 v2 应定义明确的 JSON tag，统一字段大小写规则。
// 这里逐一固定每种扩展协议结构体的 JSON 输出，而不是只测“能不能往返”：
// 这些类型没有显式 json tag，字段名大小写直接来自 Go 字段名，一旦改动就会破坏与既有 extender 的兼容性。
// 额外做一次小写化再反序列化，是因为历史上有些 extender 依赖大小写不敏感的匹配行为，
// 测试把它当作必须维持的对外契约锁住。
func TestCompatibility(t *testing.T) {
	testcases := []struct {
		emptyObj   interface{}
		obj        interface{}
		expectJSON string
	}{
		{
			emptyObj: &ExtenderPreemptionResult{},
			obj: &ExtenderPreemptionResult{
				NodeNameToMetaVictims: map[string]*MetaVictims{"foo": {Pods: []*MetaPod{{UID: "myuid"}}, NumPDBViolations: 1}},
			},
			expectJSON: `{"NodeNameToMetaVictims":{"foo":{"Pods":[{"UID":"myuid"}],"NumPDBViolations":1}}}`,
		},
		{
			emptyObj: &ExtenderPreemptionArgs{},
			obj: &ExtenderPreemptionArgs{
				Pod:                   &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "podname"}},
				NodeNameToVictims:     map[string]*Victims{"foo": {Pods: []*corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "podname"}}}, NumPDBViolations: 1}},
				NodeNameToMetaVictims: map[string]*MetaVictims{"foo": {Pods: []*MetaPod{{UID: "myuid"}}, NumPDBViolations: 1}},
			},
			expectJSON: `{"Pod":{"metadata":{"name":"podname"},"spec":{"containers":null},"status":{}},"NodeNameToVictims":{"foo":{"Pods":[{"metadata":{"name":"podname"},"spec":{"containers":null},"status":{}}],"NumPDBViolations":1}},"NodeNameToMetaVictims":{"foo":{"Pods":[{"UID":"myuid"}],"NumPDBViolations":1}}}`,
		},
		{
			emptyObj: &ExtenderArgs{},
			obj: &ExtenderArgs{
				Pod:       &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "podname"}},
				Nodes:     &corev1.NodeList{Items: []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "nodename"}}}},
				NodeNames: &[]string{"node1"},
			},
			expectJSON: `{"Pod":{"metadata":{"name":"podname"},"spec":{"containers":null},"status":{}},"Nodes":{"metadata":{},"items":[{"metadata":{"name":"nodename"},"spec":{},"status":{"daemonEndpoints":{"kubeletEndpoint":{"Port":0}},"nodeInfo":{"machineID":"","systemUUID":"","bootID":"","kernelVersion":"","osImage":"","containerRuntimeVersion":"","kubeletVersion":"","kubeProxyVersion":"","operatingSystem":"","architecture":""}}}]},"NodeNames":["node1"]}`,
		},
		{
			emptyObj: &ExtenderFilterResult{},
			obj: &ExtenderFilterResult{
				Nodes:                      &corev1.NodeList{Items: []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "nodename"}}}},
				NodeNames:                  &[]string{"node1"},
				FailedNodes:                FailedNodesMap{"foo": "bar"},
				FailedAndUnresolvableNodes: FailedNodesMap{"baz": "qux"},
				Error:                      "myerror",
			},
			expectJSON: `{"Nodes":{"metadata":{},"items":[{"metadata":{"name":"nodename"},"spec":{},"status":{"daemonEndpoints":{"kubeletEndpoint":{"Port":0}},"nodeInfo":{"machineID":"","systemUUID":"","bootID":"","kernelVersion":"","osImage":"","containerRuntimeVersion":"","kubeletVersion":"","kubeProxyVersion":"","operatingSystem":"","architecture":""}}}]},"NodeNames":["node1"],"FailedNodes":{"foo":"bar"},"FailedAndUnresolvableNodes":{"baz":"qux"},"Error":"myerror"}`,
		},
		{
			emptyObj: &ExtenderBindingArgs{},
			obj: &ExtenderBindingArgs{
				PodName:      "mypodname",
				PodNamespace: "mypodnamespace",
				PodUID:       types.UID("mypoduid"),
				Node:         "mynode",
			},
			expectJSON: `{"PodName":"mypodname","PodNamespace":"mypodnamespace","PodUID":"mypoduid","Node":"mynode"}`,
		},
		{
			emptyObj:   &ExtenderBindingResult{},
			obj:        &ExtenderBindingResult{Error: "myerror"},
			expectJSON: `{"Error":"myerror"}`,
		},
		{
			emptyObj:   &HostPriority{},
			obj:        &HostPriority{Host: "myhost", Score: 1},
			expectJSON: `{"Host":"myhost","Score":1}`,
		},
	}

	// 每个用例都跑三件事：确认编码格式、确认小写输入能解码、确认解码后与原对象完全一致。
	// 用空对象作为解码目标，避免原有字段值掩盖解码失败。
	for _, tc := range testcases {
		t.Run(reflect.TypeOf(tc.obj).String(), func(t *testing.T) {
			data, err := json.Marshal(tc.obj)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != tc.expectJSON {
				t.Fatalf("expected %s, got %s", tc.expectJSON, string(data))
			}
			if err := json.Unmarshal([]byte(strings.ToLower(string(data))), tc.emptyObj); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tc.emptyObj, tc.obj) {
				t.Fatalf("round-tripped case-insensitive diff: %s", cmp.Diff(tc.obj, tc.emptyObj))
			}
		})
	}
}
