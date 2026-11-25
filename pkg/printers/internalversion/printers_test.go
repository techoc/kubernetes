/*
Copyright 2014 The Kubernetes Authors.

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

package internalversion

import (
	"fmt"
	"math"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apiv1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/certificate/csr"
	"k8s.io/kubernetes/pkg/apis/admissionregistration"
	"k8s.io/kubernetes/pkg/apis/apiserverinternal"
	"k8s.io/kubernetes/pkg/apis/apps"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	"k8s.io/kubernetes/pkg/apis/batch"
	"k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/apis/coordination"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/discovery"
	"k8s.io/kubernetes/pkg/apis/flowcontrol"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	"k8s.io/kubernetes/pkg/apis/networking"
	nodeapi "k8s.io/kubernetes/pkg/apis/node"
	"k8s.io/kubernetes/pkg/apis/policy"
	"k8s.io/kubernetes/pkg/apis/rbac"
	resourceapis "k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/apis/storagemigration"
	"k8s.io/kubernetes/pkg/printers"
	"k8s.io/utils/ptr"
)

// containerRestartPolicyAlways 避免在测试中重复引用 api.ContainerRestartPolicyAlways，
// 保持测试代码简洁。
var containerRestartPolicyAlways = api.ContainerRestartPolicyAlways

// TestFormatResourceName 验证 formatResourceName 函数正确格式化资源名称。
// 测试空值、仅 Kind、Kind+Group 等组合，确保输出格式为 "kind/name" 或 "kind.group/name"。
func TestFormatResourceName(t *testing.T) {
	tests := []struct {
		kind schema.GroupKind
		name string
		want string
	}{
		{schema.GroupKind{}, "", ""},
		{schema.GroupKind{}, "name", "name"},
		{schema.GroupKind{Kind: "Kind"}, "", "kind/"}, // 实际不应出现这种输入
		{schema.GroupKind{Kind: "Kind"}, "name", "kind/name"},
		{schema.GroupKind{Group: "group", Kind: "Kind"}, "name", "kind.group/name"},
	}
	for _, tt := range tests {
		if got := formatResourceName(tt.kind, tt.name, true); got != tt.want {
			t.Errorf("formatResourceName(%q, %q) = %q, want %q", tt.kind, tt.name, got, tt.want)
		}
	}
}

// TestPrintHandler 是 printers.PrintHandler 的简易实现，仅记录 TableHandler 被调用的次数，
// 用于验证 AddHandlers 是否正确注册了所有 handler。
type TestPrintHandler struct {
	numCalls int
}

func (t *TestPrintHandler) TableHandler(columnDefinitions []metav1.TableColumnDefinition, printFunc interface{}) error {
	t.numCalls++
	return nil
}

func (t *TestPrintHandler) getNumCalls() int {
	return t.numCalls
}

// TestAllHandlers 验证 AddHandlers 函数确实向 handler 注册了至少一个 TableHandler。
// 如果 numCalls 为 0，说明 AddHandlers 没有调用任何 handler，注册逻辑有问题。
func TestAllHandlers(t *testing.T) {
	h := &TestPrintHandler{numCalls: 0}
	AddHandlers(h)
	if h.getNumCalls() == 0 {
		t.Error("TableHandler not called in AddHandlers")
	}
}

func TestPrintEvent(t *testing.T) {
	tests := []struct {
		event    api.Event
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		// 基础事件；不启用生成选项
		{
			event: api.Event{
				Source: api.EventSource{Component: "kubelet"},
				InvolvedObject: api.ObjectReference{
					Kind:      "Pod",
					Name:      "Pod Name",
					FieldPath: "spec.containers{foo}",
				},
				Reason:         "Event Reason",
				Message:        "Message Data",
				FirstTimestamp: metav1.Time{Time: time.Now().UTC().AddDate(0, 0, -3)},
				LastTimestamp:  metav1.Time{Time: time.Now().UTC().AddDate(0, 0, -2)},
				Count:          6,
				Type:           api.EventTypeNormal,
				ObjectMeta:     metav1.ObjectMeta{Name: "event1"},
			},
			options: printers.GenerateOptions{},
			// 列： Last Seen, Type, Reason, Object, Message
			expected: []metav1.TableRow{{Cells: []interface{}{"2d", "Normal", "Event Reason", "pod/Pod Name", "Message Data"}}},
		},
		// 基础事件；启用 Wide 生成选项
		{
			event: api.Event{
				Source: api.EventSource{
					Component: "kubelet",
					Host:      "Node1",
				},
				InvolvedObject: api.ObjectReference{
					Kind:      "Deployment",
					Name:      "Deployment Name",
					FieldPath: "spec.containers{foo}",
				},
				Reason:         "Event Reason",
				Message:        "Message Data",
				FirstTimestamp: metav1.Time{Time: time.Now().UTC().AddDate(0, 0, -3)},
				LastTimestamp:  metav1.Time{Time: time.Now().UTC().AddDate(0, 0, -2)},
				Count:          6,
				Type:           api.EventTypeWarning,
				ObjectMeta:     metav1.ObjectMeta{Name: "event2"},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Last Seen, Type, Reason, Object, Subobject, Message, First Seen, Count, Name
			expected: []metav1.TableRow{{Cells: []interface{}{"2d", "Warning", "Event Reason", "deployment/Deployment Name", "spec.containers{foo}", "kubelet, Node1", "Message Data", "3d", int64(6), "event2"}}},
		},
		// 基础事件；未设置 FirstTimestamp
		{
			event: api.Event{
				Source: api.EventSource{
					Component: "kubelet",
					Host:      "Node1",
				},
				InvolvedObject: api.ObjectReference{
					Kind:      "Deployment",
					Name:      "Deployment Name",
					FieldPath: "spec.containers{foo}",
				},
				Reason:        "Event Reason",
				Message:       "Message Data",
				EventTime:     metav1.MicroTime{Time: time.Now().UTC().AddDate(0, 0, -3)},
				LastTimestamp: metav1.Time{Time: time.Now().UTC().AddDate(0, 0, -3)},
				Count:         1,
				Type:          api.EventTypeWarning,
				ObjectMeta:    metav1.ObjectMeta{Name: "event3"},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Last Seen, Type, Reason, Object, Subobject, Message, First Seen, Count, Name
			expected: []metav1.TableRow{{Cells: []interface{}{"3d", "Warning", "Event Reason", "deployment/Deployment Name", "spec.containers{foo}", "kubelet, Node1", "Message Data", "3d", int64(1), "event3"}}},
		},
		// 基础事件；未设置 LastTimestamp
		{
			event: api.Event{
				Source: api.EventSource{
					Component: "kubelet",
					Host:      "Node1",
				},
				InvolvedObject: api.ObjectReference{
					Kind:      "Deployment",
					Name:      "Deployment Name",
					FieldPath: "spec.containers{foo}",
				},
				Reason:         "Event Reason",
				Message:        "Message Data",
				EventTime:      metav1.MicroTime{Time: time.Now().UTC().AddDate(0, 0, -3)},
				FirstTimestamp: metav1.Time{Time: time.Now().UTC().AddDate(0, 0, -3)},
				Count:          1,
				Type:           api.EventTypeWarning,
				ObjectMeta:     metav1.ObjectMeta{Name: "event4"},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Last Seen, Type, Reason, Object, Subobject, Message, First Seen, Count, Name
			expected: []metav1.TableRow{{Cells: []interface{}{"3d", "Warning", "Event Reason", "deployment/Deployment Name", "spec.containers{foo}", "kubelet, Node1", "Message Data", "3d", int64(1), "event4"}}},
		},
		// 基础事件；FirstTimestamp 与 LastTimestamp 均未设置
		{
			event: api.Event{
				Source: api.EventSource{
					Component: "kubelet",
					Host:      "Node1",
				},
				InvolvedObject: api.ObjectReference{
					Kind:      "Deployment",
					Name:      "Deployment Name",
					FieldPath: "spec.containers{foo}",
				},
				Reason:     "Event Reason",
				Message:    "Message Data",
				EventTime:  metav1.MicroTime{Time: time.Now().UTC().AddDate(0, 0, -3)},
				Count:      1,
				Type:       api.EventTypeWarning,
				ObjectMeta: metav1.ObjectMeta{Name: "event5"},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Last Seen, Type, Reason, Object, Subobject, Message, First Seen, Count, Name
			expected: []metav1.TableRow{{Cells: []interface{}{"3d", "Warning", "Event Reason", "deployment/Deployment Name", "spec.containers{foo}", "kubelet, Node1", "Message Data", "3d", int64(1), "event5"}}},
		},
		// 基础事件序列；FirstTimestamp、LastTimestamp 与 Count 均未设置
		{
			event: api.Event{
				Source: api.EventSource{
					Component: "kubelet",
					Host:      "Node1",
				},
				InvolvedObject: api.ObjectReference{
					Kind:      "Deployment",
					Name:      "Deployment Name",
					FieldPath: "spec.containers{foo}",
				},
				Series: &api.EventSeries{
					Count:            2,
					LastObservedTime: metav1.MicroTime{Time: time.Now().UTC().AddDate(0, 0, -2)},
				},
				Reason:     "Event Reason",
				Message:    "Message Data",
				EventTime:  metav1.MicroTime{Time: time.Now().UTC().AddDate(0, 0, -3)},
				Type:       api.EventTypeWarning,
				ObjectMeta: metav1.ObjectMeta{Name: "event6"},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Last Seen, Type, Reason, Object, Subobject, Message, First Seen, Count, Name
			expected: []metav1.TableRow{{Cells: []interface{}{"2d", "Warning", "Event Reason", "deployment/Deployment Name", "spec.containers{foo}", "kubelet, Node1", "Message Data", "3d", int64(2), "event6"}}},
		},
		// 单次事件；FirstTimestamp、LastTimestamp 与 Count 均未设置
		{
			event: api.Event{
				Source: api.EventSource{
					Component: "kubelet",
					Host:      "Node1",
				},
				InvolvedObject: api.ObjectReference{
					Kind:      "Deployment",
					Name:      "Deployment Name",
					FieldPath: "spec.containers{foo}",
				},
				Reason:     "Event Reason",
				Message:    "Message Data",
				EventTime:  metav1.MicroTime{Time: time.Now().UTC().AddDate(0, 0, -3)},
				Type:       api.EventTypeWarning,
				ObjectMeta: metav1.ObjectMeta{Name: "event7"},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Last Seen, Type, Reason, Object, Subobject, Message, First Seen, Count, Name
			expected: []metav1.TableRow{{Cells: []interface{}{"3d", "Warning", "Event Reason", "deployment/Deployment Name", "spec.containers{foo}", "kubelet, Node1", "Message Data", "3d", int64(1), "event7"}}},
		},
		// 基础事件；Source 为空且启用 Wide，验证来源字段的回退逻辑
		{
			event: api.Event{
				ReportingController: "kubelet",
				ReportingInstance:   "test",
				InvolvedObject: api.ObjectReference{
					Kind:      "Deployment",
					Name:      "Deployment Name",
					FieldPath: "spec.containers{foo}",
				},
				Reason:         "Event Reason",
				Message:        "Message Data",
				FirstTimestamp: metav1.Time{Time: time.Now().UTC().AddDate(0, 0, -3)},
				LastTimestamp:  metav1.Time{Time: time.Now().UTC().AddDate(0, 0, -2)},
				Count:          6,
				Type:           api.EventTypeWarning,
				ObjectMeta:     metav1.ObjectMeta{Name: "event2"},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Last Seen, Type, Reason, Object, Subobject, Source, Message, First Seen, Count, Name
			expected: []metav1.TableRow{{Cells: []interface{}{"2d", "Warning", "Event Reason", "deployment/Deployment Name", "spec.containers{foo}", "kubelet, test", "Message Data", "3d", int64(6), "event2"}}},
		},
	}

	for i, test := range tests {
		rows, err := printEvent(&test.event, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintEventsResultSorted(t *testing.T) {

	eventList := api.EventList{
		Items: []api.Event{
			{
				Source:         api.EventSource{Component: "kubelet"},
				Message:        "Item 1",
				FirstTimestamp: metav1.NewTime(time.Date(2014, time.January, 15, 0, 0, 0, 0, time.UTC)),
				LastTimestamp:  metav1.NewTime(time.Date(2014, time.January, 15, 0, 0, 0, 0, time.UTC)),
				Count:          1,
				Type:           api.EventTypeNormal,
			},
			{
				Source:         api.EventSource{Component: "scheduler"},
				Message:        "Item 2",
				FirstTimestamp: metav1.NewTime(time.Date(1987, time.June, 17, 0, 0, 0, 0, time.UTC)),
				LastTimestamp:  metav1.NewTime(time.Date(1987, time.June, 17, 0, 0, 0, 0, time.UTC)),
				Count:          1,
				Type:           api.EventTypeNormal,
			},
			{
				Source:         api.EventSource{Component: "kubelet"},
				Message:        "Item 3",
				FirstTimestamp: metav1.NewTime(time.Date(2002, time.December, 25, 0, 0, 0, 0, time.UTC)),
				LastTimestamp:  metav1.NewTime(time.Date(2002, time.December, 25, 0, 0, 0, 0, time.UTC)),
				Count:          1,
				Type:           api.EventTypeNormal,
			},
		},
	}

	rows, err := printEventList(&eventList, printers.GenerateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Errorf("Generate Event List: Wrong number of table rows returned. Expected 3, got (%d)", len(rows))
	}
	// 验证 watch 事件的时间顺序。
	firstRow := rows[0]
	message1 := firstRow.Cells[4]
	if message1.(string) != "Item 1" {
		t.Errorf("Wrong event ordering: expecting (Item 1), got (%s)", message1)
	}
	secondRow := rows[1]
	message2 := secondRow.Cells[4]
	if message2 != "Item 2" {
		t.Errorf("Wrong event ordering: expecting (Item 2), got (%s)", message2)
	}
	thirdRow := rows[2]
	message3 := thirdRow.Cells[4]
	if message3 != "Item 3" {
		t.Errorf("Wrong event ordering: expecting (Item 3), got (%s)", message3)
	}
}

func TestPrintNamespace(t *testing.T) {
	tests := []struct {
		namespace api.Namespace
		expected  []metav1.TableRow
	}{
		// 基础 Namespace；包含状态和年龄
		{
			namespace: api.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "namespace1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Status: api.NamespaceStatus{
					Phase: "FooStatus",
				},
			},
			// 列： Name, Status, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"namespace1", "FooStatus", "0s"}}},
		},
		// 基础 Namespace；缺少状态或年龄
		{
			namespace: api.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace2",
				},
			},
			// 列： Name, Status, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"namespace2", "", "<unknown>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printNamespace(&test.namespace, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintSecret(t *testing.T) {
	tests := []struct {
		secret   api.Secret
		expected []metav1.TableRow
	}{
		// 基础 Secret；包含类型、数据和年龄
		{
			secret: api.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "secret1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Type: "kubernetes.io/service-account-token",
				Data: map[string][]byte{
					"token": []byte("secret data"),
				},
			},
			// 列： Name, Type, Data, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"secret1", "kubernetes.io/service-account-token", int64(1), "0s"}}},
		},
		// 基础 Secret；包含类型和年龄，但没有数据
		{
			secret: api.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "secret1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Type: "kubernetes.io/service-account-token",
			},
			// 列： Name, Type, Data, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"secret1", "kubernetes.io/service-account-token", int64(0), "0s"}}},
		},
	}

	for i, test := range tests {
		rows, err := printSecret(&test.secret, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintServiceAccount(t *testing.T) {
	tests := []struct {
		serviceAccount api.ServiceAccount
		expected       []metav1.TableRow
	}{
		// 无 Secret 的基础 ServiceAccount；当前表格仅展示名称和年龄
		{
			serviceAccount: api.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "sa1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Secrets: []api.ObjectReference{},
			},
			// 列： Name, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"sa1", "0s"}}},
		},
		// 带两个 Secret 的 ServiceAccount；验证密钥不占表格列
		{
			serviceAccount: api.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "sa1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Secrets: []api.ObjectReference{
					{Name: "Secret1"},
					{Name: "Secret2"},
				},
			},
			// 列： Name, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"sa1", "0s"}}},
		},
	}

	for i, test := range tests {
		rows, err := printServiceAccount(&test.serviceAccount, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintNodeStatus(t *testing.T) {

	table := []struct {
		node     api.Node
		expected []metav1.TableRow
	}{
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
				Status:     api.NodeStatus{Conditions: []api.NodeCondition{{Type: api.NodeReady, Status: api.ConditionTrue}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo1", "Ready", "<none>", "<unknown>", ""}}},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo2"},
				Spec:       api.NodeSpec{Unschedulable: true},
				Status:     api.NodeStatus{Conditions: []api.NodeCondition{{Type: api.NodeReady, Status: api.ConditionTrue}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo2", "Ready,SchedulingDisabled", "<none>", "<unknown>", ""}}},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo3"},
				Status: api.NodeStatus{Conditions: []api.NodeCondition{
					{Type: api.NodeReady, Status: api.ConditionTrue},
					{Type: api.NodeReady, Status: api.ConditionTrue}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo3", "Ready", "<none>", "<unknown>", ""}}},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo4"},
				Status:     api.NodeStatus{Conditions: []api.NodeCondition{{Type: api.NodeReady, Status: api.ConditionFalse}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo4", "NotReady", "<none>", "<unknown>", ""}}},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo5"},
				Spec:       api.NodeSpec{Unschedulable: true},
				Status:     api.NodeStatus{Conditions: []api.NodeCondition{{Type: api.NodeReady, Status: api.ConditionFalse}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo5", "NotReady,SchedulingDisabled", "<none>", "<unknown>", ""}}},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo6"},
				Status:     api.NodeStatus{Conditions: []api.NodeCondition{{Type: "InvalidValue", Status: api.ConditionTrue}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo6", "Unknown", "<none>", "<unknown>", ""}}},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo7"},
				Status:     api.NodeStatus{Conditions: []api.NodeCondition{{}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo7", "Unknown", "<none>", "<unknown>", ""}}},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo8"},
				Spec:       api.NodeSpec{Unschedulable: true},
				Status:     api.NodeStatus{Conditions: []api.NodeCondition{{Type: "InvalidValue", Status: api.ConditionTrue}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo8", "Unknown,SchedulingDisabled", "<none>", "<unknown>", ""}}},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo9"},
				Spec:       api.NodeSpec{Unschedulable: true},
				Status:     api.NodeStatus{Conditions: []api.NodeCondition{{}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo9", "Unknown,SchedulingDisabled", "<none>", "<unknown>", ""}}},
		},
	}

	for i, test := range table {
		rows, err := printNode(&test.node, printers.GenerateOptions{})
		if err != nil {
			t.Fatalf("Error generating table rows for Node: %#v", err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintNodeRole(t *testing.T) {

	table := []struct {
		node     api.Node
		expected []metav1.TableRow
	}{
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo9"},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo9", "Unknown", "<none>", "<unknown>", ""}}},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "foo10",
					Labels: map[string]string{"node-role.kubernetes.io/master": "", "node-role.kubernetes.io/control-plane": "", "node-role.kubernetes.io/proxy": "", "kubernetes.io/role": "node"},
				},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo10", "Unknown", "control-plane,master,node,proxy", "<unknown>", ""}}},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "foo11",
					Labels: map[string]string{"kubernetes.io/role": "node"},
				},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"foo11", "Unknown", "node", "<unknown>", ""}}},
		},
	}

	for i, test := range table {
		rows, err := printNode(&test.node, printers.GenerateOptions{})
		if err != nil {
			t.Fatalf("An error occurred generating table rows for Node: %#v", err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintNodeOSImage(t *testing.T) {

	table := []struct {
		node     api.Node
		expected []metav1.TableRow
	}{
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
				Status: api.NodeStatus{
					NodeInfo:  api.NodeSystemInfo{OSImage: "fake-os-image"},
					Addresses: []api.NodeAddress{{Type: api.NodeExternalIP, Address: "1.1.1.1"}},
				},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo1", "Unknown", "<none>", "<unknown>", "", "<none>", "1.1.1.1", "fake-os-image", "<unknown>", "<unknown>"},
				},
			},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo2"},
				Status: api.NodeStatus{
					NodeInfo:  api.NodeSystemInfo{KernelVersion: "fake-kernel-version"},
					Addresses: []api.NodeAddress{{Type: api.NodeExternalIP, Address: "1.1.1.1"}},
				},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo2", "Unknown", "<none>", "<unknown>", "", "<none>", "1.1.1.1", "<unknown>", "fake-kernel-version", "<unknown>"},
				},
			},
		},
	}

	for i, test := range table {
		rows, err := printNode(&test.node, printers.GenerateOptions{Wide: true})
		if err != nil {
			t.Fatalf("An error occurred generating table for Node: %#v", err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintNodeKernelVersion(t *testing.T) {

	table := []struct {
		node     api.Node
		expected []metav1.TableRow
	}{
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
				Status: api.NodeStatus{
					NodeInfo:  api.NodeSystemInfo{KernelVersion: "fake-kernel-version"},
					Addresses: []api.NodeAddress{{Type: api.NodeExternalIP, Address: "1.1.1.1"}},
				},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo1", "Unknown", "<none>", "<unknown>", "", "<none>", "1.1.1.1", "<unknown>", "fake-kernel-version", "<unknown>"},
				},
			},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo2"},
				Status: api.NodeStatus{
					NodeInfo:  api.NodeSystemInfo{OSImage: "fake-os-image"},
					Addresses: []api.NodeAddress{{Type: api.NodeExternalIP, Address: "1.1.1.1"}},
				},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo2", "Unknown", "<none>", "<unknown>", "", "<none>", "1.1.1.1", "fake-os-image", "<unknown>", "<unknown>"},
				},
			},
		},
	}

	for i, test := range table {
		rows, err := printNode(&test.node, printers.GenerateOptions{Wide: true})
		if err != nil {
			t.Fatalf("An error occurred generating table rows Node: %#v", err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintNodeArch(t *testing.T) {

	table := []struct {
		node     api.Node
		expected []metav1.TableRow
	}{
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
				Status: api.NodeStatus{
					NodeInfo:  api.NodeSystemInfo{KernelVersion: "fake-kernel-version", Architecture: "fake-arch-amd64"},
					Addresses: []api.NodeAddress{{Type: api.NodeExternalIP, Address: "1.1.1.1"}},
				},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					// 情形 1：KernelVersion 与 Arch 均非空
					Cells: []interface{}{"foo", "Unknown", "<none>", "<unknown>", "", "<none>", "1.1.1.1", "<unknown>", "fake-kernel-version (fake-arch-amd64)", "<unknown>"},
				},
			},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
				Status: api.NodeStatus{
					NodeInfo:  api.NodeSystemInfo{Architecture: "fake-arch-arm64"},
					Addresses: []api.NodeAddress{{Type: api.NodeExternalIP, Address: "1.1.1.1"}},
				},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					// 情形 2：KernelVersion 为空，但 Arch 非空
					Cells: []interface{}{"foo", "Unknown", "<none>", "<unknown>", "", "<none>", "1.1.1.1", "<unknown>", "<unknown> (fake-arch-arm64)", "<unknown>"},
				},
			},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
				Status: api.NodeStatus{
					NodeInfo:  api.NodeSystemInfo{KernelVersion: "fake-kernel-version"},
					Addresses: []api.NodeAddress{{Type: api.NodeExternalIP, Address: "1.1.1.1"}},
				},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					// 情形 3：KernelVersion 非空，但 Arch 为空
					Cells: []interface{}{"foo", "Unknown", "<none>", "<unknown>", "", "<none>", "1.1.1.1", "<unknown>", "fake-kernel-version", "<unknown>"},
				},
			},
		},
	}

	for i, test := range table {
		rows, err := printNode(&test.node, printers.GenerateOptions{Wide: true})
		if err != nil {
			t.Fatalf("An error occurred generating table rows Node: %#v", err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintNodeContainerRuntimeVersion(t *testing.T) {

	table := []struct {
		node     api.Node
		expected []metav1.TableRow
	}{
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
				Status: api.NodeStatus{
					NodeInfo:  api.NodeSystemInfo{ContainerRuntimeVersion: "foo://1.2.3"},
					Addresses: []api.NodeAddress{{Type: api.NodeExternalIP, Address: "1.1.1.1"}},
				},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo1", "Unknown", "<none>", "<unknown>", "", "<none>", "1.1.1.1", "<unknown>", "<unknown>", "foo://1.2.3"},
				},
			},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo2"},
				Status: api.NodeStatus{
					NodeInfo:  api.NodeSystemInfo{},
					Addresses: []api.NodeAddress{{Type: api.NodeExternalIP, Address: "1.1.1.1"}},
				},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo2", "Unknown", "<none>", "<unknown>", "", "<none>", "1.1.1.1", "<unknown>", "<unknown>", "<unknown>"},
				},
			},
		},
	}

	for i, test := range table {
		rows, err := printNode(&test.node, printers.GenerateOptions{Wide: true})
		if err != nil {
			t.Fatalf("An error occurred generating table rows Node: %#v", err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintNodeName(t *testing.T) {

	table := []struct {
		node     api.Node
		expected []metav1.TableRow
	}{
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "127.0.0.1"},
				Status:     api.NodeStatus{},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"127.0.0.1", "Unknown", "<none>", "<unknown>", ""}}}},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: ""},
				Status:     api.NodeStatus{},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion
			expected: []metav1.TableRow{{Cells: []interface{}{"", "Unknown", "<none>", "<unknown>", ""}}},
		},
	}

	for i, test := range table {
		rows, err := printNode(&test.node, printers.GenerateOptions{})
		if err != nil {
			t.Fatalf("An error occurred generating table rows Node: %#v", err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintNodeExternalIP(t *testing.T) {

	table := []struct {
		node     api.Node
		expected []metav1.TableRow
	}{
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
				Status:     api.NodeStatus{Addresses: []api.NodeAddress{{Type: api.NodeExternalIP, Address: "1.1.1.1"}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo1", "Unknown", "<none>", "<unknown>", "", "<none>", "1.1.1.1", "<unknown>", "<unknown>", "<unknown>"},
				},
			},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo2"},
				Status:     api.NodeStatus{Addresses: []api.NodeAddress{{Type: api.NodeInternalIP, Address: "1.1.1.1"}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo2", "Unknown", "<none>", "<unknown>", "", "1.1.1.1", "<none>", "<unknown>", "<unknown>", "<unknown>"},
				},
			},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo3"},
				Status: api.NodeStatus{Addresses: []api.NodeAddress{
					{Type: api.NodeExternalIP, Address: "2.2.2.2"},
					{Type: api.NodeInternalIP, Address: "3.3.3.3"},
					{Type: api.NodeExternalIP, Address: "4.4.4.4"},
				}},
			},
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo3", "Unknown", "<none>", "<unknown>", "", "3.3.3.3", "2.2.2.2", "<unknown>", "<unknown>", "<unknown>"},
				},
			},
		},
	}

	for i, test := range table {
		rows, err := printNode(&test.node, printers.GenerateOptions{Wide: true})
		if err != nil {
			t.Fatalf("An error occurred generating table rows Node: %#v", err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintNodeInternalIP(t *testing.T) {

	table := []struct {
		node     api.Node
		expected []metav1.TableRow
	}{
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo1"},
				Status:     api.NodeStatus{Addresses: []api.NodeAddress{{Type: api.NodeInternalIP, Address: "1.1.1.1"}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo1", "Unknown", "<none>", "<unknown>", "", "1.1.1.1", "<none>", "<unknown>", "<unknown>", "<unknown>"},
				},
			},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo2"},
				Status:     api.NodeStatus{Addresses: []api.NodeAddress{{Type: api.NodeExternalIP, Address: "1.1.1.1"}}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo2", "Unknown", "<none>", "<unknown>", "", "<none>", "1.1.1.1", "<unknown>", "<unknown>", "<unknown>"},
				},
			},
		},
		{
			node: api.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "foo3"},
				Status: api.NodeStatus{Addresses: []api.NodeAddress{
					{Type: api.NodeInternalIP, Address: "2.2.2.2"},
					{Type: api.NodeExternalIP, Address: "3.3.3.3"},
					{Type: api.NodeInternalIP, Address: "4.4.4.4"},
				}},
			},
			// 列： Name, Status, Roles, Age, KubeletVersion, NodeInternalIP, NodeExternalIP, OSImage, KernelVersion, ContainerRuntimeVersion
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"foo3", "Unknown", "<none>", "<unknown>", "", "2.2.2.2", "3.3.3.3", "<unknown>", "<unknown>", "<unknown>"},
				},
			},
		},
	}

	for i, test := range table {
		rows, err := printNode(&test.node, printers.GenerateOptions{Wide: true})
		if err != nil {
			t.Fatalf("An error occurred generating table rows Node: %#v", err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintIngress(t *testing.T) {
	ingress := networking.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test1",
			CreationTimestamp: metav1.Time{Time: time.Now().Add(time.Duration(-10 * 365 * 24 * time.Hour))},
		},
		Spec: networking.IngressSpec{
			IngressClassName: ptr.To("foo"),
			DefaultBackend: &networking.IngressBackend{
				Service: &networking.IngressServiceBackend{
					Name: "default-backend",
					Port: networking.ServiceBackendPort{
						Name:   "default-backend",
						Number: 80,
					},
				},
			},
		},
		Status: networking.IngressStatus{
			LoadBalancer: networking.IngressLoadBalancerStatus{
				Ingress: []networking.IngressLoadBalancerIngress{
					{
						IP:       "2.3.4.5",
						Hostname: "localhost.localdomain",
					},
				},
			},
		},
	}
	// 列： Name, Hosts, Address, Ports, Age
	expected := []metav1.TableRow{{Cells: []interface{}{"test1", "foo", "*", "2.3.4.5", "80", "10y"}}}

	rows, err := printIngress(&ingress, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error generating table rows for Ingress: %#v", err)
	}
	rows[0].Object.Object = nil
	if !reflect.DeepEqual(expected, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expected, rows))
	}
}

func TestPrintIngressClass(t *testing.T) {
	testCases := []struct {
		name         string
		ingressClass *networking.IngressClass
		expected     []metav1.TableRow
	}{{
		name: "example with params",
		ingressClass: &networking.IngressClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test1",
				CreationTimestamp: metav1.Time{Time: time.Now().Add(time.Duration(-10 * 365 * 24 * time.Hour))},
			},
			Spec: networking.IngressClassSpec{
				Controller: "example.com/controller",
				Parameters: &networking.IngressClassParametersReference{Kind: "customgroup", Name: "example"},
			},
		},
		expected: []metav1.TableRow{{Cells: []interface{}{"test1", "example.com/controller", "customgroup/example", "10y"}}},
	}, {
		name: "example with params + API Group",
		ingressClass: &networking.IngressClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test1",
				CreationTimestamp: metav1.Time{Time: time.Now().Add(time.Duration(-10 * 365 * 24 * time.Hour))},
			},
			Spec: networking.IngressClassSpec{
				Controller: "example.com/controller",
				Parameters: &networking.IngressClassParametersReference{
					APIGroup: ptr.To("example.com"),
					Kind:     "customgroup",
					Name:     "example",
				},
			},
		},
		expected: []metav1.TableRow{{Cells: []interface{}{"test1", "example.com/controller", "customgroup.example.com/example", "10y"}}},
	}, {
		name: "example without params",
		ingressClass: &networking.IngressClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test2",
				CreationTimestamp: metav1.Time{Time: time.Now().Add(time.Duration(-11 * 365 * 24 * time.Hour))},
			},
			Spec: networking.IngressClassSpec{
				Controller: "example.com/controller2",
			},
		},
		expected: []metav1.TableRow{{Cells: []interface{}{"test2", "example.com/controller2", "<none>", "11y"}}},
	}, {
		name: "example with default annotation",
		ingressClass: &networking.IngressClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test-default",
				CreationTimestamp: metav1.Time{Time: time.Now().Add(time.Duration(-9 * 365 * 24 * time.Hour))},
				Annotations: map[string]string{
					networkingv1.AnnotationIsDefaultIngressClass: "true",
				},
			},
			Spec: networking.IngressClassSpec{
				Controller: "example.com/controller",
			},
		},
		expected: []metav1.TableRow{{Cells: []interface{}{"test-default (default)", "example.com/controller", "<none>", "9y"}}},
	}}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rows, err := printIngressClass(testCase.ingressClass, printers.GenerateOptions{})
			if err != nil {
				t.Fatalf("Error generating table rows for Ingress: %#v", err)
			}
			for i := range rows {
				rows[i].Object.Object = nil
			}
			if !reflect.DeepEqual(testCase.expected, rows) {
				t.Errorf("mismatch: %s", cmp.Diff(testCase.expected, rows))
			}
		})
	}
}

func TestPrintServiceLoadBalancer(t *testing.T) {
	tests := []struct {
		service  api.Service
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		// 验证多个外部 IP 去重后按字典序展示。
		{
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "service1"},
				Spec: api.ServiceSpec{
					ClusterIPs: []string{"1.2.3.4"},
					Type:       "LoadBalancer",
					Ports:      []api.ServicePort{{Port: 80, Protocol: "TCP"}},
				},
				Status: api.ServiceStatus{
					LoadBalancer: api.LoadBalancerStatus{
						Ingress: []api.LoadBalancerIngress{{IP: "2.3.4.5"}, {IP: "3.4.5.6"}}},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"service1", "LoadBalancer", "1.2.3.4", "2.3.4.5,3.4.5.6", "80/TCP", "<unknown>"}}},
		},
		// 验证负载均衡外部 IP 尚未分配时展示 <pending>。
		{
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "service2"},
				Spec: api.ServiceSpec{
					ClusterIPs: []string{"1.3.4.5"},
					Type:       "LoadBalancer",
					Ports:      []api.ServicePort{{Port: 80, Protocol: "TCP"}, {Port: 8090, Protocol: "UDP"}, {Port: 8000, Protocol: "TCP"}, {Port: 7777, Protocol: "SCTP"}},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"service2", "LoadBalancer", "1.3.4.5", "<pending>", "80/TCP,8090/UDP,8000/TCP,7777/SCTP", "<unknown>"}}},
		},
		// 验证同一服务的多个端口按定义顺序展示。
		{
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "service3"},
				Spec: api.ServiceSpec{
					ClusterIPs: []string{"1.4.5.6"},
					Type:       "LoadBalancer",
					Ports:      []api.ServicePort{{Port: 80, Protocol: "TCP"}, {Port: 8090, Protocol: "UDP"}, {Port: 8000, Protocol: "TCP"}},
				},
				Status: api.ServiceStatus{
					LoadBalancer: api.LoadBalancerStatus{
						Ingress: []api.LoadBalancerIngress{{IP: "2.3.4.5"}}},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"service3", "LoadBalancer", "1.4.5.6", "2.3.4.5", "80/TCP,8090/UDP,8000/TCP", "<unknown>"}}},
		},
		// 验证普通模式截断过长的外部 IP 列表。
		{
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "service4"},
				Spec: api.ServiceSpec{
					ClusterIPs: []string{"1.5.6.7"},
					Type:       "LoadBalancer",
					Ports:      []api.ServicePort{{Port: 80, Protocol: "TCP"}, {Port: 8090, Protocol: "UDP"}, {Port: 8000, Protocol: "TCP"}},
				},
				Status: api.ServiceStatus{
					LoadBalancer: api.LoadBalancerStatus{
						Ingress: []api.LoadBalancerIngress{{IP: "2.3.4.5"}, {IP: "3.4.5.6"}, {IP: "5.6.7.8", Hostname: "host5678"}}},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"service4", "LoadBalancer", "1.5.6.7", "2.3.4.5,3.4.5...", "80/TCP,8090/UDP,8000/TCP", "<unknown>"}}},
		},
		// 启用 Wide，额外展示选择器。
		{
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "service4"},
				Spec: api.ServiceSpec{
					ClusterIPs: []string{"1.5.6.7"},
					Type:       "LoadBalancer",
					Ports:      []api.ServicePort{{Port: 80, Protocol: "TCP"}, {Port: 8090, Protocol: "UDP"}, {Port: 8000, Protocol: "TCP"}},
					Selector:   map[string]string{"foo": "bar"},
				},
				Status: api.ServiceStatus{
					LoadBalancer: api.LoadBalancerStatus{
						Ingress: []api.LoadBalancerIngress{{IP: "2.3.4.5"}, {IP: "3.4.5.6"}, {IP: "5.6.7.8", Hostname: "host5678"}}},
				},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"service4", "LoadBalancer", "1.5.6.7", "2.3.4.5,3.4.5.6,5.6.7.8", "80/TCP,8090/UDP,8000/TCP", "<unknown>", "foo=bar"}}},
		},
	}

	for i, test := range tests {
		rows, err := printService(&test.service, test.options)
		if err != nil {
			t.Fatalf("Error printing table rows for Service: %#v", err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintPod(t *testing.T) {
	deleteTime := metav1.NewTime(time.Now().Add(-10 * time.Second))
	tests := []struct {
		pod    api.Pod
		expect []metav1.TableRow
	}{
		{
			// 验证名称、容器数、重启次数和容器就绪状态
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test1"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: "podPhase",
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{RestartCount: 3},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test1", "1/2", "podPhase", "6", "<unknown>"}}},
		},
		{
			// 验证容器错误优先于 Pod 阶段展示
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test2"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: "podPhase",
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{State: api.ContainerState{Waiting: &api.ContainerStateWaiting{Reason: "ContainerWaitingReason"}}, RestartCount: 3},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test2", "1/2", "ContainerWaitingReason", "6", "<unknown>"}}},
		},
		{
			// 将上一场景改为 Terminated，验证倒序扫描时首个容器状态覆盖后续容器
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test3"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: "podPhase",
					ContainerStatuses: []api.ContainerStatus{
						{State: api.ContainerState{Waiting: &api.ContainerStateWaiting{Reason: "ContainerWaitingReason"}}, RestartCount: 3},
						{State: api.ContainerState{Terminated: &api.ContainerStateTerminated{Reason: "ContainerTerminatedReason"}}, RestartCount: 3},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test3", "0/2", "ContainerWaitingReason", "6", "<unknown>"}}},
		},
		{
			// 验证仅 Ready 不足以判定容器正在运行
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test4"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: "podPhase",
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{Ready: true, RestartCount: 3},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test4", "1/2", "podPhase", "6", "<unknown>"}}},
		},
		{
			// 验证仅 Ready 不足以判定容器正在运行
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test5"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Reason: "podReason",
					Phase:  "podPhase",
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{Ready: true, RestartCount: 3},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test5", "1/2", "podReason", "6", "<unknown>"}}},
		},
		{
			// 两个容器分别为 Running、Completed，且无就绪条件
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test6"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase:  "Running",
					Reason: "",
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Terminated: &api.ContainerStateTerminated{Reason: "Completed", ExitCode: 0}}},
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test6", "1/2", "NotReady", "6", "<unknown>"}}},
		},
		{
			// 两个容器分别为 Running、Completed，且有就绪条件，验证 Completed 被修正为 Running
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test6"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase:  "Running",
					Reason: "",
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Terminated: &api.ContainerStateTerminated{Reason: "Completed", ExitCode: 0}}},
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
					},
					Conditions: []api.PodCondition{
						{Type: api.PodReady, Status: api.ConditionTrue, LastProbeTime: metav1.Time{Time: time.Now()}},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test6", "1/2", "Running", "6", "<unknown>"}}},
		},
		{
			// 一个初始化容器重启、一个业务容器未运行，验证初始化状态优先
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test7"},
				Spec:       api.PodSpec{InitContainers: make([]api.Container, 1), Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: "podPhase",
					InitContainerStatuses: []api.ContainerStatus{
						{
							Ready:                false,
							RestartCount:         3,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-10 * time.Second))}},
						},
					},
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready:        false,
							RestartCount: 0,
							State:        api.ContainerState{Waiting: &api.ContainerStateWaiting{}},
						},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test7", "0/1", "Init:0/1", "3 (10s ago)", "<unknown>"}}},
		},
		{
			// 两个初始化容器分别重启、未运行，一个业务容器未运行
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test8"},
				Spec:       api.PodSpec{InitContainers: make([]api.Container, 2), Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: "podPhase",
					InitContainerStatuses: []api.ContainerStatus{
						{
							Ready:                false,
							RestartCount:         3,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-10 * time.Second))}},
						},
						{
							Ready: false,
							State: api.ContainerState{Waiting: &api.ContainerStateWaiting{}},
						},
					},
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready: false,
							State: api.ContainerState{Waiting: &api.ContainerStateWaiting{}},
						},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test8", "0/1", "Init:0/2", "3 (10s ago)", "<unknown>"}}},
		},
		{
			// 两个初始化容器分别无重启完成、正在重启，一个业务容器未运行
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test9"},
				Spec:       api.PodSpec{InitContainers: make([]api.Container, 2), Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: "podPhase",
					InitContainerStatuses: []api.ContainerStatus{
						{
							Ready: false,
							State: api.ContainerState{Terminated: &api.ContainerStateTerminated{}},
						},
						{
							Ready:                false,
							RestartCount:         3,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-10 * time.Second))}},
						},
					},
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready: false,
							State: api.ContainerState{Waiting: &api.ContainerStateWaiting{}},
						},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test9", "0/1", "Init:1/2", "3 (10s ago)", "<unknown>"}}},
		},
		{
			// 两个初始化容器分别重启后完成、正在重启，一个业务容器未运行
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test10"},
				Spec:       api.PodSpec{InitContainers: make([]api.Container, 2), Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: "podPhase",
					InitContainerStatuses: []api.ContainerStatus{
						{
							Ready:                false,
							RestartCount:         2,
							State:                api.ContainerState{Terminated: &api.ContainerStateTerminated{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-2 * time.Minute))}},
						},
						{
							Ready:                false,
							RestartCount:         3,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-10 * time.Second))}},
						},
					},
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready: false,
							State: api.ContainerState{Waiting: &api.ContainerStateWaiting{}},
						},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test10", "0/1", "Init:1/2", "5 (10s ago)", "<unknown>"}}},
		},
		{
			// 初始化容器重启后完成，业务容器正在重启
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test11"},
				Spec:       api.PodSpec{InitContainers: make([]api.Container, 1), Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: "Running",
					InitContainerStatuses: []api.ContainerStatus{
						{
							Ready:                false,
							RestartCount:         2,
							State:                api.ContainerState{Terminated: &api.ContainerStateTerminated{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-2 * time.Minute))}},
						},
					},
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready:                false,
							RestartCount:         4,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-20 * time.Second))}},
						},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test11", "0/1", "Running", "4 (20s ago)", "<unknown>"}}},
		},
		{
			// 单个容器在 5 天前重启，验证相对时间展示
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test12"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: "Running",
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready:                true,
							RestartCount:         3,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-5 * 24 * time.Hour))}},
						},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test12", "1/1", "Running", "3 (5d ago)", "<unknown>"}}},
		},
		{
			// 两个容器分别从未重启、10 天前重启
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test13"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: "Running",
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready:        true,
							RestartCount: 0,
							State:        api.ContainerState{Running: &api.ContainerStateRunning{}},
						},
						{
							Ready:                true,
							RestartCount:         3,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-10 * 24 * time.Hour))}},
						},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test13", "2/2", "Running", "3 (10d ago)", "<unknown>"}}},
		},
		{
			// 两个容器分别在 5 天、20 天前重启，验证展示最近一次重启
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test14"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: "Running",
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready:                true,
							RestartCount:         6,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-5 * 24 * time.Hour))}},
						},
						{
							Ready:                true,
							RestartCount:         3,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-20 * 24 * time.Hour))}},
						},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test14", "2/2", "Running", "9 (5d ago)", "<unknown>"}}},
		},
		{
			// 验证 PodScheduled 的 SchedulingGated 原因优先于阶段展示
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test15"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: "podPhase",
					Conditions: []api.PodCondition{
						{
							Type:   api.PodScheduled,
							Status: api.ConditionFalse,
							Reason: apiv1.PodReasonSchedulingGated,
						},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test15", "0/2", apiv1.PodReasonSchedulingGated, "0", "<unknown>"}}},
		},
		{
			// 验证成功 Pod 附带完成条件
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test16"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: api.PodSucceeded,
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready: false,
							State: api.ContainerState{Terminated: &api.ContainerStateTerminated{Reason: "Completed", ExitCode: 0}},
						},
					},
				},
			},
			[]metav1.TableRow{{Conditions: podSuccessConditions, Cells: []interface{}{"test16", "0/1", "Completed", "0", "<unknown>"}}},
		},
		{
			// 验证失败 Pod 附带完成条件
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test17"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: api.PodFailed,
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready: false,
							State: api.ContainerState{Terminated: &api.ContainerStateTerminated{Reason: "Error", ExitCode: 1}},
						},
					},
				},
			},
			[]metav1.TableRow{{Conditions: podFailedConditions, Cells: []interface{}{"test17", "0/1", "Error", "0", "<unknown>"}}},
		},
		{
			// 验证删除中的成功 Pod 仍保留终态，不改写为 Terminating
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test18", DeletionTimestamp: &deleteTime},
				Spec:       api.PodSpec{Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: api.PodSucceeded,
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready: false,
							State: api.ContainerState{Terminated: &api.ContainerStateTerminated{Reason: "Completed", ExitCode: 0}},
						},
					},
				},
			},
			[]metav1.TableRow{{Conditions: podSuccessConditions, Cells: []interface{}{"test18", "0/1", "Completed", "0", "<unknown>"}}},
		},
		{
			// 验证删除中的运行 Pod 展示为 Terminating
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test19", DeletionTimestamp: &deleteTime},
				Spec:       api.PodSpec{Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: api.PodRunning,
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready: false,
							State: api.ContainerState{Running: &api.ContainerStateRunning{}},
						},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test19", "0/1", "Terminating", "0", "<unknown>"}}},
		},
		{ // 验证删除中的待处理 Pod 展示为 Terminating
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test20", DeletionTimestamp: &deleteTime},
				Spec:       api.PodSpec{Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: api.PodPending,
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test20", "0/1", "Terminating", "0", "<unknown>"}}},
		},
		// 两个容器分别为 Error、Running，其中一个就绪；覆盖错误状态与运行状态并存
		// 参考：https://github.com/kubernetes/kubernetes/issues/107713
		{
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test21"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: api.PodFailed,
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready: false,
							State: api.ContainerState{Terminated: &api.ContainerStateTerminated{
								FinishedAt: metav1.NewTime(time.Now()),
								ExitCode:   1,
								Reason:     "podCrash"},
							},
							LastTerminationState: api.ContainerState{},
						},
						{
							Ready: true,
							State: api.ContainerState{Running: &api.ContainerStateRunning{
								StartedAt: metav1.NewTime(time.Now())},
							},
							LastTerminationState: api.ContainerState{},
						},
					},
				},
			},
			[]metav1.TableRow{
				{
					Cells:      []interface{}{"test21", "1/2", "podCrash", "0", "<unknown>"},
					Conditions: podFailedConditions,
				},
			},
		},
		// 交换上例的容器状态顺序，验证结果不依赖状态列表顺序
		// 参考：https://github.com/kubernetes/kubernetes/issues/107713
		{
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test22"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: api.PodFailed,
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready: true,
							State: api.ContainerState{Running: &api.ContainerStateRunning{
								StartedAt: metav1.NewTime(time.Now())},
							},
							LastTerminationState: api.ContainerState{},
						},
						{
							Ready: false,
							State: api.ContainerState{Terminated: &api.ContainerStateTerminated{
								FinishedAt: metav1.NewTime(time.Now()),
								ExitCode:   1,
								Reason:     "podCrash"},
							},
							LastTerminationState: api.ContainerState{},
						},
					},
				},
			},
			[]metav1.TableRow{
				{
					Cells:      []interface{}{"test22", "1/2", "podCrash", "0", "<unknown>"},
					Conditions: podFailedConditions,
				},
			},
		},
		{
			// 三个容器分别为 Running、Error、Completed，且均就绪
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test23"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 3)},
				Status: api.PodStatus{
					Phase: "Failed",
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready: true,
							State: api.ContainerState{Running: &api.ContainerStateRunning{
								StartedAt: metav1.NewTime(time.Now())},
							},
							LastTerminationState: api.ContainerState{},
						},
						{
							Ready: true,
							State: api.ContainerState{Terminated: &api.ContainerStateTerminated{
								FinishedAt: metav1.NewTime(time.Now()),
								ExitCode:   1,
								Reason:     "podCrash"},
							},
							LastTerminationState: api.ContainerState{},
						},
						{
							Ready: true,
							State: api.ContainerState{Terminated: &api.ContainerStateTerminated{
								FinishedAt: metav1.NewTime(time.Now()),
								ExitCode:   0},
							},
							LastTerminationState: api.ContainerState{},
						},
					},
				},
			},
			[]metav1.TableRow{
				{
					Cells:      []interface{}{"test23", "1/3", "podCrash", "0", "<unknown>"},
					Conditions: podFailedConditions,
				},
			},
		},
		{
			// 三个容器分别为 Running、Error、Completed，但均未就绪
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test24"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 3)},
				Status: api.PodStatus{
					Phase: "Failed",
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready: false,
							State: api.ContainerState{Running: &api.ContainerStateRunning{
								StartedAt: metav1.NewTime(time.Now())},
							},
							LastTerminationState: api.ContainerState{},
						},
						{
							Ready: false,
							State: api.ContainerState{Terminated: &api.ContainerStateTerminated{
								FinishedAt: metav1.NewTime(time.Now()),
								ExitCode:   1,
								Reason:     "podCrash"},
							},
							LastTerminationState: api.ContainerState{},
						},
						{
							Ready: false,
							State: api.ContainerState{Terminated: &api.ContainerStateTerminated{
								FinishedAt: metav1.NewTime(time.Now()),
								ExitCode:   0},
							},
							LastTerminationState: api.ContainerState{},
						},
					},
				},
			},
			[]metav1.TableRow{
				{
					Cells:      []interface{}{"test24", "0/3", "podCrash", "0", "<unknown>"},
					Conditions: podFailedConditions,
				},
			},
		},
	}

	for i, test := range tests {
		rows, err := printPod(&test.pod, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expect, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expect, rows))
		}
	}
}

func TestPrintPodWithRestartableInitContainer(t *testing.T) {
	tests := []struct {
		pod    api.Pod
		expect []metav1.TableRow
	}{
		{
			// 两个可重启初始化容器，首个 Running 但 Started 为 false
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test1"},
				Spec: api.PodSpec{
					InitContainers: []api.Container{
						{Name: "restartable-init-1", RestartPolicy: &containerRestartPolicyAlways},
						{Name: "restartable-init-2", RestartPolicy: &containerRestartPolicyAlways},
					}, Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: "Pending",
					InitContainerStatuses: []api.ContainerStatus{
						{
							Name:                 "restartable-init-1",
							Ready:                false,
							RestartCount:         3,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							Started:              ptr.To(false),
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-10 * time.Second))}},
						},
						{
							Name:    "restartable-init-2",
							Ready:   false,
							State:   api.ContainerState{Waiting: &api.ContainerStateWaiting{}},
							Started: ptr.To(false),
						},
					},
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready:        false,
							RestartCount: 0,
							State:        api.ContainerState{Waiting: &api.ContainerStateWaiting{}},
						},
					},
					Conditions: []api.PodCondition{
						{Type: api.PodInitialized, Status: api.ConditionFalse},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test1", "0/3", "Init:0/2", "3 (10s ago)", "<unknown>"}}},
		},
		{
			// 两个可重启初始化容器，首个已启动，第二个 Running 但未启动
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test1"},
				Spec: api.PodSpec{
					InitContainers: []api.Container{
						{Name: "restartable-init-1", RestartPolicy: &containerRestartPolicyAlways},
						{Name: "restartable-init-2", RestartPolicy: &containerRestartPolicyAlways},
					}, Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: "Pending",
					InitContainerStatuses: []api.ContainerStatus{
						{
							Name:                 "restartable-init-1",
							Ready:                false,
							RestartCount:         3,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							Started:              ptr.To(true),
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-10 * time.Second))}},
						},
						{
							Name:    "restartable-init-2",
							Ready:   false,
							State:   api.ContainerState{Running: &api.ContainerStateRunning{}},
							Started: ptr.To(false),
						},
					},
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready:        false,
							RestartCount: 0,
							State:        api.ContainerState{Waiting: &api.ContainerStateWaiting{}},
						},
					},
					Conditions: []api.PodCondition{
						{Type: api.PodInitialized, Status: api.ConditionFalse},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test1", "0/3", "Init:1/2", "3 (10s ago)", "<unknown>"}}},
		},
		{
			// 两个可重启初始化容器均已启动，一个业务容器正在运行
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test2"},
				Spec: api.PodSpec{
					InitContainers: []api.Container{
						{Name: "restartable-init-1", RestartPolicy: &containerRestartPolicyAlways},
						{Name: "restartable-init-2", RestartPolicy: &containerRestartPolicyAlways},
					}, Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: "Running",
					InitContainerStatuses: []api.ContainerStatus{
						{
							Name:                 "restartable-init-1",
							Ready:                false,
							RestartCount:         3,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							Started:              ptr.To(true),
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-10 * time.Second))}},
						},
						{
							Name:    "restartable-init-2",
							Ready:   false,
							State:   api.ContainerState{Running: &api.ContainerStateRunning{}},
							Started: ptr.To(true),
						},
					},
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready:                true,
							RestartCount:         4,
							State:                api.ContainerState{Running: &api.ContainerStateRunning{}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-20 * time.Second))}},
						},
					},
					Conditions: []api.PodCondition{
						{Type: api.PodInitialized, Status: api.ConditionTrue},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test2", "1/3", "Running", "7 (10s ago)", "<unknown>"}}},
		},
		{
			// 两个可重启初始化容器以非零码完成，一个业务容器完成
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test3"},
				Spec: api.PodSpec{
					InitContainers: []api.Container{
						{Name: "restartable-init-1", RestartPolicy: &containerRestartPolicyAlways},
						{Name: "restartable-init-2", RestartPolicy: &containerRestartPolicyAlways},
					}, Containers: make([]api.Container, 1)},
				Status: api.PodStatus{
					Phase: "Succeeded",
					InitContainerStatuses: []api.ContainerStatus{
						{
							Name:                 "restartable-init-1",
							Ready:                false,
							RestartCount:         3,
							State:                api.ContainerState{Terminated: &api.ContainerStateTerminated{Reason: "Error", ExitCode: 137}},
							Started:              ptr.To(false),
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-10 * time.Second))}},
						},
						{
							Name:    "restartable-init-2",
							Ready:   false,
							State:   api.ContainerState{Terminated: &api.ContainerStateTerminated{Reason: "Error", ExitCode: 137}},
							Started: ptr.To(false),
						},
					},
					ContainerStatuses: []api.ContainerStatus{
						{
							Ready:                false,
							RestartCount:         4,
							State:                api.ContainerState{Terminated: &api.ContainerStateTerminated{Reason: "Completed", ExitCode: 0}},
							LastTerminationState: api.ContainerState{Terminated: &api.ContainerStateTerminated{FinishedAt: metav1.NewTime(time.Now().Add(-20 * time.Second))}},
						},
					},
					Conditions: []api.PodCondition{
						{Type: api.PodInitialized, Status: api.ConditionTrue},
					},
				},
			},
			[]metav1.TableRow{
				{
					Cells: []interface{}{"test3", "0/3", "Completed", "7 (10s ago)", "<unknown>"},
					Conditions: []metav1.TableRowCondition{
						{Type: metav1.RowCompleted, Status: metav1.ConditionTrue, Reason: "Succeeded", Message: "The pod has completed successfully."},
					},
				},
			},
		},
		{
			// 状态列表包含规格中不存在的容器，验证不会因此崩溃
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test4"},
				Spec: api.PodSpec{
					InitContainers: []api.Container{
						{Name: "init1", Image: "initimage"},
						{Name: "sidecar1", Image: "sidecarimage", RestartPolicy: ptr.To(api.ContainerRestartPolicyAlways)},
					},
					Containers: []api.Container{{Name: "container1", Image: "containerimage"}},
				},
				Status: api.PodStatus{
					Phase: "Running",
					InitContainerStatuses: []api.ContainerStatus{
						{Name: "initinvalid"},
						{Name: "init1"},
						{Name: "sidecar1"},
					},
					ContainerStatuses: []api.ContainerStatus{
						{Name: "containerinvalid"},
						{Name: "container1"},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test4", "0/2", "Init:0/2", "0", "<unknown>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printPod(&test.pod, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expect, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expect, rows))
		}
	}
}

func TestPrintPodwide(t *testing.T) {
	condition1 := "condition1"
	condition2 := "condition2"
	condition3 := "condition3"
	tests := []struct {
		pod    api.Pod
		expect []metav1.TableRow
	}{
		{
			// 验证 NodeName 与 PodIP 非空时按实际值展示
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test1"},
				Spec: api.PodSpec{
					Containers: make([]api.Container, 2),
					NodeName:   "test1",
					ReadinessGates: []api.PodReadinessGate{
						{
							ConditionType: api.PodConditionType(condition1),
						},
						{
							ConditionType: api.PodConditionType(condition2),
						},
						{
							ConditionType: api.PodConditionType(condition3),
						},
					},
				},
				Status: api.PodStatus{
					Conditions: []api.PodCondition{
						{
							Type:   api.PodConditionType(condition1),
							Status: api.ConditionFalse,
						},
						{
							Type:   api.PodConditionType(condition2),
							Status: api.ConditionTrue,
						},
					},
					Phase:  "podPhase",
					PodIPs: []api.PodIP{{IP: "1.1.1.1"}},
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{RestartCount: 3},
					},
					NominatedNodeName: "node1",
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test1", "1/2", "podPhase", "6", "<unknown>", "1.1.1.1", "test1", "node1", "1/3"}}},
		},
		{
			// 验证 NodeName 与 PodIP 非空时按实际值展示
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test1"},
				Spec: api.PodSpec{
					Containers: make([]api.Container, 2),
					NodeName:   "test1",
					ReadinessGates: []api.PodReadinessGate{
						{
							ConditionType: api.PodConditionType(condition1),
						},
						{
							ConditionType: api.PodConditionType(condition2),
						},
						{
							ConditionType: api.PodConditionType(condition3),
						},
					},
				},
				Status: api.PodStatus{
					Conditions: []api.PodCondition{
						{
							Type:   api.PodConditionType(condition1),
							Status: api.ConditionFalse,
						},
						{
							Type:   api.PodConditionType(condition2),
							Status: api.ConditionTrue,
						},
					},
					Phase:  "podPhase",
					PodIPs: []api.PodIP{{IP: "1.1.1.1"}, {IP: "2001:db8::"}},
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{RestartCount: 3},
					},
					NominatedNodeName: "node1",
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test1", "1/2", "podPhase", "6", "<unknown>", "1.1.1.1", "test1", "node1", "1/3"}}},
		},
		{
			// 验证 NodeName 与 PodIP 为空时展示 <none>
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test2"},
				Spec: api.PodSpec{
					Containers: make([]api.Container, 2),
					NodeName:   "",
				},
				Status: api.PodStatus{
					Phase: "podPhase",
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{State: api.ContainerState{Waiting: &api.ContainerStateWaiting{Reason: "ContainerWaitingReason"}}, RestartCount: 3},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test2", "1/2", "ContainerWaitingReason", "6", "<unknown>", "<none>", "<none>", "<none>", "<none>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printPod(&test.pod, printers.GenerateOptions{Wide: true})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expect, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expect, rows))
		}
	}
}

func TestPrintPodConditions(t *testing.T) {
	runningPod := &api.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test1", Labels: map[string]string{"a": "1", "b": "2"}},
		Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
		Status: api.PodStatus{
			Phase: "Running",
			ContainerStatuses: []api.ContainerStatus{
				{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
				{RestartCount: 3},
			},
		},
	}
	succeededPod := &api.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test1", Labels: map[string]string{"a": "1", "b": "2"}},
		Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
		Status: api.PodStatus{
			Phase: "Succeeded",
			ContainerStatuses: []api.ContainerStatus{
				{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
				{RestartCount: 3},
			},
		},
	}
	failedPod := &api.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test2", Labels: map[string]string{"b": "2"}},
		Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
		Status: api.PodStatus{
			Phase: "Failed",
			ContainerStatuses: []api.ContainerStatus{
				{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
				{RestartCount: 3},
			},
		},
	}
	tests := []struct {
		pod    *api.Pod
		expect []metav1.TableRow
	}{
		// 非终态 Pod 不应附带 TableRowCondition
		{
			pod: runningPod,
			// 列： Name, Ready, Reason, Restarts, Age
			expect: []metav1.TableRow{{Cells: []interface{}{"test1", "1/2", "Running", "6", "<unknown>"}}},
		},
		// 成功 Pod 应附带 podSuccessConditions
		{
			pod: succeededPod,
			expect: []metav1.TableRow{
				{
					// 列： Name, Ready, Reason, Restarts, Age
					Cells:      []interface{}{"test1", "1/2", "Succeeded", "6", "<unknown>"},
					Conditions: podSuccessConditions,
				},
			},
		},
		// 失败 Pod 应附带 podFailedConditions
		{
			pod: failedPod,
			expect: []metav1.TableRow{
				{
					// 列： Name, Ready, Reason, Restarts, Age
					Cells:      []interface{}{"test2", "1/2", "Failed", "6", "<unknown>"},
					Conditions: podFailedConditions,
				},
			},
		},
	}

	for i, test := range tests {
		rows, err := printPod(test.pod, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expect, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expect, rows))
		}
	}
}

func TestPrintPodList(t *testing.T) {
	tests := []struct {
		pods   api.PodList
		expect []metav1.TableRow
	}{
		// 验证 PodList 中单个 Pod 的名称、容器数、重启次数和就绪状态
		{
			api.PodList{
				Items: []api.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "test1"},
						Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
						Status: api.PodStatus{
							Phase: "podPhase",
							ContainerStatuses: []api.ContainerStatus{
								{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
								{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "test2"},
						Spec:       api.PodSpec{Containers: make([]api.Container, 1)},
						Status: api.PodStatus{
							Phase: "podPhase",
							ContainerStatuses: []api.ContainerStatus{
								{Ready: true, RestartCount: 1, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
							},
						},
					},
				},
			},
			[]metav1.TableRow{{Cells: []interface{}{"test1", "2/2", "podPhase", "6", "<unknown>"}}, {Cells: []interface{}{"test2", "1/1", "podPhase", "1", "<unknown>"}}},
		},
	}

	for _, test := range tests {
		rows, err := printPodList(&test.pods, printers.GenerateOptions{})

		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expect, rows) {
			t.Errorf("mismatch: %s", cmp.Diff(test.expect, rows))
		}
	}
}

func TestPrintNonTerminatedPod(t *testing.T) {
	tests := []struct {
		pod    api.Pod
		expect []metav1.TableRow
	}{
		{
			// Running 阶段应原样展示
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test1"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: api.PodRunning,
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{RestartCount: 3},
					},
				},
			},
			// 列： Name, Ready, Reason, Restarts, Age
			[]metav1.TableRow{{Cells: []interface{}{"test1", "1/2", "Running", "6", "<unknown>"}}},
		},
		{
			// Pending 阶段应原样展示
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test2"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: api.PodPending,
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{RestartCount: 3},
					},
				},
			},
			// 列： Name, Ready, Reason, Restarts, Age
			[]metav1.TableRow{{Cells: []interface{}{"test2", "1/2", "Pending", "6", "<unknown>"}}},
		},
		{
			// Unknown 阶段应原样展示
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test3"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: api.PodUnknown,
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{RestartCount: 3},
					},
				},
			},
			// 列： Name, Ready, Reason, Restarts, Age
			[]metav1.TableRow{{Cells: []interface{}{"test3", "1/2", "Unknown", "6", "<unknown>"}}},
		},
		{
			// Succeeded 阶段应原样展示
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test4"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: api.PodSucceeded,
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{RestartCount: 3},
					},
				},
			},
			// 列： Name, Ready, Reason, Restarts, Age
			[]metav1.TableRow{
				{
					Cells:      []interface{}{"test4", "1/2", "Succeeded", "6", "<unknown>"},
					Conditions: podSuccessConditions,
				},
			},
		},
		{
			// Failed 阶段不应进入非终态 Pod 的输出
			api.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test5"},
				Spec:       api.PodSpec{Containers: make([]api.Container, 2)},
				Status: api.PodStatus{
					Phase: api.PodFailed,
					ContainerStatuses: []api.ContainerStatus{
						{Ready: true, RestartCount: 3, State: api.ContainerState{Running: &api.ContainerStateRunning{}}},
						{Ready: true, RestartCount: 3},
					},
				},
			},
			// 列： Name, Ready, Reason, Restarts, Age
			[]metav1.TableRow{
				{
					Cells:      []interface{}{"test5", "1/2", "Failed", "6", "<unknown>"},
					Conditions: podFailedConditions,
				},
			},
		},
	}

	for i, test := range tests {
		rows, err := printPod(&test.pod, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expect, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expect, rows))
		}
	}
}

func TestPrintPodTemplate(t *testing.T) {
	tests := []struct {
		podTemplate api.PodTemplate
		options     printers.GenerateOptions
		expected    []metav1.TableRow
	}{
		// 基础 PodTemplate；无容器
		{
			podTemplate: api.PodTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-template-1"},
				Template: api.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-template-1"},
					Spec: api.PodSpec{
						Containers: []api.Container{},
					},
				},
			},

			options: printers.GenerateOptions{},
			// 列： Name, Containers, Images, Pod Labels
			expected: []metav1.TableRow{{Cells: []interface{}{"pod-template-1", "", "", "<none>"}}},
		},
		// 基础 PodTemplate；两个容器
		{
			podTemplate: api.PodTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-template-2"},
				Template: api.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-template-2"},
					Spec: api.PodSpec{
						Containers: []api.Container{
							{
								Name:  "fake-container1",
								Image: "fake-image1",
							},
							{
								Name:  "fake-container2",
								Image: "fake-image2",
							},
						},
					},
				},
			},

			options: printers.GenerateOptions{},
			// 列： Name, Containers, Images, Pod Labels
			expected: []metav1.TableRow{{Cells: []interface{}{"pod-template-2", "fake-container1,fake-container2", "fake-image1,fake-image2", "<none>"}}},
		},
		// 基础 PodTemplate；验证 Pod 标签展示
		{
			podTemplate: api.PodTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-template-3"},
				Template: api.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "pod-template-3",
						Labels: map[string]string{"foo": "bar"},
					},
					Spec: api.PodSpec{
						Containers: []api.Container{},
					},
				},
			},

			options: printers.GenerateOptions{},
			// 列： Name, Containers, Images, Pod Labels
			expected: []metav1.TableRow{{Cells: []interface{}{"pod-template-3", "", "", "foo=bar"}}},
		},
	}

	for i, test := range tests {
		rows, err := printPodTemplate(&test.podTemplate, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintPodTemplateList(t *testing.T) {

	templateList := api.PodTemplateList{
		Items: []api.PodTemplate{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-template-1"},
				Template: api.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "pod-template-2",
						Labels: map[string]string{"foo": "bar"},
					},
					Spec: api.PodSpec{
						Containers: []api.Container{},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-template-2"},
				Template: api.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "pod-template-2",
						Labels: map[string]string{"a": "b"},
					},
					Spec: api.PodSpec{
						Containers: []api.Container{},
					},
				},
			},
		},
	}

	// 列： Name, Containers, Images, Pod Labels
	expectedRows := []metav1.TableRow{
		{Cells: []interface{}{"pod-template-1", "", "", "foo=bar"}},
		{Cells: []interface{}{"pod-template-2", "", "", "a=b"}},
	}

	rows, err := printPodTemplateList(&templateList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error printing pod template list: %#v", err)
	}
	for i := range rows {
		rows[i].Object.Object = nil
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expectedRows, rows))
	}
}

type stringTestList []struct {
	name, got, exp string
}

func TestTranslateTimestampSince(t *testing.T) {
	tl := stringTestList{
		{"a while from now", translateTimestampSince(metav1.Time{Time: time.Now().Add(2.1e9)}), "<invalid>"},
		{"almost now", translateTimestampSince(metav1.Time{Time: time.Now().Add(1.9e9)}), "0s"},
		{"now", translateTimestampSince(metav1.Time{Time: time.Now()}), "0s"},
		{"unknown", translateTimestampSince(metav1.Time{}), "<unknown>"},
		{"30 seconds ago", translateTimestampSince(metav1.Time{Time: time.Now().Add(-3e10)}), "30s"},
		{"5 minutes ago", translateTimestampSince(metav1.Time{Time: time.Now().Add(-3e11)}), "5m"},
		{"an hour ago", translateTimestampSince(metav1.Time{Time: time.Now().Add(-6e12)}), "100m"},
		{"2 days ago", translateTimestampSince(metav1.Time{Time: time.Now().UTC().AddDate(0, 0, -2)}), "2d"},
		{"months ago", translateTimestampSince(metav1.Time{Time: time.Now().UTC().AddDate(0, 0, -90)}), "90d"},
		{"10 years ago", translateTimestampSince(metav1.Time{Time: time.Now().UTC().Add(time.Duration(-10 * 365 * 24 * time.Hour))}), "10y"},
	}
	for _, test := range tl {
		if test.got != test.exp {
			t.Errorf("On %v, expected '%v', but got '%v'",
				test.name, test.exp, test.got)
		}
	}
}

func TestTranslateTimestampUntil(t *testing.T) {
	// 被测函数内部调用 time.Now()，因此比较时预留 0.1 秒余量，
	// 避免函数调用开销让耗时字符串比预期更短。
	const buf = 1e8
	tl := stringTestList{
		{"a while ago", translateTimestampUntil(metav1.Time{Time: time.Now().Add(-2.1e9)}), "<invalid>"},
		{"almost now", translateTimestampUntil(metav1.Time{Time: time.Now().Add(-1.9e9)}), "0s"},
		{"now", translateTimestampUntil(metav1.Time{Time: time.Now()}), "0s"},
		{"unknown", translateTimestampUntil(metav1.Time{}), "<unknown>"},
		{"in 30 seconds", translateTimestampUntil(metav1.Time{Time: time.Now().Add(3e10 + buf)}), "30s"},
		{"in 5 minutes", translateTimestampUntil(metav1.Time{Time: time.Now().Add(3e11 + buf)}), "5m"},
		{"in an hour", translateTimestampUntil(metav1.Time{Time: time.Now().Add(6e12 + buf)}), "100m"},
		{"in 2 days", translateTimestampUntil(metav1.Time{Time: time.Now().UTC().AddDate(0, 0, 2).Add(buf)}), "2d"},
		{"in months", translateTimestampUntil(metav1.Time{Time: time.Now().UTC().AddDate(0, 0, 90).Add(buf)}), "90d"},
		{"in 10 years", translateTimestampUntil(metav1.Time{Time: time.Now().UTC().Add(time.Duration(10 * 365 * 24 * time.Hour)).Add(buf)}), "10y"},
	}
	for _, test := range tl {
		if test.got != test.exp {
			t.Errorf("On %v, expected '%v', but got '%v'",
				test.name, test.exp, test.got)
		}
	}
}

func TestPrintDeployment(t *testing.T) {

	testDeployment := apps.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test1",
			CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
		},
		Spec: apps.DeploymentSpec{
			Replicas: 5,
			Template: api.PodTemplateSpec{
				Spec: api.PodSpec{
					Containers: []api.Container{
						{
							Name:  "fake-container1",
							Image: "fake-image1",
						},
						{
							Name:  "fake-container2",
							Image: "fake-image2",
						},
					},
				},
			},
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
		},
		Status: apps.DeploymentStatus{
			Replicas:            10,
			UpdatedReplicas:     2,
			AvailableReplicas:   1,
			UnavailableReplicas: 4,
		},
	}

	tests := []struct {
		deployment apps.Deployment
		options    printers.GenerateOptions
		expected   []metav1.TableRow
	}{
		// 验证 Deployment 在普通模式下仅展示基础列
		{
			deployment: testDeployment,
			options:    printers.GenerateOptions{},
			// 列： Name, ReadyReplicas, UpdatedReplicas, AvailableReplicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", "0/5", int64(2), int64(1), "0s"}}},
		},
		// 验证 Wide 模式增加容器、镜像和选择器列
		{
			deployment: testDeployment,
			options:    printers.GenerateOptions{Wide: true},
			// 列： Name, ReadyReplicas, UpdatedReplicas, AvailableReplicas, Age, Containers, Images, Selectors
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", "0/5", int64(2), int64(1), "0s", "fake-container1,fake-container2", "fake-image1,fake-image2", "foo=bar"}}},
		},
	}

	for i, test := range tests {
		rows, err := printDeployment(&test.deployment, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintDaemonSet(t *testing.T) {

	testDaemonSet := apps.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test1",
			CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
		},
		Spec: apps.DaemonSetSpec{
			Template: api.PodTemplateSpec{
				Spec: api.PodSpec{
					Containers: []api.Container{
						{
							Name:  "fake-container1",
							Image: "fake-image1",
						},
						{
							Name:  "fake-container2",
							Image: "fake-image2",
						},
					},
				},
			},
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
		},
		Status: apps.DaemonSetStatus{
			CurrentNumberScheduled: 2,
			DesiredNumberScheduled: 3,
			NumberReady:            1,
			UpdatedNumberScheduled: 2,
			NumberAvailable:        0,
		},
	}

	tests := []struct {
		daemonSet apps.DaemonSet
		options   printers.GenerateOptions
		expected  []metav1.TableRow
	}{
		// 验证 DaemonSet 在普通模式下的基础列
		{
			daemonSet: testDaemonSet,
			options:   printers.GenerateOptions{},
			// 列： Name, Num Desired, Num Current, Num Ready, Num Updated, Num Available, Selectors, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", int64(3), int64(2), int64(1), int64(2), int64(0), "<none>", "0s"}}},
		},
		// 测试使用 "Wide" 生成选项生成守护进程集。
		{
			daemonSet: testDaemonSet,
			options:   printers.GenerateOptions{Wide: true},
			// 列： Name, Num Desired, Num Current, Num Ready, Num Updated, Num Available, Node Selectors, Age, Containers, Images, Labels
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", int64(3), int64(2), int64(1), int64(2), int64(0), "<none>", "0s", "fake-container1,fake-container2", "fake-image1,fake-image2", "foo=bar"}}},
		},
	}

	for i, test := range tests {
		rows, err := printDaemonSet(&test.daemonSet, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintDaemonSetList(t *testing.T) {

	daemonSetList := apps.DaemonSetList{
		Items: []apps.DaemonSet{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "daemonset1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: apps.DaemonSetSpec{
					Template: api.PodTemplateSpec{
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:  "fake-container1",
									Image: "fake-image1",
								},
								{
									Name:  "fake-container2",
									Image: "fake-image2",
								},
							},
						},
					},
				},
				Status: apps.DaemonSetStatus{
					CurrentNumberScheduled: 2,
					DesiredNumberScheduled: 3,
					NumberReady:            1,
					UpdatedNumberScheduled: 2,
					NumberAvailable:        0,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "daemonset2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: apps.DaemonSetSpec{
					Template: api.PodTemplateSpec{},
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
				},
				Status: apps.DaemonSetStatus{
					CurrentNumberScheduled: 4,
					DesiredNumberScheduled: 2,
					NumberReady:            9,
					UpdatedNumberScheduled: 3,
					NumberAvailable:        3,
				},
			},
		},
	}

	// 列： Name, Num Desired, Num Current, Num Ready, Num Updated, Num Available, Selectors, Age
	expectedRows := []metav1.TableRow{
		{Cells: []interface{}{"daemonset1", int64(3), int64(2), int64(1), int64(2), int64(0), "<none>", "0s"}},
		{Cells: []interface{}{"daemonset2", int64(2), int64(4), int64(9), int64(3), int64(3), "<none>", "0s"}},
	}

	rows, err := printDaemonSetList(&daemonSetList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error printing daemon set list: %#v", err)
	}
	for i := range rows {
		rows[i].Object.Object = nil
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expectedRows, rows))
	}
}

func TestPrintJob(t *testing.T) {
	now := time.Now()
	completions := int32(2)
	tests := []struct {
		job      batch.Job
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		{
			// 为不指定生成选项的 Job 生成表格行。
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.JobSpec{
					Completions: &completions,
					Template: api.PodTemplateSpec{
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:  "fake-job-container1",
									Image: "fake-job-image1",
								},
								{
									Name:  "fake-job-container2",
									Image: "fake-job-image2",
								},
							},
						},
					},
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"job-label": "job-lable-value"}},
				},
				Status: batch.JobStatus{
					Succeeded: 1,
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Status, Completions, Duration, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"job1", "Running", "1/2", "", "0s"}}},
		},
		// 为指定生成选项 "Wide" 的 Job 生成表格行。
		{
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.JobSpec{
					Completions: &completions,
					Template: api.PodTemplateSpec{
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:  "fake-job-container1",
									Image: "fake-job-image1",
								},
								{
									Name:  "fake-job-container2",
									Image: "fake-job-image2",
								},
							},
						},
					},
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"job-label": "job-label-value"}},
				},
				Status: batch.JobStatus{
					Succeeded: 1,
				},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Name, Status, Completions, Duration, Age, Containers, Images, Selectors
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"job1", "Running", "1/2", "", "0s", "fake-job-container1,fake-job-container2", "fake-job-image1,fake-job-image2", "job-label=job-label-value"},
				},
			},
		},
		// 已存在十年的 Job。
		{
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job2",
					CreationTimestamp: metav1.Time{Time: time.Now().AddDate(-10, 0, 0)},
				},
				Spec: batch.JobSpec{
					Completions: nil,
				},
				Status: batch.JobStatus{
					Succeeded: 0,
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Status, Completions, Duration, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"job2", "Running", "0/1", "", "10y"}}},
		},
		// 带持续时间的 Job。
		{
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job3",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(time.Duration(-10 * 365 * 24 * time.Hour))},
				},
				Spec: batch.JobSpec{
					Completions: nil,
				},
				Status: batch.JobStatus{
					Succeeded:      0,
					StartTime:      &metav1.Time{Time: now.Add(time.Minute)},
					CompletionTime: &metav1.Time{Time: now.Add(31 * time.Minute)},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Status, Completions, Duration, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"job3", "Running", "0/1", "30m", "10y"}}},
		},
		{
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job4",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(time.Duration(-10 * 365 * 24 * time.Hour))},
				},
				Spec: batch.JobSpec{
					Completions: nil,
				},
				Status: batch.JobStatus{
					Succeeded: 0,
					StartTime: &metav1.Time{Time: time.Now().Add(-20 * time.Minute)},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Status, Completions, Duration, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"job4", "Running", "0/1", "20m", "10y"}}},
		},
		{
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job5",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.JobSpec{
					Completions: nil,
				},
				Status: batch.JobStatus{
					Succeeded: 0,
					Conditions: []batch.JobCondition{
						{
							Type:   batch.JobComplete,
							Status: api.ConditionTrue,
						},
					},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Status, Completions, Duration, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"job5", "Complete", "0/1", "", "0s"}}},
		},
		{
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job6",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.JobSpec{
					Completions: nil,
				},
				Status: batch.JobStatus{
					Succeeded: 0,
					Conditions: []batch.JobCondition{
						{
							Type:   batch.JobFailed,
							Status: api.ConditionTrue,
						},
					},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Status, Completions, Duration, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"job6", "Failed", "0/1", "", "0s"}}},
		},
		{
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job7",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.JobSpec{
					Completions: nil,
				},
				Status: batch.JobStatus{
					Succeeded: 0,
					Conditions: []batch.JobCondition{
						{
							Type:   batch.JobSuspended,
							Status: api.ConditionTrue,
						},
					},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Status, Completions, Duration, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"job7", "Suspended", "0/1", "", "0s"}}},
		},
		{
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job8",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.JobSpec{
					Completions: nil,
				},
				Status: batch.JobStatus{
					Succeeded: 0,
					Conditions: []batch.JobCondition{
						{
							Type:   batch.JobFailureTarget,
							Status: api.ConditionTrue,
						},
					},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Status, Completions, Duration, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"job8", "FailureTarget", "0/1", "", "0s"}}},
		},
		{
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job9",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
					DeletionTimestamp: &metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.JobSpec{
					Completions: nil,
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Status, Completions, Duration, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"job9", "Terminating", "0/1", "", "0s"}}},
		},
		{
			job: batch.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job10",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.JobSpec{
					Completions: nil,
				},
				Status: batch.JobStatus{
					Succeeded: 0,
					Conditions: []batch.JobCondition{
						{
							Type:   batch.JobSuccessCriteriaMet,
							Status: api.ConditionTrue,
						},
					},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Status, Completions, Duration, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"job10", "SuccessCriteriaMet", "0/1", "", "0s"}}},
		},
	}

	for i, test := range tests {
		rows, err := printJob(&test.job, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintJobList(t *testing.T) {
	completions := int32(2)
	jobList := batch.JobList{
		Items: []batch.Job{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.JobSpec{
					Completions: &completions,
					Template: api.PodTemplateSpec{
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:  "fake-job-container1",
									Image: "fake-job-image1",
								},
								{
									Name:  "fake-job-container2",
									Image: "fake-job-image2",
								},
							},
						},
					},
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"job-label": "job-lable-value"}},
				},
				Status: batch.JobStatus{
					Succeeded: 1,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "job2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.JobSpec{
					Completions: &completions,
					Template: api.PodTemplateSpec{
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:  "fake-job-container1",
									Image: "fake-job-image1",
								},
								{
									Name:  "fake-job-container2",
									Image: "fake-job-image2",
								},
							},
						},
					},
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"job-label": "job-lable-value"}},
				},
				Status: batch.JobStatus{
					Succeeded: 2,
					StartTime: &metav1.Time{Time: time.Now().Add(-20 * time.Minute)},
				},
			},
		},
	}

	// 列： Name, Status, Completions, Duration, Age
	expectedRows := []metav1.TableRow{
		{Cells: []interface{}{"job1", "Running", "1/2", "", "0s"}},
		{Cells: []interface{}{"job2", "Running", "2/2", "20m", "0s"}},
	}

	rows, err := printJobList(&jobList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error printing job list: %#v", err)
	}
	for i := range rows {
		rows[i].Object.Object = nil
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expectedRows, rows))
	}
}

func TestPrintHPA(t *testing.T) {
	minReplicasVal := int32(2)
	targetUtilizationVal := int32(80)
	currentUtilizationVal := int32(50)
	metricLabelSelector, err := metav1.ParseToLabelSelector("label=value")
	if err != nil {
		t.Errorf("unable to parse label selector: %v", err)
	}
	tests := []struct {
		hpa      autoscaling.HorizontalPodAutoscaler
		expected []metav1.TableRow
	}{
		// 未设置 minReplicas
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MaxReplicas: 10,
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "<none>", "<unset>", int64(10), int64(4), "<unknown>"}}},
		},
		// External 来源类型，目标平均值（无当前值）
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ExternalMetricSourceType,
							External: &autoscaling.ExternalMetricSource{
								Metric: autoscaling.MetricIdentifier{
									Name:     "some-external-metric",
									Selector: metricLabelSelector,
								},
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "<unknown>/100m (avg)", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// External 来源类型，目标平均值
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ExternalMetricSourceType,
							External: &autoscaling.ExternalMetricSource{
								Metric: autoscaling.MetricIdentifier{
									Name:     "some-external-metric",
									Selector: metricLabelSelector,
								},
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
					CurrentMetrics: []autoscaling.MetricStatus{
						{
							Type: autoscaling.ExternalMetricSourceType,
							External: &autoscaling.ExternalMetricStatus{
								Metric: autoscaling.MetricIdentifier{
									Name:     "some-external-metric",
									Selector: metricLabelSelector,
								},
								Current: autoscaling.MetricValueStatus{
									AverageValue: resource.NewMilliQuantity(50, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "50m/100m (avg)", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// External 来源类型，目标值（无当前值）
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ExternalMetricSourceType,
							External: &autoscaling.ExternalMetricSource{
								Metric: autoscaling.MetricIdentifier{
									Name:     "some-service-metric",
									Selector: metricLabelSelector,
								},
								Target: autoscaling.MetricTarget{
									Type:  autoscaling.ValueMetricType,
									Value: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "<unknown>/100m", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// External 来源类型，目标值
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ExternalMetricSourceType,
							External: &autoscaling.ExternalMetricSource{
								Metric: autoscaling.MetricIdentifier{
									Name:     "some-external-metric",
									Selector: metricLabelSelector,
								},
								Target: autoscaling.MetricTarget{
									Type:  autoscaling.ValueMetricType,
									Value: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
					CurrentMetrics: []autoscaling.MetricStatus{
						{
							Type: autoscaling.ExternalMetricSourceType,
							External: &autoscaling.ExternalMetricStatus{
								Metric: autoscaling.MetricIdentifier{
									Name: "some-external-metric",
								},
								Current: autoscaling.MetricValueStatus{
									Value: resource.NewMilliQuantity(50, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "50m/100m", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// Pods 来源类型（无当前值）
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.PodsMetricSourceType,
							Pods: &autoscaling.PodsMetricSource{
								Metric: autoscaling.MetricIdentifier{
									Name: "some-pods-metric",
								},
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "<unknown>/100m", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// Pods 来源类型
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.PodsMetricSourceType,
							Pods: &autoscaling.PodsMetricSource{
								Metric: autoscaling.MetricIdentifier{
									Name: "some-pods-metric",
								},
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
					CurrentMetrics: []autoscaling.MetricStatus{
						{
							Type: autoscaling.PodsMetricSourceType,
							Pods: &autoscaling.PodsMetricStatus{
								Metric: autoscaling.MetricIdentifier{
									Name: "some-pods-metric",
								},
								Current: autoscaling.MetricValueStatus{
									AverageValue: resource.NewMilliQuantity(50, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "50m/100m", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// Object 来源类型（无当前值）
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ObjectMetricSourceType,
							Object: &autoscaling.ObjectMetricSource{
								DescribedObject: autoscaling.CrossVersionObjectReference{
									Name: "some-service",
									Kind: "Service",
								},
								Metric: autoscaling.MetricIdentifier{
									Name: "some-service-metric",
								},
								Target: autoscaling.MetricTarget{
									Type:  autoscaling.ValueMetricType,
									Value: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "<unknown>/100m", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// Object 来源类型
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ObjectMetricSourceType,
							Object: &autoscaling.ObjectMetricSource{
								DescribedObject: autoscaling.CrossVersionObjectReference{
									Name: "some-service",
									Kind: "Service",
								},
								Metric: autoscaling.MetricIdentifier{
									Name: "some-service-metric",
								},
								Target: autoscaling.MetricTarget{
									Type:  autoscaling.ValueMetricType,
									Value: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
					CurrentMetrics: []autoscaling.MetricStatus{
						{
							Type: autoscaling.ObjectMetricSourceType,
							Object: &autoscaling.ObjectMetricStatus{
								DescribedObject: autoscaling.CrossVersionObjectReference{
									Name: "some-service",
									Kind: "Service",
								},
								Metric: autoscaling.MetricIdentifier{
									Name: "some-service-metric",
								},
								Current: autoscaling.MetricValueStatus{
									Value: resource.NewMilliQuantity(50, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "50m/100m", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// Resource 来源类型，目标值（无当前值）
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "cpu: <unknown>/100m", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// Resource 来源类型，目标值
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
					CurrentMetrics: []autoscaling.MetricStatus{
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricStatus{
								Name: api.ResourceCPU,
								Current: autoscaling.MetricValueStatus{
									AverageValue: resource.NewMilliQuantity(50, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "cpu: 50m/100m", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// Resource 来源类型，目标利用率（无当前值）
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:               autoscaling.UtilizationMetricType,
									AverageUtilization: &targetUtilizationVal,
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "cpu: <unknown>/80%", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// Resource 来源类型，目标利用率
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:               autoscaling.UtilizationMetricType,
									AverageUtilization: &targetUtilizationVal,
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
					CurrentMetrics: []autoscaling.MetricStatus{
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricStatus{
								Name: api.ResourceCPU,
								Current: autoscaling.MetricValueStatus{
									AverageUtilization: &currentUtilizationVal,
									AverageValue:       resource.NewMilliQuantity(40, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "cpu: 50%/80%", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// ContainerResource 来源类型，目标值（无当前值）
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ContainerResourceMetricSourceType,
							ContainerResource: &autoscaling.ContainerResourceMetricSource{
								Name:      api.ResourceCPU,
								Container: "application",
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "cpu: <unknown>/100m", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// ContainerResource 来源类型，目标值
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ContainerResourceMetricSourceType,
							ContainerResource: &autoscaling.ContainerResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
					CurrentMetrics: []autoscaling.MetricStatus{
						{
							Type: autoscaling.ContainerResourceMetricSourceType,
							ContainerResource: &autoscaling.ContainerResourceMetricStatus{
								Name:      api.ResourceCPU,
								Container: "application",
								Current: autoscaling.MetricValueStatus{
									AverageValue: resource.NewMilliQuantity(50, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "cpu: 50m/100m", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// ContainerResource 来源类型，目标利用率（无当前值）
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ContainerResourceMetricSourceType,
							ContainerResource: &autoscaling.ContainerResourceMetricSource{
								Name:      api.ResourceCPU,
								Container: "application",
								Target: autoscaling.MetricTarget{
									Type:               autoscaling.UtilizationMetricType,
									AverageUtilization: &targetUtilizationVal,
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "cpu: <unknown>/80%", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// ContainerResource 来源类型，目标利用率
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ContainerResourceMetricSourceType,
							ContainerResource: &autoscaling.ContainerResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:               autoscaling.UtilizationMetricType,
									AverageUtilization: &targetUtilizationVal,
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
					CurrentMetrics: []autoscaling.MetricStatus{
						{
							Type: autoscaling.ContainerResourceMetricSourceType,
							ContainerResource: &autoscaling.ContainerResourceMetricStatus{
								Name:      api.ResourceCPU,
								Container: "application",
								Current: autoscaling.MetricValueStatus{
									AverageUtilization: &currentUtilizationVal,
									AverageValue:       resource.NewMilliQuantity(40, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "cpu: 50%/80%", "2", int64(10), int64(4), "<unknown>"}}},
		},
		// 多个规格
		{
			hpa: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "some-hpa"},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Name: "some-rc",
						Kind: "ReplicationController",
					},
					MinReplicas: &minReplicasVal,
					MaxReplicas: 10,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.PodsMetricSourceType,
							Pods: &autoscaling.PodsMetricSource{
								Metric: autoscaling.MetricIdentifier{
									Name: "some-pods-metric",
								},
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
								},
							},
						},
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:               autoscaling.UtilizationMetricType,
									AverageUtilization: &targetUtilizationVal,
								},
							},
						},
						{
							Type: autoscaling.PodsMetricSourceType,
							Pods: &autoscaling.PodsMetricSource{
								Metric: autoscaling.MetricIdentifier{
									Name: "other-pods-metric",
								},
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(400, resource.DecimalSI),
								},
							},
						},
					},
				},
				Status: autoscaling.HorizontalPodAutoscalerStatus{
					CurrentReplicas: 4,
					DesiredReplicas: 5,
					CurrentMetrics: []autoscaling.MetricStatus{
						{
							Type: autoscaling.PodsMetricSourceType,
							Pods: &autoscaling.PodsMetricStatus{
								Metric: autoscaling.MetricIdentifier{
									Name: "some-pods-metric",
								},
								Current: autoscaling.MetricValueStatus{
									AverageValue: resource.NewMilliQuantity(50, resource.DecimalSI),
								},
							},
						},
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricStatus{
								Name: api.ResourceCPU,
								Current: autoscaling.MetricValueStatus{
									AverageUtilization: &currentUtilizationVal,
									AverageValue:       resource.NewMilliQuantity(40, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
			// 列： Name, Reference, Targets, MinPods, MaxPods, Replicas, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"some-hpa", "ReplicationController/some-rc", "50m/100m, cpu: 50%/80% + 1 more...", "2", int64(10), int64(4), "<unknown>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printHorizontalPodAutoscaler(&test.hpa, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintService(t *testing.T) {
	singleExternalIP := []string{"80.11.12.10"}
	mulExternalIP := []string{"80.11.12.10", "80.11.12.11"}
	tests := []struct {
		service  api.Service
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		{
			// 测试名称、集群 IP、带协议的端口
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "test1"},
				Spec: api.ServiceSpec{
					Type: api.ServiceTypeClusterIP,
					Ports: []api.ServicePort{
						{
							Protocol: "tcp",
							Port:     2233,
						},
					},
					ClusterIPs: []string{"10.9.8.7"},
					Selector:   map[string]string{"foo": "bar"}, // 不打印。
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", "ClusterIP", "10.9.8.7", "<none>", "2233/tcp", "<unknown>"}}},
		},
		{
			// 测试生成选项：Wide 包含选择器。
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "test1"},
				Spec: api.ServiceSpec{
					Type: api.ServiceTypeClusterIP,
					Ports: []api.ServicePort{
						{
							Protocol: "tcp",
							Port:     2233,
						},
					},
					ClusterIPs: []string{"10.9.8.7"},
					Selector:   map[string]string{"foo": "bar"},
				},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age, Selector
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", "ClusterIP", "10.9.8.7", "<none>", "2233/tcp", "<unknown>", "foo=bar"}}},
		},
		{
			// 测试 NodePort 服务
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "test2"},
				Spec: api.ServiceSpec{
					Type: api.ServiceTypeNodePort,
					Ports: []api.ServicePort{
						{
							Protocol: "tcp",
							Port:     8888,
							NodePort: 9999,
						},
					},
					ClusterIPs: []string{"10.9.8.7"},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test2", "NodePort", "10.9.8.7", "<none>", "8888:9999/tcp", "<unknown>"}}},
		},
		{
			// 测试 LoadBalancer 服务
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "test3"},
				Spec: api.ServiceSpec{
					Type: api.ServiceTypeLoadBalancer,
					Ports: []api.ServicePort{
						{
							Protocol: "tcp",
							Port:     8888,
						},
					},
					ClusterIPs: []string{"10.9.8.7"},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test3", "LoadBalancer", "10.9.8.7", "<pending>", "8888/tcp", "<unknown>"}}},
		},
		{
			// 测试具有单个 ExternalIP 且没有 LoadBalancerStatus 的 LoadBalancer 服务
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "test4"},
				Spec: api.ServiceSpec{
					Type: api.ServiceTypeLoadBalancer,
					Ports: []api.ServicePort{
						{
							Protocol: "tcp",
							Port:     8888,
						},
					},
					ClusterIPs:  []string{"10.9.8.7"},
					ExternalIPs: singleExternalIP,
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test4", "LoadBalancer", "10.9.8.7", "80.11.12.10", "8888/tcp", "<unknown>"}}},
		},
		{
			// 测试具有单个 ExternalIP 的 LoadBalancer 服务
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "test5"},
				Spec: api.ServiceSpec{
					Type: api.ServiceTypeLoadBalancer,
					Ports: []api.ServicePort{
						{
							Protocol: "tcp",
							Port:     8888,
						},
					},
					ClusterIPs:  []string{"10.9.8.7"},
					ExternalIPs: singleExternalIP,
				},
				Status: api.ServiceStatus{
					LoadBalancer: api.LoadBalancerStatus{
						Ingress: []api.LoadBalancerIngress{
							{
								IP:       "3.4.5.6",
								Hostname: "test.cluster.com",
							},
						},
					},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test5", "LoadBalancer", "10.9.8.7", "3.4.5.6,80.11.12.10", "8888/tcp", "<unknown>"}}},
		},
		{
			// 测试具有多个 ExternalIP 的 LoadBalancer 服务
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "test6"},
				Spec: api.ServiceSpec{
					Type: api.ServiceTypeLoadBalancer,
					Ports: []api.ServicePort{
						{
							Protocol: "tcp",
							Port:     8888,
						},
					},
					ClusterIPs:  []string{"10.9.8.7"},
					ExternalIPs: mulExternalIP,
				},
				Status: api.ServiceStatus{
					LoadBalancer: api.LoadBalancerStatus{
						Ingress: []api.LoadBalancerIngress{
							{
								IP:       "2.3.4.5",
								Hostname: "test.cluster.local",
							},
							{
								IP:       "3.4.5.6",
								Hostname: "test.cluster.com",
							},
						},
					},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test6", "LoadBalancer", "10.9.8.7", "2.3.4.5,3.4.5.6,80.11.12.10,80.11.12.11", "8888/tcp", "<unknown>"}}},
		},
		{
			// 测试 ExternalName 服务
			service: api.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "test7"},
				Spec: api.ServiceSpec{
					Type:         api.ServiceTypeExternalName,
					ExternalName: "my.database.example.com",
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test7", "ExternalName", "<none>", "my.database.example.com", "<none>", "<unknown>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printService(&test.service, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintServiceList(t *testing.T) {
	serviceList := api.ServiceList{
		Items: []api.Service{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "service1"},
				Spec: api.ServiceSpec{
					Type: api.ServiceTypeClusterIP,
					Ports: []api.ServicePort{
						{
							Protocol: "tcp",
							Port:     2233,
						},
					},
					ClusterIPs: []string{"10.9.8.7"},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "service2"},
				Spec: api.ServiceSpec{
					Type: api.ServiceTypeNodePort,
					Ports: []api.ServicePort{
						{
							Protocol: "udp",
							Port:     5566,
						},
					},
					ClusterIPs: []string{"1.2.3.4"},
				},
			},
		},
	}

	// 列： Name, Type, Cluster-IP, External-IP, Port(s), Age
	expectedRows := []metav1.TableRow{
		{Cells: []interface{}{"service1", "ClusterIP", "10.9.8.7", "<none>", "2233/tcp", "<unknown>"}},
		{Cells: []interface{}{"service2", "NodePort", "1.2.3.4", "<none>", "5566/udp", "<unknown>"}},
	}

	rows, err := printServiceList(&serviceList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error printing service list: %#v", err)
	}
	for i := range rows {
		rows[i].Object.Object = nil
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expectedRows, rows))
	}
}

func TestPrintPodDisruptionBudget(t *testing.T) {
	minAvailable := intstr.FromInt32(22)
	maxUnavailable := intstr.FromInt32(11)
	tests := []struct {
		pdb      policy.PodDisruptionBudget
		expected []metav1.TableRow
	}{
		// 已设置 Min Available，未设置 Max Available。
		{
			pdb: policy.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         "ns1",
					Name:              "pdb1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: policy.PodDisruptionBudgetSpec{
					MinAvailable: &minAvailable,
				},
				Status: policy.PodDisruptionBudgetStatus{
					DisruptionsAllowed: 5,
				},
			},
			// 列： Name, Min Available, Max Available, Allowed Disruptions, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"pdb1", "22", "N/A", int64(5), "0s"}}},
		},
		// 已设置 Max Available，未设置 Min Available。
		{
			pdb: policy.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         "ns2",
					Name:              "pdb2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: policy.PodDisruptionBudgetSpec{
					MaxUnavailable: &maxUnavailable,
				},
				Status: policy.PodDisruptionBudgetStatus{
					DisruptionsAllowed: 5,
				},
			},
			// 列： Name, Min Available, Max Available, Allowed Disruptions, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"pdb2", "N/A", "11", int64(5), "0s"}}},
		}}

	for i, test := range tests {
		rows, err := printPodDisruptionBudget(&test.pdb, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintPodDisruptionBudgetList(t *testing.T) {
	minAvailable := intstr.FromInt32(22)
	maxUnavailable := intstr.FromInt32(11)

	pdbList := policy.PodDisruptionBudgetList{
		Items: []policy.PodDisruptionBudget{
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         "ns1",
					Name:              "pdb1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: policy.PodDisruptionBudgetSpec{
					MaxUnavailable: &maxUnavailable,
				},
				Status: policy.PodDisruptionBudgetStatus{
					DisruptionsAllowed: 5,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         "ns2",
					Name:              "pdb2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: policy.PodDisruptionBudgetSpec{
					MinAvailable: &minAvailable,
				},
				Status: policy.PodDisruptionBudgetStatus{
					DisruptionsAllowed: 3,
				},
			},
		},
	}

	// 列： Name, Min Available, Max Available, Allowed Disruptions, Age
	expectedRows := []metav1.TableRow{
		{Cells: []interface{}{"pdb1", "N/A", "11", int64(5), "0s"}},
		{Cells: []interface{}{"pdb2", "22", "N/A", int64(3), "0s"}},
	}

	rows, err := printPodDisruptionBudgetList(&pdbList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error printing pod template list: %#v", err)
	}
	for i := range rows {
		rows[i].Object.Object = nil
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expectedRows, rows))
	}
}

func TestPrintControllerRevision(t *testing.T) {
	tests := []struct {
		history  apps.ControllerRevision
		expected []metav1.TableRow
	}{
		{
			history: apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
					OwnerReferences: []metav1.OwnerReference{
						{
							Controller: ptr.To(true),
							APIVersion: "apps/v1",
							Kind:       "DaemonSet",
							Name:       "foo",
						},
					},
				},
				Revision: 1,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", "daemonset.apps/foo", int64(1), "0s"}}},
		},
		{
			history: apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
					OwnerReferences: []metav1.OwnerReference{
						{
							Controller: ptr.To(false),
							Kind:       "ABC",
							Name:       "foo",
						},
					},
				},
				Revision: 2,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test2", "<none>", int64(2), "0s"}}},
		},
		{
			history: apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test3",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
					OwnerReferences:   []metav1.OwnerReference{},
				},
				Revision: 3,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test3", "<none>", int64(3), "0s"}}},
		},
		{
			history: apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test4",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
					OwnerReferences:   nil,
				},
				Revision: 4,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test4", "<none>", int64(4), "0s"}}},
		},
	}

	for i, test := range tests {
		rows, err := printControllerRevision(&test.history, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintConfigMap(t *testing.T) {
	tests := []struct {
		configMap api.ConfigMap
		expected  []metav1.TableRow
	}{
		// 无数据的基本 ConfigMap。
		{
			configMap: api.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "configmap1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
			},
			// 列： Name, Data, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"configmap1", int64(0), "0s"}}},
		},
		// 包含一个数据项的基本 ConfigMap
		{
			configMap: api.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "configmap2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Data: map[string]string{
					"foo": "bar",
				},
			},
			// 列： Name, (Num) Data, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"configmap2", int64(1), "0s"}}},
		},
		// 包含一个数据项和一个二进制数据项的基本 ConfigMap。
		{
			configMap: api.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "configmap3",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Data: map[string]string{
					"foo": "bar",
				},
				BinaryData: map[string][]byte{
					"bin": []byte("binary data"),
				},
			},
			// 列： Name, (Num) Data, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"configmap3", int64(2), "0s"}}},
		},
	}

	for i, test := range tests {
		rows, err := printConfigMap(&test.configMap, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintNetworkPolicy(t *testing.T) {
	tests := []struct {
		policy   networking.NetworkPolicy
		expected []metav1.TableRow
	}{
		// 空 Spec 的基本 NetworkPolicy。
		{
			policy: networking.NetworkPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "policy1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: networking.NetworkPolicySpec{},
			},
			// 列： Name, Pod-Selector, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"policy1", "<none>", "0s"}}},
		},
		// 带 Pod 选择器的基本 NetworkPolicy。
		{
			policy: networking.NetworkPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "policy2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: networking.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
				},
			},
			// 列： Name, Pod-Selector, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"policy2", "foo=bar", "0s"}}},
		},
	}

	for i, test := range tests {
		rows, err := printNetworkPolicy(&test.policy, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintRoleBinding(t *testing.T) {
	tests := []struct {
		binding  rbac.RoleBinding
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		// 基本 RoleBinding
		{
			binding: rbac.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "binding1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Subjects: []rbac.Subject{
					{
						Kind: "User",
						Name: "system:kube-controller-manager",
					},
				},
				RoleRef: rbac.RoleRef{
					Kind: "Role",
					Name: "extension-apiserver-authentication-reader",
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"binding1", "Role/extension-apiserver-authentication-reader", "0s"}}},
		},
		// 生成选项为 Wide；打印主体和角色。
		{
			binding: rbac.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "binding2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Subjects: []rbac.Subject{
					{
						Kind: "User",
						Name: "user-name",
					},
					{
						Kind: "Group",
						Name: "group-name",
					},
					{
						Kind:      "ServiceAccount",
						Name:      "service-account-name",
						Namespace: "service-account-namespace",
					},
				},
				RoleRef: rbac.RoleRef{
					Kind: "Role",
					Name: "role-name",
				},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Name, Age, Role, Users, Groups, ServiceAccounts
			expected: []metav1.TableRow{{Cells: []interface{}{"binding2", "Role/role-name", "0s", "user-name", "group-name", "service-account-namespace/service-account-name"}}},
		},
	}

	for i, test := range tests {
		rows, err := printRoleBinding(&test.binding, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintClusterRoleBinding(t *testing.T) {
	tests := []struct {
		binding  rbac.ClusterRoleBinding
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		// 基本 ClusterRoleBinding
		{
			binding: rbac.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "binding1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Subjects: []rbac.Subject{
					{
						Kind: "User",
						Name: "system:kube-controller-manager",
					},
				},
				RoleRef: rbac.RoleRef{
					Kind: "Role",
					Name: "extension-apiserver-authentication-reader",
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"binding1", "Role/extension-apiserver-authentication-reader", "0s"}}},
		},
		// 生成选项为 Wide；打印主体和角色。
		{
			binding: rbac.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "binding2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Subjects: []rbac.Subject{
					{
						Kind: "User",
						Name: "user-name",
					},
					{
						Kind: "Group",
						Name: "group-name",
					},
					{
						Kind:      "ServiceAccount",
						Name:      "service-account-name",
						Namespace: "service-account-namespace",
					},
				},
				RoleRef: rbac.RoleRef{
					Kind: "Role",
					Name: "role-name",
				},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Name, Age, Role, Users, Groups, ServiceAccounts
			expected: []metav1.TableRow{{Cells: []interface{}{"binding2", "Role/role-name", "0s", "user-name", "group-name", "service-account-namespace/service-account-name"}}},
		},
	}

	for i, test := range tests {
		rows, err := printClusterRoleBinding(&test.binding, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}
func TestPrintCertificateSigningRequest(t *testing.T) {
	tests := []struct {
		csr      certificates.CertificateSigningRequest
		expected []metav1.TableRow
	}{
		// 基本 CSR，未设置 Spec 或 Status；默认状态为 Pending。
		{
			csr: certificates.CertificateSigningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "csr1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec:   certificates.CertificateSigningRequestSpec{},
				Status: certificates.CertificateSigningRequestStatus{},
			},
			// 列： Name, Age, SignerName, Requestor, RequestedDuration, Condition
			expected: []metav1.TableRow{{Cells: []interface{}{"csr1", "0s", "<none>", "", "<none>", "Pending"}}},
		},
		// 基本 CSR，已设置 Spec 且 Status=Approved。
		{
			csr: certificates.CertificateSigningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "csr2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: certificates.CertificateSigningRequestSpec{
					Username: "CSR Requestor",
				},
				Status: certificates.CertificateSigningRequestStatus{
					Conditions: []certificates.CertificateSigningRequestCondition{
						{
							Type: certificates.CertificateApproved,
						},
					},
				},
			},
			// 列： Name, Age, SignerName, Requestor, RequestedDuration, Condition
			expected: []metav1.TableRow{{Cells: []interface{}{"csr2", "0s", "<none>", "CSR Requestor", "<none>", "Approved"}}},
		},
		// 基本 CSR，已设置 Spec 和 SignerName
		{
			csr: certificates.CertificateSigningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "csr2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: certificates.CertificateSigningRequestSpec{
					Username:   "CSR Requestor",
					SignerName: "example.com/test-signer",
				},
				Status: certificates.CertificateSigningRequestStatus{
					Conditions: []certificates.CertificateSigningRequestCondition{
						{
							Type: certificates.CertificateApproved,
						},
					},
				},
			},
			// 列： Name, Age, SignerName, Requestor, RequestedDuration, Condition
			expected: []metav1.TableRow{{Cells: []interface{}{"csr2", "0s", "example.com/test-signer", "CSR Requestor", "<none>", "Approved"}}},
		},
		// 基本 CSR，已设置 Spec、SignerName 和 ExpirationSeconds
		{
			csr: certificates.CertificateSigningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "csr2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: certificates.CertificateSigningRequestSpec{
					Username:          "CSR Requestor",
					SignerName:        "example.com/test-signer",
					ExpirationSeconds: csr.DurationToExpirationSeconds(7*24*time.Hour + time.Hour), // 比一周稍长
				},
				Status: certificates.CertificateSigningRequestStatus{
					Conditions: []certificates.CertificateSigningRequestCondition{
						{
							Type: certificates.CertificateApproved,
						},
					},
				},
			},
			// 列： Name, Age, SignerName, Requestor, RequestedDuration, Condition
			expected: []metav1.TableRow{{Cells: []interface{}{"csr2", "0s", "example.com/test-signer", "CSR Requestor", "7d1h", "Approved"}}},
		},
		// 基本 CSR，已设置 Spec 且 Status=Approved；已签发证书。
		{
			csr: certificates.CertificateSigningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "csr2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: certificates.CertificateSigningRequestSpec{
					Username: "CSR Requestor",
				},
				Status: certificates.CertificateSigningRequestStatus{
					Conditions: []certificates.CertificateSigningRequestCondition{
						{
							Type: certificates.CertificateApproved,
						},
					},
					Certificate: []byte("cert data"),
				},
			},
			// 列： Name, Age, SignerName, Requestor, RequestedDuration, Condition
			expected: []metav1.TableRow{{Cells: []interface{}{"csr2", "0s", "<none>", "CSR Requestor", "<none>", "Approved,Issued"}}},
		},
		// 基本 CSR，已设置 Spec 且 Status=Denied。
		{
			csr: certificates.CertificateSigningRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "csr3",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: certificates.CertificateSigningRequestSpec{
					Username: "CSR Requestor",
				},
				Status: certificates.CertificateSigningRequestStatus{
					Conditions: []certificates.CertificateSigningRequestCondition{
						{
							Type: certificates.CertificateDenied,
						},
					},
				},
			},
			// 列： Name, Age, SignerName, Requestor, RequestedDuration, Condition
			expected: []metav1.TableRow{{Cells: []interface{}{"csr3", "0s", "<none>", "CSR Requestor", "<none>", "Denied"}}},
		},
	}

	for i, test := range tests {
		rows, err := printCertificateSigningRequest(&test.csr, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintReplicationController(t *testing.T) {
	tests := []struct {
		name     string
		rc       api.ReplicationController
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		// 基本 ReplicationController 打印结果，未设置副本数或状态。
		{
			name: "basic",
			rc: api.ReplicationController{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rc1",
					Namespace: "test-namespace",
				},
				Spec: api.ReplicationControllerSpec{
					Replicas: ptr.To[int32](0),
					Selector: map[string]string{"a": "b"},
					Template: &api.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"a": "b"},
						},
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:                     "test",
									Image:                    "test_image",
									ImagePullPolicy:          api.PullIfNotPresent,
									TerminationMessagePolicy: api.TerminationMessageReadFile,
								},
							},
							RestartPolicy: api.RestartPolicyAlways,
							DNSPolicy:     api.DNSClusterFirst,
						},
					},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Desired, Current, Ready, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"rc1", int64(0), int64(0), int64(0), "<unknown>"}}},
		},
		// 基本 ReplicationController 打印结果，已设置副本数；不打印容器或标签
		{
			name: "basic with replicas",
			rc: api.ReplicationController{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rc1",
					Namespace: "test-namespace",
				},
				Spec: api.ReplicationControllerSpec{
					Replicas: ptr.To[int32](5),
					Selector: map[string]string{"a": "b"},
					Template: &api.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"a": "b"},
						},
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:                     "test",
									Image:                    "test_image",
									ImagePullPolicy:          api.PullIfNotPresent,
									TerminationMessagePolicy: api.TerminationMessageReadFile,
								},
							},
							RestartPolicy: api.RestartPolicyAlways,
							DNSPolicy:     api.DNSClusterFirst,
						},
					},
				},
				Status: api.ReplicationControllerStatus{
					Replicas:      3,
					ReadyReplicas: 1,
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Desired, Current, Ready, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"rc1", int64(5), int64(3), int64(1), "<unknown>"}}},
		},
		// 生成选项为 Wide；打印容器和标签。
		{
			name: "wide",
			rc: api.ReplicationController{
				ObjectMeta: metav1.ObjectMeta{
					Name: "rc1",
				},
				Spec: api.ReplicationControllerSpec{
					Replicas: ptr.To[int32](5),
					Selector: map[string]string{"a": "b"},
					Template: &api.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"a": "b"},
						},
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:                     "test",
									Image:                    "test_image",
									ImagePullPolicy:          api.PullIfNotPresent,
									TerminationMessagePolicy: api.TerminationMessageReadFile,
								},
							},
							RestartPolicy: api.RestartPolicyAlways,
							DNSPolicy:     api.DNSClusterFirst,
						},
					},
				},
				Status: api.ReplicationControllerStatus{
					Replicas:      3,
					ReadyReplicas: 1,
				},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Name, Desired, Current, Ready, Age, Containers, Images, Selector
			expected: []metav1.TableRow{{Cells: []interface{}{"rc1", int64(5), int64(3), int64(1), "<unknown>", "test", "test_image", "a=b"}}},
		},
		{
			// 确保 Bookmark 事件不会引发 panic
			name: "no panic",
			rc: api.ReplicationController{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						metav1.InitialEventsAnnotationKey: "true",
					},
				},
				Spec: api.ReplicationControllerSpec{
					Replicas: ptr.To[int32](0),
				},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Name, Desired, Current, Ready, Age, Containers, Images, Selector
			expected: []metav1.TableRow{{Cells: []interface{}{"", int64(0), int64(0), int64(0), "<unknown>", "", "", "<none>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printReplicationController(&test.rc, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintReplicaSet(t *testing.T) {
	tests := []struct {
		replicaSet apps.ReplicaSet
		options    printers.GenerateOptions
		expected   []metav1.TableRow
	}{
		// 生成选项为空
		{
			replicaSet: apps.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: apps.ReplicaSetSpec{
					Replicas: 5,
					Template: api.PodTemplateSpec{
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:  "fake-container1",
									Image: "fake-image1",
								},
								{
									Name:  "fake-container2",
									Image: "fake-image2",
								},
							},
						},
					},
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
				},
				Status: apps.ReplicaSetStatus{
					Replicas:      5,
					ReadyReplicas: 2,
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Desired, Current, Ready, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", int64(5), int64(5), int64(2), "0s"}}},
		},
		// 生成选项为 "Wide"
		{
			replicaSet: apps.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: apps.ReplicaSetSpec{
					Replicas: 5,
					Template: api.PodTemplateSpec{
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:  "fake-container1",
									Image: "fake-image1",
								},
								{
									Name:  "fake-container2",
									Image: "fake-image2",
								},
							},
						},
					},
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
				},
				Status: apps.ReplicaSetStatus{
					Replicas:      5,
					ReadyReplicas: 2,
				},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Name, Desired, Current, Ready, Age, Containers, Images, Selector
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", int64(5), int64(5), int64(2), "0s", "fake-container1,fake-container2", "fake-image1,fake-image2", "foo=bar"}}},
		},
	}

	for i, test := range tests {
		rows, err := printReplicaSet(&test.replicaSet, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintReplicaSetList(t *testing.T) {

	replicaSetList := apps.ReplicaSetList{
		Items: []apps.ReplicaSet{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "replicaset1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: apps.ReplicaSetSpec{
					Replicas: 5,
					Template: api.PodTemplateSpec{
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:  "fake-container1",
									Image: "fake-image1",
								},
								{
									Name:  "fake-container2",
									Image: "fake-image2",
								},
							},
						},
					},
				},
				Status: apps.ReplicaSetStatus{
					Replicas:      5,
					ReadyReplicas: 2,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "replicaset2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: apps.ReplicaSetSpec{
					Replicas: 4,
					Template: api.PodTemplateSpec{},
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
				},
				Status: apps.ReplicaSetStatus{
					Replicas:      3,
					ReadyReplicas: 1,
				},
			},
		},
	}

	// 列： Name, Desired, Current, Ready, Age
	expectedRows := []metav1.TableRow{
		{Cells: []interface{}{"replicaset1", int64(5), int64(5), int64(2), "0s"}},
		{Cells: []interface{}{"replicaset2", int64(4), int64(3), int64(1), "0s"}},
	}

	rows, err := printReplicaSetList(&replicaSetList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error printing replica set list: %#v", err)
	}
	for i := range rows {
		rows[i].Object.Object = nil
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expectedRows, rows))
	}
}

func TestPrintStatefulSet(t *testing.T) {
	tests := []struct {
		statefulSet apps.StatefulSet
		options     printers.GenerateOptions
		expected    []metav1.TableRow
	}{
		// 基本 StatefulSet；不指定生成选项。
		{
			statefulSet: apps.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: apps.StatefulSetSpec{
					Replicas: 5,
					Template: api.PodTemplateSpec{
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:  "fake-container1",
									Image: "fake-image1",
								},
								{
									Name:  "fake-container2",
									Image: "fake-image2",
								},
							},
						},
					},
				},
				Status: apps.StatefulSetStatus{
					Replicas:      5,
					ReadyReplicas: 2,
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Ready, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", "2/5", "0s"}}},
		},
		// 生成选项为 "Wide"；包含容器和镜像。
		{
			statefulSet: apps.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: apps.StatefulSetSpec{
					Replicas: 5,
					Template: api.PodTemplateSpec{
						Spec: api.PodSpec{
							Containers: []api.Container{
								{
									Name:  "fake-container1",
									Image: "fake-image1",
								},
								{
									Name:  "fake-container2",
									Image: "fake-image2",
								},
							},
						},
					},
				},
				Status: apps.StatefulSetStatus{
					Replicas:      5,
					ReadyReplicas: 2,
				},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Name, Ready, Age, Containers, Images
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", "2/5", "0s", "fake-container1,fake-container2", "fake-image1,fake-image2"}}},
		},
	}

	for i, test := range tests {
		rows, err := printStatefulSet(&test.statefulSet, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintPersistentVolume(t *testing.T) {
	myScn := "my-scn"
	myVacn := "my-vacn"

	claimRef := api.ObjectReference{
		Name:      "test",
		Namespace: "default",
	}
	tests := []struct {
		pv       api.PersistentVolume
		expected []metav1.TableRow
	}{
		{
			// 测试 Bound 状态
			pv: api.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test1",
				},
				Spec: api.PersistentVolumeSpec{
					ClaimRef:    &claimRef,
					AccessModes: []api.PersistentVolumeAccessMode{api.ReadOnlyMany},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("4Gi"),
					},
				},
				Status: api.PersistentVolumeStatus{
					Phase: api.VolumeBound,
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", "4Gi", "ROX", "", "Bound", "default/test", "", "<unset>", "", "<unknown>", "<unset>"}}},
		},
		{
			// 测试 Failed 状态
			pv: api.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test2",
				},
				Spec: api.PersistentVolumeSpec{
					ClaimRef:    &claimRef,
					AccessModes: []api.PersistentVolumeAccessMode{api.ReadOnlyMany},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("4Gi"),
					},
				},
				Status: api.PersistentVolumeStatus{
					Phase: api.VolumeFailed,
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test2", "4Gi", "ROX", "", "Failed", "default/test", "", "<unset>", "", "<unknown>", "<unset>"}}},
		},
		{
			// 测试 Pending 状态
			pv: api.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test3",
				},
				Spec: api.PersistentVolumeSpec{
					ClaimRef:    &claimRef,
					AccessModes: []api.PersistentVolumeAccessMode{api.ReadWriteMany},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				Status: api.PersistentVolumeStatus{
					Phase: api.VolumePending,
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test3", "10Gi", "RWX", "", "Pending", "default/test", "", "<unset>", "", "<unknown>", "<unset>"}}},
		},
		{
			// 测试 Pending 状态、StorageClass
			pv: api.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test4",
				},
				Spec: api.PersistentVolumeSpec{
					ClaimRef:         &claimRef,
					StorageClassName: myScn,
					AccessModes:      []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				Status: api.PersistentVolumeStatus{
					Phase: api.VolumePending,
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test4", "10Gi", "RWO", "", "Pending", "default/test", "my-scn", "<unset>", "", "<unknown>", "<unset>"}}},
		},
		{
			// 测试 Pending 状态、StorageClass、VolumeAttributesClass
			pv: api.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test4",
				},
				Spec: api.PersistentVolumeSpec{
					ClaimRef:                  &claimRef,
					StorageClassName:          myScn,
					VolumeAttributesClassName: &myVacn,
					AccessModes:               []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				Status: api.PersistentVolumeStatus{
					Phase: api.VolumePending,
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test4", "10Gi", "RWO", "", "Pending", "default/test", "my-scn", "my-vacn", "", "<unknown>", "<unset>"}}},
		},
		{
			// 测试 Available 状态
			pv: api.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test5",
				},
				Spec: api.PersistentVolumeSpec{
					ClaimRef:         &claimRef,
					StorageClassName: myScn,
					AccessModes:      []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				Status: api.PersistentVolumeStatus{
					Phase: api.VolumeAvailable,
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test5", "10Gi", "RWO", "", "Available", "default/test", "my-scn", "<unset>", "", "<unknown>", "<unset>"}}},
		},
		{
			// 测试 Released 状态
			pv: api.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test6",
				},
				Spec: api.PersistentVolumeSpec{
					ClaimRef:         &claimRef,
					StorageClassName: myScn,
					AccessModes:      []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				Status: api.PersistentVolumeStatus{
					Phase: api.VolumeReleased,
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test6", "10Gi", "RWO", "", "Released", "default/test", "my-scn", "<unset>", "", "<unknown>", "<unset>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printPersistentVolume(&test.pv, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintPersistentVolumeClaim(t *testing.T) {
	volumeMode := api.PersistentVolumeFilesystem
	myVacn := "my-vacn"
	myScn := "my-scn"
	tests := []struct {
		pvc      api.PersistentVolumeClaim
		expected []metav1.TableRow
	}{
		{
			// 验证名称、容器数、重启次数和容器就绪状态
			pvc: api.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test1",
				},
				Spec: api.PersistentVolumeClaimSpec{
					VolumeName: "my-volume",
					VolumeMode: &volumeMode,
				},
				Status: api.PersistentVolumeClaimStatus{
					Phase:       api.ClaimBound,
					AccessModes: []api.PersistentVolumeAccessMode{api.ReadOnlyMany},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("4Gi"),
					},
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", "Bound", "my-volume", "4Gi", "ROX", "", "<unset>", "<unknown>", "Filesystem"}}},
		},
		{
			// 验证名称、容器数、重启次数和容器就绪状态
			pvc: api.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test2",
				},
				Spec: api.PersistentVolumeClaimSpec{
					VolumeMode: &volumeMode,
				},
				Status: api.PersistentVolumeClaimStatus{
					Phase:       api.ClaimLost,
					AccessModes: []api.PersistentVolumeAccessMode{api.ReadOnlyMany},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("4Gi"),
					},
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test2", "Lost", "", "", "", "", "<unset>", "<unknown>", "Filesystem"}}},
		},
		{
			// 验证名称、容器数、重启次数和容器就绪状态
			pvc: api.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test3",
				},
				Spec: api.PersistentVolumeClaimSpec{
					VolumeName: "my-volume",
					VolumeMode: &volumeMode,
				},
				Status: api.PersistentVolumeClaimStatus{
					Phase:       api.ClaimPending,
					AccessModes: []api.PersistentVolumeAccessMode{api.ReadWriteMany},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test3", "Pending", "my-volume", "10Gi", "RWX", "", "<unset>", "<unknown>", "Filesystem"}}},
		},
		{
			// 验证名称、容器数、重启次数和容器就绪状态
			pvc: api.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test4",
				},
				Spec: api.PersistentVolumeClaimSpec{
					VolumeName:       "my-volume",
					StorageClassName: &myScn,
					VolumeMode:       &volumeMode,
				},
				Status: api.PersistentVolumeClaimStatus{
					Phase:       api.ClaimPending,
					AccessModes: []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test4", "Pending", "my-volume", "10Gi", "RWO", "my-scn", "<unset>", "<unknown>", "Filesystem"}}},
		},
		{
			// 验证名称、容器数、重启次数和容器就绪状态
			pvc: api.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test5",
				},
				Spec: api.PersistentVolumeClaimSpec{
					VolumeName:       "my-volume",
					StorageClassName: &myScn,
				},
				Status: api.PersistentVolumeClaimStatus{
					Phase:       api.ClaimPending,
					AccessModes: []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test5", "Pending", "my-volume", "10Gi", "RWO", "my-scn", "<unset>", "<unknown>", "<unset>"}}},
		},
		{
			// 验证名称、容器数、重启次数和容器就绪状态
			pvc: api.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test5",
				},
				Spec: api.PersistentVolumeClaimSpec{
					VolumeName:                "my-volume",
					StorageClassName:          &myScn,
					VolumeAttributesClassName: &myVacn,
				},
				Status: api.PersistentVolumeClaimStatus{
					Phase:       api.ClaimPending,
					AccessModes: []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
					Capacity: map[api.ResourceName]resource.Quantity{
						api.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"test5", "Pending", "my-volume", "10Gi", "RWO", "my-scn", "my-vacn", "<unknown>", "<unset>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printPersistentVolumeClaim(&test.pvc, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintComponentStatus(t *testing.T) {
	tests := []struct {
		componentStatus api.ComponentStatus
		expected        []metav1.TableRow
	}{
		// 无 Condition 的基本 ComponentStatus。
		{
			componentStatus: api.ComponentStatus{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cs1",
				},
				Conditions: []api.ComponentCondition{},
			},
			// 列： Name, Status, Message, Error
			expected: []metav1.TableRow{{Cells: []interface{}{"cs1", "Unknown", "", ""}}},
		},
		// 带健康 Condition 的基本 ComponentStatus。
		{
			componentStatus: api.ComponentStatus{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cs2",
				},
				Conditions: []api.ComponentCondition{
					{
						Type:    "Healthy",
						Status:  api.ConditionTrue,
						Message: "test message",
						Error:   "test error",
					},
				},
			},
			// 列： Name, Status, Message, Error
			expected: []metav1.TableRow{{Cells: []interface{}{"cs2", "Healthy", "test message", "test error"}}},
		},
		// 带健康 Condition 的基本 ComponentStatus。
		{
			componentStatus: api.ComponentStatus{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cs3",
				},
				Conditions: []api.ComponentCondition{
					{
						Type:    "Healthy",
						Status:  api.ConditionFalse,
						Message: "test message",
						Error:   "test error",
					},
				},
			},
			// 列： Name, Status, Message, Error
			expected: []metav1.TableRow{{Cells: []interface{}{"cs3", "Unhealthy", "test message", "test error"}}},
		},
	}

	for i, test := range tests {
		rows, err := printComponentStatus(&test.componentStatus, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintCronJob(t *testing.T) {
	timeZone := "LOCAL"
	completions := int32(2)
	suspend := false
	tests := []struct {
		cronjob  batch.CronJob
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		// 基本 CronJob；不打印容器、镜像或标签。
		{
			cronjob: batch.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "cronjob1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.CronJobSpec{
					Schedule: "0/5 * * * ?",
					Suspend:  &suspend,
					JobTemplate: batch.JobTemplateSpec{
						Spec: batch.JobSpec{
							Completions: &completions,
							Template: api.PodTemplateSpec{
								Spec: api.PodSpec{
									Containers: []api.Container{
										{
											Name:  "fake-job-container1",
											Image: "fake-job-image1",
										},
										{
											Name:  "fake-job-container2",
											Image: "fake-job-image2",
										},
									},
								},
							},
							Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
						},
					},
				},
				Status: batch.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: time.Now().Add(1.9e9)},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Schedule, Suspend, Active, Last Schedule, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"cronjob1", "0/5 * * * ?", "<none>", "False", int64(0), "0s", "0s"}}},
		},
		// 基本 CronJob；不打印容器、镜像或标签。
		{
			cronjob: batch.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "cronjob1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.CronJobSpec{
					Schedule: "0/5 * * * ?",
					TimeZone: &timeZone,
					Suspend:  &suspend,
					JobTemplate: batch.JobTemplateSpec{
						Spec: batch.JobSpec{
							Completions: &completions,
							Template: api.PodTemplateSpec{
								Spec: api.PodSpec{
									Containers: []api.Container{
										{
											Name:  "fake-job-container1",
											Image: "fake-job-image1",
										},
										{
											Name:  "fake-job-container2",
											Image: "fake-job-image2",
										},
									},
								},
							},
							Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
						},
					},
				},
				Status: batch.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: time.Now().Add(1.9e9)},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Schedule, Suspend, Active, Last Schedule, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"cronjob1", "0/5 * * * ?", "LOCAL", "False", int64(0), "0s", "0s"}}},
		},
		// 生成选项为 Wide；打印容器、镜像和标签。
		{
			cronjob: batch.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "cronjob1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.CronJobSpec{
					Schedule: "0/5 * * * ?",
					Suspend:  &suspend,
					JobTemplate: batch.JobTemplateSpec{
						Spec: batch.JobSpec{
							Completions: &completions,
							Template: api.PodTemplateSpec{
								Spec: api.PodSpec{
									Containers: []api.Container{
										{
											Name:  "fake-job-container1",
											Image: "fake-job-image1",
										},
										{
											Name:  "fake-job-container2",
											Image: "fake-job-image2",
										},
									},
								},
							},
							Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
						},
					},
				},
				Status: batch.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: time.Now().Add(1.9e9)},
				},
			},
			options: printers.GenerateOptions{Wide: true},
			// 列： Name, Schedule, Suspend, Active, Last Schedule, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"cronjob1", "0/5 * * * ?", "<none>", "False", int64(0), "0s", "0s", "fake-job-container1,fake-job-container2", "fake-job-image1,fake-job-image2", "a=b"}}},
		},
		// 包含 Last Schedule 和 Age 的 CronJob
		{
			cronjob: batch.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "cronjob2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: batch.CronJobSpec{
					Schedule: "0/5 * * * ?",
					Suspend:  &suspend,
				},
				Status: batch.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: time.Now().Add(-3e10)},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Schedule, Suspend, Active, Last Schedule, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"cronjob2", "0/5 * * * ?", "<none>", "False", int64(0), "30s", "5m"}}},
		},
		// 不包含 Last Schedule 的 CronJob
		{
			cronjob: batch.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "cronjob3",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: batch.CronJobSpec{
					Schedule: "0/5 * * * ?",
					Suspend:  &suspend,
				},
				Status: batch.CronJobStatus{},
			},
			options: printers.GenerateOptions{},
			// 列： Name, Schedule, Suspend, Active, Last Schedule, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"cronjob3", "0/5 * * * ?", "<none>", "False", int64(0), "<none>", "5m"}}},
		},
	}

	for i, test := range tests {
		rows, err := printCronJob(&test.cronjob, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintCronJobList(t *testing.T) {
	timeZone := "LOCAL"
	completions := int32(2)
	suspend := false

	cronJobList := batch.CronJobList{
		Items: []batch.CronJob{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "cronjob1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.CronJobSpec{
					Schedule: "0/5 * * * ?",
					TimeZone: &timeZone,
					Suspend:  &suspend,
					JobTemplate: batch.JobTemplateSpec{
						Spec: batch.JobSpec{
							Completions: &completions,
							Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
						},
					},
				},
				Status: batch.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: time.Now().Add(1.9e9)},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "cronjob2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: batch.CronJobSpec{
					Schedule: "4/5 1 1 1 ?",
					Suspend:  &suspend,
					JobTemplate: batch.JobTemplateSpec{
						Spec: batch.JobSpec{
							Completions: &completions,
							Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
						},
					},
				},
				Status: batch.CronJobStatus{
					LastScheduleTime: &metav1.Time{Time: time.Now().Add(-20 * time.Minute)},
				},
			},
		},
	}

	// 列： Name, Schedule, Suspend, Active, Last Schedule, Age
	expectedRows := []metav1.TableRow{
		{Cells: []interface{}{"cronjob1", "0/5 * * * ?", "LOCAL", "False", int64(0), "0s", "0s"}},
		{Cells: []interface{}{"cronjob2", "4/5 1 1 1 ?", "<none>", "False", int64(0), "20m", "0s"}},
	}

	rows, err := printCronJobList(&cronJobList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error printing job list: %#v", err)
	}
	for i := range rows {
		rows[i].Object.Object = nil
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expectedRows, rows))
	}
}

func TestPrintStorageClass(t *testing.T) {
	policyDelte := api.PersistentVolumeReclaimDelete
	policyRetain := api.PersistentVolumeReclaimRetain
	bindModeImmediate := storage.VolumeBindingImmediate
	bindModeWait := storage.VolumeBindingWaitForFirstConsumer
	tests := []struct {
		sc       storage.StorageClass
		expected []metav1.TableRow
	}{
		{
			sc: storage.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "sc1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Provisioner: "kubernetes.io/glusterfs",
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"sc1", "kubernetes.io/glusterfs", "Delete",
				"Immediate", false, "0s"}}},
		},
		{
			sc: storage.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "sc2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Provisioner: "kubernetes.io/nfs",
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"sc2", "kubernetes.io/nfs", "Delete",
				"Immediate", false, "5m"}}},
		},
		{
			sc: storage.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "sc3",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Provisioner:   "kubernetes.io/nfs",
				ReclaimPolicy: &policyDelte,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"sc3", "kubernetes.io/nfs", "Delete",
				"Immediate", false, "5m"}}},
		},
		{
			sc: storage.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "sc4",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Provisioner:       "kubernetes.io/nfs",
				ReclaimPolicy:     &policyRetain,
				VolumeBindingMode: &bindModeImmediate,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"sc4", "kubernetes.io/nfs", "Retain",
				"Immediate", false, "5m"}}},
		},
		{
			sc: storage.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "sc5",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Provisioner:       "kubernetes.io/nfs",
				ReclaimPolicy:     &policyRetain,
				VolumeBindingMode: &bindModeWait,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"sc5", "kubernetes.io/nfs", "Retain",
				"WaitForFirstConsumer", false, "5m"}}},
		},
		{
			sc: storage.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "sc6",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Provisioner:          "kubernetes.io/nfs",
				ReclaimPolicy:        &policyRetain,
				AllowVolumeExpansion: ptr.To(true),
				VolumeBindingMode:    &bindModeWait,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"sc6", "kubernetes.io/nfs", "Retain",
				"WaitForFirstConsumer", true, "5m"}}},
		},
	}

	for i, test := range tests {
		rows, err := printStorageClass(&test.sc, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintStorageClassListEffectiveDefault(t *testing.T) {
	now := time.Now()
	earlier := now.Add(-1 * time.Hour)

	tests := []struct {
		name     string
		scList   storage.StorageClassList
		expected []metav1.TableRow
	}{
		{
			name: "single default",
			scList: storage.StorageClassList{
				Items: []storage.StorageClass{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "standard",
							CreationTimestamp: metav1.Time{Time: now},
							Annotations: map[string]string{
								"storageclass.kubernetes.io/is-default-class": "true",
							},
						},
						Provisioner: "kubernetes.io/gce-pd",
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "fast",
							CreationTimestamp: metav1.Time{Time: earlier},
						},
						Provisioner: "kubernetes.io/gce-pd",
					},
				},
			},
			expected: []metav1.TableRow{
				{Cells: []interface{}{"standard (default)", "kubernetes.io/gce-pd", "Delete", "Immediate", false, "0s"}},
				{Cells: []interface{}{"fast", "kubernetes.io/gce-pd", "Delete", "Immediate", false, "60m"}},
			},
		},
		{
			name: "multiple defaults - newest wins",
			scList: storage.StorageClassList{
				Items: []storage.StorageClass{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "standard",
							CreationTimestamp: metav1.Time{Time: earlier},
							Annotations: map[string]string{
								"storageclass.kubernetes.io/is-default-class": "true",
							},
						},
						Provisioner: "kubernetes.io/gce-pd",
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "fast",
							CreationTimestamp: metav1.Time{Time: now},
							Annotations: map[string]string{
								"storageclass.kubernetes.io/is-default-class": "true",
							},
						},
						Provisioner: "kubernetes.io/gce-pd",
					},
				},
			},
			expected: []metav1.TableRow{
				{Cells: []interface{}{"standard", "kubernetes.io/gce-pd", "Delete", "Immediate", false, "60m"}},
				{Cells: []interface{}{"fast (default)", "kubernetes.io/gce-pd", "Delete", "Immediate", false, "0s"}},
			},
		},
		{
			name: "multiple defaults same timestamp - alphabetically first wins",
			scList: storage.StorageClassList{
				Items: []storage.StorageClass{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "zeta",
							CreationTimestamp: metav1.Time{Time: now},
							Annotations: map[string]string{
								"storageclass.kubernetes.io/is-default-class": "true",
							},
						},
						Provisioner: "kubernetes.io/gce-pd",
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "alpha",
							CreationTimestamp: metav1.Time{Time: now},
							Annotations: map[string]string{
								"storageclass.kubernetes.io/is-default-class": "true",
							},
						},
						Provisioner: "kubernetes.io/gce-pd",
					},
				},
			},
			expected: []metav1.TableRow{
				{Cells: []interface{}{"zeta", "kubernetes.io/gce-pd", "Delete", "Immediate", false, "0s"}},
				{Cells: []interface{}{"alpha (default)", "kubernetes.io/gce-pd", "Delete", "Immediate", false, "0s"}},
			},
		},
		{
			name: "no defaults",
			scList: storage.StorageClassList{
				Items: []storage.StorageClass{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "standard",
							CreationTimestamp: metav1.Time{Time: now},
						},
						Provisioner: "kubernetes.io/gce-pd",
					},
				},
			},
			expected: []metav1.TableRow{
				{Cells: []interface{}{"standard", "kubernetes.io/gce-pd", "Delete", "Immediate", false, "0s"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows, err := printStorageClassList(&test.scList, printers.GenerateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for i := range rows {
				rows[i].Object.Object = nil
			}
			if !reflect.DeepEqual(test.expected, rows) {
				t.Errorf("mismatch: %s", cmp.Diff(test.expected, rows))
			}
		})
	}
}

func TestPrintVolumeAttributesClass(t *testing.T) {
	tests := []struct {
		vac      storage.VolumeAttributesClass
		expected []metav1.TableRow
	}{
		{
			vac: storage.VolumeAttributesClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "vac1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				DriverName: "fake",
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"vac1", "fake", "0s"}}},
		},
		{
			vac: storage.VolumeAttributesClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "vac2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				DriverName: "fake",
				Parameters: map[string]string{
					"iops":       "500",
					"throughput": "50MiB/s",
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"vac2", "fake", "5m"}}},
		},
	}

	for i, test := range tests {
		rows, err := printVolumeAttributesClass(&test.vac, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintLease(t *testing.T) {
	holder1 := "holder1"
	holder2 := "holder2"
	tests := []struct {
		lease    coordination.Lease
		expected []metav1.TableRow
	}{
		{
			lease: coordination.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "lease1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: coordination.LeaseSpec{
					HolderIdentity: &holder1,
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"lease1", "holder1", "0s"}}},
		},
		{
			lease: coordination.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "lease2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: coordination.LeaseSpec{
					HolderIdentity: &holder2,
				},
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"lease2", "holder2", "5m"}}},
		},
	}

	for i, test := range tests {
		rows, err := printLease(&test.lease, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintPriorityClass(t *testing.T) {
	preemptNever := api.PreemptNever
	preemptLowerPriority := api.PreemptLowerPriority
	tests := []struct {
		name     string
		options  printers.GenerateOptions
		pc       scheduling.PriorityClass
		expected []metav1.TableRow
	}{
		{
			name: "Test case with PreemptNever policy",
			pc: scheduling.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pc1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Value:            1,
				PreemptionPolicy: &preemptNever,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"pc1", int64(1), bool(false), "0s"}}},
		},
		{
			name: "Test case with PreemptLowerPriority policy",
			pc: scheduling.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pc2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Value:            1000000000,
				GlobalDefault:    true,
				PreemptionPolicy: &preemptLowerPriority,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"pc2", int64(1000000000), bool(true), "5m"}}},
		},
		{
			name:    "Test case with PreemptLowerPriority policy and wide output",
			options: printers.GenerateOptions{Wide: true},
			pc: scheduling.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pc2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Value:            1000000000,
				GlobalDefault:    true,
				PreemptionPolicy: &preemptLowerPriority,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"pc2", int64(1000000000), bool(true), "5m", string(api.PreemptLowerPriority)}}},
		},
		{
			name:    "Test case without set PreemptLowerPriority policy",
			options: printers.GenerateOptions{Wide: true},
			pc: scheduling.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pc2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Value:            1000000000,
				GlobalDefault:    true,
				PreemptionPolicy: nil,
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"pc2", int64(1000000000), bool(true), "5m", string("")}}},
		},
	}

	for i, test := range tests {
		rows, err := printPriorityClass(&test.pc, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("test case %s: %d mismatch: %s", test.name, i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintRuntimeClass(t *testing.T) {
	tests := []struct {
		rc       nodeapi.RuntimeClass
		expected []metav1.TableRow
	}{
		{
			rc: nodeapi.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "rc1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Handler: "h1",
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"rc1", "h1", "0s"}}},
		},
		{
			rc: nodeapi.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "rc2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Handler: "h2",
			},
			expected: []metav1.TableRow{{Cells: []interface{}{"rc2", "h2", "5m"}}},
		},
	}

	for i, test := range tests {
		rows, err := printRuntimeClass(&test.rc, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintEndpoint(t *testing.T) {

	tests := []struct {
		endpoint api.Endpoints
		expected []metav1.TableRow
	}{
		// 无 IP 的基本 Endpoint。
		{
			endpoint: api.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "endpoint1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
			},
			// 列： Name, Endpoints, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"endpoint1", "<none>", "0s"}}},
		},
		// 无端口的 Endpoint
		{
			endpoint: api.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "endpoint3",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Subsets: []api.EndpointSubset{
					{
						Addresses: []api.EndpointAddress{
							{
								IP: "1.2.3.4",
							},
							{
								IP: "5.6.7.8",
							},
						},
					},
				},
			},
			// 列： Name, Endpoints, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"endpoint3", "1.2.3.4,5.6.7.8", "5m"}}},
		},
		// 包含两个 IP 和一个端口的基本 Endpoint
		{
			endpoint: api.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "endpoint2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Subsets: []api.EndpointSubset{
					{
						Addresses: []api.EndpointAddress{
							{
								IP: "1.2.3.4",
							},
							{
								IP: "5.6.7.8",
							},
						},
						Ports: []api.EndpointPort{
							{
								Port:     8001,
								Protocol: "tcp",
							},
						},
					},
				},
			},
			// 列： Name, Endpoints, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"endpoint2", "1.2.3.4:8001,5.6.7.8:8001", "0s"}}},
		},
		// 包含超过三个 IP 并触发 "more" 字符串的基本 Endpoint
		{
			endpoint: api.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "endpoint2",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Subsets: []api.EndpointSubset{
					{
						Addresses: []api.EndpointAddress{
							{
								IP: "1.2.3.4",
							},
							{
								IP: "5.6.7.8",
							},
							{
								IP: "9.8.7.6",
							},
							{
								IP: "6.6.6.6",
							},
						},
						Ports: []api.EndpointPort{
							{
								Port:     8001,
								Protocol: "tcp",
							},
						},
					},
				},
			},
			// 列： Name, Endpoints, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"endpoint2", "1.2.3.4:8001,5.6.7.8:8001,9.8.7.6:8001 + 1 more...", "0s"}}},
		},
	}

	for i, test := range tests {
		rows, err := printEndpoints(&test.endpoint, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}

}

func TestPrintEndpointSlice(t *testing.T) {
	tcpProtocol := apiv1.ProtocolTCP

	tests := []struct {
		endpointSlice discovery.EndpointSlice
		expected      []metav1.TableRow
	}{
		{
			endpointSlice: discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "abcslice.123",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Port:     ptr.To[int32](80),
					Protocol: &tcpProtocol,
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"10.1.2.3", "2001:db8::1234:5678"},
				}},
			},
			// 列： Name, AddressType, Ports, Endpoints, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"abcslice.123", "IPv4", "80", "10.1.2.3,2001:db8::1234:5678", "0s"}}},
		}, {
			endpointSlice: discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "longerslicename.123",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				AddressType: discovery.AddressTypeIPv6,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Port:     ptr.To[int32](80),
					Protocol: &tcpProtocol,
				}, {
					Name:     ptr.To("https"),
					Port:     ptr.To[int32](443),
					Protocol: &tcpProtocol,
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"10.1.2.3", "2001:db8::1234:5678"},
				}, {
					Addresses: []string{"10.2.3.4", "2001:db8::2345:6789"},
				}},
			},
			// 列： Name, AddressType, Ports, Endpoints, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"longerslicename.123", "IPv6", "80,443", "10.1.2.3,2001:db8::1234:5678,10.2.3.4 + 1 more...", "5m"}}},
		}, {
			endpointSlice: discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "multiportslice.123",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Port:     ptr.To[int32](80),
					Protocol: &tcpProtocol,
				}, {
					Name:     ptr.To("https"),
					Port:     ptr.To[int32](443),
					Protocol: &tcpProtocol,
				}, {
					Name:     ptr.To("extra1"),
					Port:     ptr.To[int32](3000),
					Protocol: &tcpProtocol,
				}, {
					Name:     ptr.To("extra2"),
					Port:     ptr.To[int32](3001),
					Protocol: &tcpProtocol,
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"10.1.2.3", "2001:db8::1234:5678"},
				}, {
					Addresses: []string{"10.2.3.4", "2001:db8::2345:6789"},
				}},
			},
			// 列： Name, AddressType, Ports, Endpoints, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"multiportslice.123", "IPv4", "80,443,3000 + 1 more...", "10.1.2.3,2001:db8::1234:5678,10.2.3.4 + 1 more...", "5m"}}},
		},
	}

	for i, test := range tests {
		rows, err := printEndpointSlice(&test.endpointSlice, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintFlowSchema(t *testing.T) {
	all := []string{"*"}

	tests := []struct {
		fs       flowcontrol.FlowSchema
		expected []metav1.TableRow
	}{
		{
			fs: flowcontrol.FlowSchema{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "all-matcher",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: flowcontrol.FlowSchemaSpec{
					PriorityLevelConfiguration: flowcontrol.PriorityLevelConfigurationReference{Name: "allee"},
					MatchingPrecedence:         math.MaxInt32,
					DistinguisherMethod:        &flowcontrol.FlowDistinguisherMethod{Type: flowcontrol.FlowDistinguisherMethodByUserType},
					Rules: []flowcontrol.PolicyRulesWithSubjects{{
						Subjects: []flowcontrol.Subject{{
							Kind:  flowcontrol.SubjectKindGroup,
							Group: &flowcontrol.GroupSubject{Name: "system:authenticated"},
						}},
						ResourceRules: []flowcontrol.ResourcePolicyRule{{
							Verbs:        all,
							APIGroups:    all,
							Resources:    all,
							ClusterScope: true,
							Namespaces:   all,
						}},
					}, {
						Subjects: []flowcontrol.Subject{{
							Kind:  flowcontrol.SubjectKindGroup,
							Group: &flowcontrol.GroupSubject{Name: "system:unauthenticated"},
						}},
						ResourceRules: []flowcontrol.ResourcePolicyRule{{
							Verbs:        all,
							APIGroups:    all,
							Resources:    all,
							ClusterScope: true,
							Namespaces:   all,
						}},
					}, {
						Subjects: []flowcontrol.Subject{{
							Kind:  flowcontrol.SubjectKindGroup,
							Group: &flowcontrol.GroupSubject{Name: "system:authenticated"},
						}, {
							Kind:  flowcontrol.SubjectKindGroup,
							Group: &flowcontrol.GroupSubject{Name: "system:unauthenticated"},
						}},
						NonResourceRules: []flowcontrol.NonResourcePolicyRule{{
							Verbs:           all,
							NonResourceURLs: all,
						}},
					}},
				},
			},
			// 列： Name, PriorityLevelName, MatchingPrecedence, DistinguisherMethod, Age, MissingPL
			expected: []metav1.TableRow{{Cells: []interface{}{"all-matcher", "allee", int64(math.MaxInt32), "ByUser", "0s", "?"}}},
		}, {
			fs: flowcontrol.FlowSchema{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "some-matcher",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: flowcontrol.FlowSchemaSpec{
					PriorityLevelConfiguration: flowcontrol.PriorityLevelConfigurationReference{Name: "allee"},
					MatchingPrecedence:         0,
					DistinguisherMethod:        &flowcontrol.FlowDistinguisherMethod{Type: flowcontrol.FlowDistinguisherMethodByNamespaceType},
					Rules: []flowcontrol.PolicyRulesWithSubjects{{
						Subjects: []flowcontrol.Subject{{
							Kind:  flowcontrol.SubjectKindGroup,
							Group: &flowcontrol.GroupSubject{Name: "system:unauthenticated"},
						}},
						ResourceRules: []flowcontrol.ResourcePolicyRule{{
							Verbs:        all,
							APIGroups:    all,
							Resources:    all,
							ClusterScope: true,
							Namespaces:   all,
						}},
					}, {
						Subjects: []flowcontrol.Subject{{
							Kind:  flowcontrol.SubjectKindGroup,
							Group: &flowcontrol.GroupSubject{Name: "system:authenticated"},
						}, {
							Kind:  flowcontrol.SubjectKindGroup,
							Group: &flowcontrol.GroupSubject{Name: "system:unauthenticated"},
						}},
						NonResourceRules: []flowcontrol.NonResourcePolicyRule{{
							Verbs:           all,
							NonResourceURLs: all,
						}},
					}},
				},
				Status: flowcontrol.FlowSchemaStatus{
					Conditions: []flowcontrol.FlowSchemaCondition{{
						Type:               flowcontrol.FlowSchemaConditionDangling,
						Status:             "True",
						LastTransitionTime: metav1.Time{Time: time.Now().Add(-time.Hour)},
					}},
				},
			},
			// 列： Name, PriorityLevelName, MatchingPrecedence, DistinguisherMethod, Age, MissingPL
			expected: []metav1.TableRow{{Cells: []interface{}{"some-matcher", "allee", int64(0), "ByNamespace", "5m", "True"}}},
		}, {
			fs: flowcontrol.FlowSchema{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "exempt",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: flowcontrol.FlowSchemaSpec{
					PriorityLevelConfiguration: flowcontrol.PriorityLevelConfigurationReference{Name: "allee"},
					MatchingPrecedence:         0,
					DistinguisherMethod:        nil,
					Rules: []flowcontrol.PolicyRulesWithSubjects{{
						Subjects: []flowcontrol.Subject{{
							Kind:  flowcontrol.SubjectKindGroup,
							Group: &flowcontrol.GroupSubject{Name: "system:masters"},
						}},
						ResourceRules: []flowcontrol.ResourcePolicyRule{{
							Verbs:        all,
							APIGroups:    all,
							Resources:    all,
							ClusterScope: true,
							Namespaces:   all,
						}},
					}},
				},
			},
			// 列： Name, PriorityLevelName, MatchingPrecedence, DistinguisherMethod, Age, MissingPL
			expected: []metav1.TableRow{{Cells: []interface{}{"exempt", "allee", int64(0), "<none>", "5m", "?"}}},
		},
	}

	for i, test := range tests {
		rows, err := printFlowSchema(&test.fs, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintDeviceClass(t *testing.T) {

	tests := []struct {
		deviceClass resourceapis.DeviceClass
		expected    []metav1.TableRow
	}{
		{
			deviceClass: resourceapis.DeviceClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-deviceclass",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
			},
			// 列： Name, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test-deviceclass", "5m"}}},
		},
		{
			// 空创建时间戳转换为 <unknown> 的测试用例
			deviceClass: resourceapis.DeviceClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-deviceclass",
					CreationTimestamp: metav1.Time{},
				},
			},
			// 列： Name, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test-deviceclass", "<unknown>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printDeviceClass(&test.deviceClass, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintResourceClaim(t *testing.T) {

	tests := []struct {
		name          string
		resourceClaim resourceapis.ResourceClaim
		expected      []metav1.TableRow
	}{
		{
			name: "ResourceClaim with Pending State",
			resourceClaim: resourceapis.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-resourceclaim",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: resourceapis.ResourceClaimSpec{
					Devices: resourceapis.DeviceClaim{
						Requests: []resourceapis.DeviceRequest{
							{
								Name: "deviceRequest",
								Exactly: &resourceapis.ExactDeviceRequest{
									DeviceClassName: "deviceClass",
									AllocationMode:  resourceapis.DeviceAllocationModeExactCount,
									Count:           1,
								},
							},
						},
					},
				},
			},
			// 列： Name, State, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test-resourceclaim", "pending", "5m"}}},
		},
		{
			// 空创建时间戳转换为 <unknown> 的测试用例
			name: "ResourceClaim with Pending State and Empty creation timestamp",
			resourceClaim: resourceapis.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-resourceclaim",
					CreationTimestamp: metav1.Time{},
				},
				Spec: resourceapis.ResourceClaimSpec{
					Devices: resourceapis.DeviceClaim{
						Requests: []resourceapis.DeviceRequest{
							{
								Name: "deviceRequest",
								Exactly: &resourceapis.ExactDeviceRequest{
									DeviceClassName: "deviceClass",
									AllocationMode:  resourceapis.DeviceAllocationModeExactCount,
									Count:           1,
								},
							},
						},
					},
				},
			},
			// 列： Name, State, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test-resourceclaim", "pending", "<unknown>"}}},
		},
		{
			name: "ResourceClaim with Allocated and Reserved State",
			resourceClaim: resourceapis.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-resourceclaim",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: resourceapis.ResourceClaimSpec{
					Devices: resourceapis.DeviceClaim{
						Requests: []resourceapis.DeviceRequest{
							{
								Name: "deviceRequest",
								Exactly: &resourceapis.ExactDeviceRequest{
									DeviceClassName: "deviceClass",
									AllocationMode:  resourceapis.DeviceAllocationModeExactCount,
									Count:           1,
								},
							},
						},
					},
				},
				Status: resourceapis.ResourceClaimStatus{
					Allocation: &resourceapis.AllocationResult{},
					ReservedFor: []resourceapis.ResourceClaimConsumerReference{
						{
							Resource: "pods",
							Name:     "pod-test",
							UID:      types.UID("pod-test"),
						},
					},
				},
			},
			// 列： Name, State, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test-resourceclaim", "allocated,reserved", "5m"}}},
		},
		{
			name: "ResourceClaim with Deleted State",
			resourceClaim: resourceapis.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-resourceclaim",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
				Spec: resourceapis.ResourceClaimSpec{
					Devices: resourceapis.DeviceClaim{
						Requests: []resourceapis.DeviceRequest{
							{
								Name: "deviceRequest",
								Exactly: &resourceapis.ExactDeviceRequest{
									DeviceClassName: "deviceClass",
									AllocationMode:  resourceapis.DeviceAllocationModeExactCount,
									Count:           1,
								},
							},
						},
					},
				},
			},
			// 列： Name, State, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test-resourceclaim", "deleted", "5m"}}},
		},
	}

	for i, test := range tests {
		rows, err := printResourceClaim(&test.resourceClaim, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintResourceClaimTemplate(t *testing.T) {

	tests := []struct {
		resourceClaimTemplate resourceapis.ResourceClaimTemplate
		expected              []metav1.TableRow
	}{
		{
			resourceClaimTemplate: resourceapis.ResourceClaimTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-resourceclaimtemplate",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: resourceapis.ResourceClaimTemplateSpec{
					Spec: resourceapis.ResourceClaimSpec{
						Devices: resourceapis.DeviceClaim{
							Requests: []resourceapis.DeviceRequest{{
								Name: "test-deviceRequest",
								Exactly: &resourceapis.ExactDeviceRequest{
									DeviceClassName: "deviceClassName",
									AllocationMode:  resourceapis.DeviceAllocationModeExactCount,
									Count:           1,
								},
							}},
						},
					},
				},
			},
			// 列： Name, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test-resourceclaimtemplate", "5m"}}},
		},
		{
			// 空创建时间戳转换为 <unknown> 的测试用例
			resourceClaimTemplate: resourceapis.ResourceClaimTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-resourceclaimtemplate",
					CreationTimestamp: metav1.Time{},
				},
				Spec: resourceapis.ResourceClaimTemplateSpec{
					Spec: resourceapis.ResourceClaimSpec{
						Devices: resourceapis.DeviceClaim{
							Requests: []resourceapis.DeviceRequest{{
								Name: "test-deviceRequest",
								Exactly: &resourceapis.ExactDeviceRequest{
									DeviceClassName: "deviceClassName",
									AllocationMode:  resourceapis.DeviceAllocationModeExactCount,
									Count:           1,
								},
							}},
						},
					},
				},
			},
			// 列： Name, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test-resourceclaimtemplate", "<unknown>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printResourceClaimTemplate(&test.resourceClaimTemplate, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintResourceSlice(t *testing.T) {

	tests := []struct {
		resourceSlice resourceapis.ResourceSlice
		expected      []metav1.TableRow
	}{
		{
			resourceSlice: resourceapis.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-resourceslice",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: resourceapis.ResourceSliceSpec{
					NodeName: ptr.To("nodeName"),
					Driver:   "driverName",
					Pool: resourceapis.ResourcePool{
						Name:               "poolName",
						ResourceSliceCount: 1,
					},
				},
			},
			// 列： Name, Node, Driver, Pool, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test-resourceslice", "nodeName", "driverName", "poolName", "5m"}}},
		},
		{
			resourceSlice: resourceapis.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-resourceslice",
					CreationTimestamp: metav1.Time{},
				},
				Spec: resourceapis.ResourceSliceSpec{
					NodeName: ptr.To("nodeName"),
					Driver:   "driverName",
					Pool: resourceapis.ResourcePool{
						Name:               "poolName",
						ResourceSliceCount: 1,
					},
				},
			},
			// 列： Name, Node, Driver, Pool, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test-resourceslice", "nodeName", "driverName", "poolName", "<unknown>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printResourceSlice(&test.resourceSlice, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintPriorityLevelConfiguration(t *testing.T) {
	tests := []struct {
		pl       flowcontrol.PriorityLevelConfiguration
		expected []metav1.TableRow
	}{
		{
			pl: flowcontrol.PriorityLevelConfiguration{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "unlimited",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: flowcontrol.PriorityLevelConfigurationSpec{
					Type: flowcontrol.PriorityLevelEnablementExempt,
				},
			},
			// 列： Name, Type, NominalConcurrencyShares, Queues, HandSize, QueueLengthLimit, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"unlimited", "Exempt", "<none>", "<none>", "<none>", "<none>", "0s"}}},
		},
		{
			pl: flowcontrol.PriorityLevelConfiguration{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "unqueued",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: flowcontrol.PriorityLevelConfigurationSpec{
					Type: flowcontrol.PriorityLevelEnablementLimited,
					Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
						NominalConcurrencyShares: 47,
						LimitResponse: flowcontrol.LimitResponse{
							Type: flowcontrol.LimitResponseTypeReject,
						},
					},
				},
			},
			// 列： Name, Type, NominalConcurrencyShares, Queues, HandSize, QueueLengthLimit, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"unqueued", "Limited", int32(47), "<none>", "<none>", "<none>", "0s"}}},
		},
		{
			pl: flowcontrol.PriorityLevelConfiguration{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "queued",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: flowcontrol.PriorityLevelConfigurationSpec{
					Type: flowcontrol.PriorityLevelEnablementLimited,
					Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
						NominalConcurrencyShares: 42,
						LimitResponse: flowcontrol.LimitResponse{
							Type: flowcontrol.LimitResponseTypeQueue,
							Queuing: &flowcontrol.QueuingConfiguration{
								Queues:           8,
								HandSize:         3,
								QueueLengthLimit: 4,
							},
						},
					},
				},
			},
			// 列： Name, Type, NominalConcurrencyShares, Queues, HandSize, QueueLengthLimit, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"queued", "Limited", int32(42), int32(8), int32(3), int32(4), "0s"}}},
		},
	}

	for i, test := range tests {
		rows, err := printPriorityLevelConfiguration(&test.pl, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintStorageVersion(t *testing.T) {
	commonEncodingVersion := "v1"
	tests := []struct {
		sv       apiserverinternal.StorageVersion
		expected []metav1.TableRow
	}{
		{
			sv: apiserverinternal.StorageVersion{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "empty",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Status: apiserverinternal.StorageVersionStatus{},
			},
			// 列： Name, CommonEncodingVersion, StorageVersions, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"empty", "<unset>", "<unset>", "0s"}}},
		},
		{
			sv: apiserverinternal.StorageVersion{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "valid",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Status: apiserverinternal.StorageVersionStatus{
					StorageVersions: []apiserverinternal.ServerStorageVersion{
						{
							APIServerID:       "1",
							EncodingVersion:   "v1",
							DecodableVersions: []string{"v1"},
						},
						{
							APIServerID:       "2",
							EncodingVersion:   "v1",
							DecodableVersions: []string{"v1", "v2"},
						},
					},
					CommonEncodingVersion: &commonEncodingVersion,
				},
			},
			// 列： Name, CommonEncodingVersion, StorageVersions, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"valid", "v1", "1=v1,2=v1", "0s"}}},
		},
		{
			sv: apiserverinternal.StorageVersion{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "disagree",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Status: apiserverinternal.StorageVersionStatus{
					StorageVersions: []apiserverinternal.ServerStorageVersion{
						{
							APIServerID:       "1",
							EncodingVersion:   "v1",
							DecodableVersions: []string{"v1"},
						},
						{
							APIServerID:       "2",
							EncodingVersion:   "v1",
							DecodableVersions: []string{"v1", "v2"},
						},
						{
							APIServerID:       "3",
							EncodingVersion:   "v2",
							DecodableVersions: []string{"v2"},
						},
					},
				},
			},
			// 列： Name, CommonEncodingVersion, StorageVersions, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"disagree", "<unset>", "1=v1,2=v1,3=v2", "0s"}}},
		},
		{
			sv: apiserverinternal.StorageVersion{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "agreeWithMore",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Status: apiserverinternal.StorageVersionStatus{
					StorageVersions: []apiserverinternal.ServerStorageVersion{
						{
							APIServerID:       "1",
							EncodingVersion:   "v1",
							DecodableVersions: []string{"v1"},
						},
						{
							APIServerID:       "2",
							EncodingVersion:   "v1",
							DecodableVersions: []string{"v1", "v2"},
						},
						{
							APIServerID:       "3",
							EncodingVersion:   "v1",
							DecodableVersions: []string{"v1", "v2"},
						},
						{
							APIServerID:       "4",
							EncodingVersion:   "v1",
							DecodableVersions: []string{"v1", "v2", "v3alpha1"},
						},
					},
					CommonEncodingVersion: &commonEncodingVersion,
				},
			},
			// 列： Name, CommonEncodingVersion, StorageVersions, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"agreeWithMore", "v1", "1=v1,2=v1,3=v1 + 1 more...", "0s"}}},
		},
	}

	for i, test := range tests {
		rows, err := printStorageVersion(&test.sv, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintScale(t *testing.T) {
	tests := []struct {
		scale    autoscaling.Scale
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		{
			scale: autoscaling.Scale{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-autoscaling",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec:   autoscaling.ScaleSpec{Replicas: 2},
				Status: autoscaling.ScaleStatus{Replicas: 1},
			},
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"test-autoscaling", int64(2), int64(1), string("0s")},
				},
			},
		},
	}

	for i, test := range tests {
		rows, err := printScale(&test.scale, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintClusterTrustBundle(t *testing.T) {
	tests := []struct {
		bundle   certificates.ClusterTrustBundle
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		{
			bundle: certificates.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster-trust-bundle",
				},
				Spec: certificates.ClusterTrustBundleSpec{
					SignerName: "test-signer-name",
				},
			},
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"test-cluster-trust-bundle", "test-signer-name"},
				},
			},
		},
	}

	for i, test := range tests {
		rows, err := printClusterTrustBundle(&test.bundle, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintPodCertificateRequest(t *testing.T) {
	tests := []struct {
		bundle   certificates.PodCertificateRequest
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		{
			bundle: certificates.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: certificates.PodCertificateRequestSpec{
					SignerName:                "foo.com/abc",
					PodName:                   "pod-1",
					ServiceAccountName:        "sa-1",
					NodeName:                  types.NodeName("node-1"),
					UnverifiedUserAnnotations: map[string]string{"test/domain": "bar", "test/foo": "abc"},
				},
				Status: certificates.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type: certificates.PodCertificateRequestConditionTypeIssued,
						},
					}},
			},
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"bar", "pod-1", "sa-1", "node-1", "foo.com/abc", "Issued", "test/domain=bar,test/foo=abc"},
				},
			},
			options: printers.GenerateOptions{Wide: true},
		},
		{
			bundle: certificates.PodCertificateRequest{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "foo",
					Name:      "bar",
				},
				Spec: certificates.PodCertificateRequestSpec{
					SignerName:                "foo.com/abc",
					PodName:                   "pod-1",
					ServiceAccountName:        "sa-1",
					NodeName:                  types.NodeName("node-1"),
					UnverifiedUserAnnotations: map[string]string{"test/domain": "bar", "test/foo": "abc"},
				},
				Status: certificates.PodCertificateRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type: certificates.PodCertificateRequestConditionTypeIssued,
						},
					}},
			},
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"bar", "pod-1", "sa-1", "node-1", "foo.com/abc", "Issued"},
				},
			},
		},
	}

	for i, test := range tests {
		rows, err := printPodCertificateRequest(&test.bundle, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintValidatingAdmissionPolicyBinding(t *testing.T) {
	tests := []struct {
		validatingAdmissionPolicyBinding admissionregistration.ValidatingAdmissionPolicyBinding
		options                          printers.GenerateOptions
		expected                         []metav1.TableRow
	}{
		{
			validatingAdmissionPolicyBinding: admissionregistration.ValidatingAdmissionPolicyBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "config",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: admissionregistration.ValidatingAdmissionPolicyBindingSpec{
					PolicyName: "test-scale.example.com",
					ParamRef: &admissionregistration.ParamRef{
						Name:                    "test-scale-setting.example.com",
						ParameterNotFoundAction: ptr.To(admissionregistration.DenyAction),
					},
				},
			},
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"config", "test-scale.example.com", "*/test-scale-setting.example.com", "5m"},
				},
			},
		},
	}

	for i, test := range tests {
		rows, err := printValidatingAdmissionPolicyBinding(&test.validatingAdmissionPolicyBinding, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintLeaseCandidate(t *testing.T) {
	tests := []struct {
		leaseCandidate coordination.LeaseCandidate
		options        printers.GenerateOptions
		expected       []metav1.TableRow
	}{
		{
			leaseCandidate: coordination.LeaseCandidate{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-lease",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: coordination.LeaseCandidateSpec{
					BinaryVersion:    "test-binary-version",
					EmulationVersion: "test-emulation-version",
					LeaseName:        "test-lease-name",
				},
			},
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"test-lease", "test-lease-name", "test-binary-version", "test-emulation-version", "5m"},
				},
			},
		},
	}

	for i, test := range tests {
		rows, err := printLeaseCandidate(&test.leaseCandidate, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintResourceQuota(t *testing.T) {
	tests := []struct {
		resourceQuota api.ResourceQuota
		options       printers.GenerateOptions
		expected      []metav1.TableRow
	}{
		{
			resourceQuota: api.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-resourcequota",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: api.ResourceQuotaSpec{},
				Status: api.ResourceQuotaStatus{
					Used: api.ResourceList{
						api.ResourceCPU:                    resource.MustParse("1"),
						api.ResourceMemory:                 resource.MustParse("1Gi"),
						api.ResourcePods:                   resource.MustParse("1"),
						api.ResourceServices:               resource.MustParse("1"),
						api.ResourceReplicationControllers: resource.MustParse("1"),
						api.ResourceQuotas:                 resource.MustParse("1"),
						api.ResourceLimitsCPU:              resource.MustParse("2"),
						api.ResourceLimitsMemory:           resource.MustParse("2Gi"),
					},
					Hard: api.ResourceList{
						api.ResourceCPU:                    resource.MustParse("100"),
						api.ResourceMemory:                 resource.MustParse("4Gi"),
						api.ResourcePods:                   resource.MustParse("10"),
						api.ResourceServices:               resource.MustParse("10"),
						api.ResourceReplicationControllers: resource.MustParse("10"),
						api.ResourceQuotas:                 resource.MustParse("1"),
						api.ResourceLimitsCPU:              resource.MustParse("200"),
						api.ResourceLimitsMemory:           resource.MustParse("8Gi"),
					},
				},
			},
			expected: []metav1.TableRow{
				{
					Cells: []interface{}{"test-resourcequota", "cpu: 1/100, memory: 1Gi/4Gi, pods: 1/10, replicationcontrollers: 1/10, resourcequotas: 1/1, services: 1/10", "limits.cpu: 2/200, limits.memory: 2Gi/8Gi", "5m"},
				},
			},
		},
	}

	for i, test := range tests {
		rows, err := printResourceQuota(&test.resourceQuota, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestTableRowDeepCopyShouldNotPanic(t *testing.T) {
	tests := []struct {
		name    string
		printer func() ([]metav1.TableRow, error)
	}{
		{
			name: "Pod",
			printer: func() ([]metav1.TableRow, error) {
				return printPod(&api.Pod{}, printers.GenerateOptions{})
			},
		},
		{
			name: "PodTemplate",
			printer: func() ([]metav1.TableRow, error) {
				return printPodTemplate(&api.PodTemplate{}, printers.GenerateOptions{})
			},
		},
		{
			name: "PodDisruptionBudget",
			printer: func() ([]metav1.TableRow, error) {
				return printPodDisruptionBudget(&policy.PodDisruptionBudget{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ReplicationController",
			printer: func() ([]metav1.TableRow, error) {
				return printReplicationController(&api.ReplicationController{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ReplicaSet",
			printer: func() ([]metav1.TableRow, error) {
				return printReplicaSet(&apps.ReplicaSet{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Job",
			printer: func() ([]metav1.TableRow, error) {
				return printJob(&batch.Job{}, printers.GenerateOptions{})
			},
		},
		{
			name: "CronJob",
			printer: func() ([]metav1.TableRow, error) {
				return printCronJob(&batch.CronJob{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Service",
			printer: func() ([]metav1.TableRow, error) {
				return printService(&api.Service{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Ingress",
			printer: func() ([]metav1.TableRow, error) {
				return printIngress(&networking.Ingress{}, printers.GenerateOptions{})
			},
		},
		{
			name: "IngressClass",
			printer: func() ([]metav1.TableRow, error) {
				return printIngressClass(&networking.IngressClass{}, printers.GenerateOptions{})
			},
		},
		{
			name: "StatefulSet",
			printer: func() ([]metav1.TableRow, error) {
				return printStatefulSet(&apps.StatefulSet{}, printers.GenerateOptions{})
			},
		},
		{
			name: "DaemonSet",
			printer: func() ([]metav1.TableRow, error) {
				return printDaemonSet(&apps.DaemonSet{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Endpoints",
			printer: func() ([]metav1.TableRow, error) {
				return printEndpoints(&api.Endpoints{}, printers.GenerateOptions{})
			},
		},
		{
			name: "EndpointSlice",
			printer: func() ([]metav1.TableRow, error) {
				return printEndpointSlice(&discovery.EndpointSlice{}, printers.GenerateOptions{})
			},
		},
		{
			name: "CSINode",
			printer: func() ([]metav1.TableRow, error) {
				return printCSINode(&storage.CSINode{}, printers.GenerateOptions{})
			},
		},
		{
			name: "CSIDriver",
			printer: func() ([]metav1.TableRow, error) {
				return printCSIDriver(&storage.CSIDriver{}, printers.GenerateOptions{})
			},
		},
		{
			name: "CSIStorageCapacity",
			printer: func() ([]metav1.TableRow, error) {
				return printCSIStorageCapacity(&storage.CSIStorageCapacity{}, printers.GenerateOptions{})
			},
		},
		{
			name: "MutatingWebhookConfiguration",
			printer: func() ([]metav1.TableRow, error) {
				return printMutatingWebhook(&admissionregistration.MutatingWebhookConfiguration{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ValidatingWebhookConfiguration",
			printer: func() ([]metav1.TableRow, error) {
				return printValidatingWebhook(&admissionregistration.ValidatingWebhookConfiguration{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ValidatingAdmissionPolicy",
			printer: func() ([]metav1.TableRow, error) {
				return printValidatingAdmissionPolicy(&admissionregistration.ValidatingAdmissionPolicy{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ValidatingAdmissionPolicyBinding",
			printer: func() ([]metav1.TableRow, error) {
				return printValidatingAdmissionPolicyBinding(&admissionregistration.ValidatingAdmissionPolicyBinding{}, printers.GenerateOptions{})
			},
		},
		{
			name: "MutatingAdmissionPolicy",
			printer: func() ([]metav1.TableRow, error) {
				return printMutatingAdmissionPolicy(&admissionregistration.MutatingAdmissionPolicy{}, printers.GenerateOptions{})
			},
		},
		{
			name: "MutatingAdmissionPolicyBinding",
			printer: func() ([]metav1.TableRow, error) {
				return printMutatingAdmissionPolicyBinding(&admissionregistration.MutatingAdmissionPolicyBinding{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Namespace",
			printer: func() ([]metav1.TableRow, error) {
				return printNamespace(&api.Namespace{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Secret",
			printer: func() ([]metav1.TableRow, error) {
				return printSecret(&api.Secret{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ServiceAccount",
			printer: func() ([]metav1.TableRow, error) {
				return printServiceAccount(&api.ServiceAccount{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Node",
			printer: func() ([]metav1.TableRow, error) {
				return printNode(&api.Node{}, printers.GenerateOptions{})
			},
		},
		{
			name: "PersistentVolume",
			printer: func() ([]metav1.TableRow, error) {
				return printPersistentVolume(&api.PersistentVolume{}, printers.GenerateOptions{})
			},
		},
		{
			name: "PersistentVolumeClaim",
			printer: func() ([]metav1.TableRow, error) {
				return printPersistentVolumeClaim(&api.PersistentVolumeClaim{}, printers.GenerateOptions{})
			},
		},
		{
			name: "PodCertificateRequest",
			printer: func() ([]metav1.TableRow, error) {
				return printPodCertificateRequest(&certificates.PodCertificateRequest{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Event",
			printer: func() ([]metav1.TableRow, error) {
				return printEvent(&api.Event{}, printers.GenerateOptions{})
			},
		},
		{
			name: "RoleBinding",
			printer: func() ([]metav1.TableRow, error) {
				return printRoleBinding(&rbac.RoleBinding{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ClusterRoleBinding",
			printer: func() ([]metav1.TableRow, error) {
				return printClusterRoleBinding(&rbac.ClusterRoleBinding{}, printers.GenerateOptions{})
			},
		},
		{
			name: "CertificateSigningRequest",
			printer: func() ([]metav1.TableRow, error) {
				return printCertificateSigningRequest(&certificates.CertificateSigningRequest{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ComponentStatus",
			printer: func() ([]metav1.TableRow, error) {
				return printComponentStatus(&api.ComponentStatus{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Deployment",
			printer: func() ([]metav1.TableRow, error) {
				return printDeployment(&apps.Deployment{}, printers.GenerateOptions{})
			},
		},
		{
			name: "HorizontalPodAutoscaler",
			printer: func() ([]metav1.TableRow, error) {
				return printHorizontalPodAutoscaler(&autoscaling.HorizontalPodAutoscaler{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ConfigMap",
			printer: func() ([]metav1.TableRow, error) {
				return printConfigMap(&api.ConfigMap{}, printers.GenerateOptions{})
			},
		},
		{
			name: "NetworkPolicy",
			printer: func() ([]metav1.TableRow, error) {
				return printNetworkPolicy(&networking.NetworkPolicy{}, printers.GenerateOptions{})
			},
		},
		{
			name: "StorageClass",
			printer: func() ([]metav1.TableRow, error) {
				return printStorageClass(&storage.StorageClass{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Lease",
			printer: func() ([]metav1.TableRow, error) {
				return printLease(&coordination.Lease{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ControllerRevision",
			printer: func() ([]metav1.TableRow, error) {
				return printControllerRevision(&apps.ControllerRevision{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ResourceQuota",
			printer: func() ([]metav1.TableRow, error) {
				return printResourceQuota(&api.ResourceQuota{}, printers.GenerateOptions{})
			},
		},
		{
			name: "PriorityClass",
			printer: func() ([]metav1.TableRow, error) {
				return printPriorityClass(&scheduling.PriorityClass{}, printers.GenerateOptions{})
			},
		},
		{
			name: "RuntimeClass",
			printer: func() ([]metav1.TableRow, error) {
				return printRuntimeClass(&nodeapi.RuntimeClass{}, printers.GenerateOptions{})
			},
		},
		{
			name: "VolumeAttachment",
			printer: func() ([]metav1.TableRow, error) {
				return printVolumeAttachment(&storage.VolumeAttachment{}, printers.GenerateOptions{})
			},
		},
		{
			name: "FlowSchema",
			printer: func() ([]metav1.TableRow, error) {
				return printFlowSchema(&flowcontrol.FlowSchema{}, printers.GenerateOptions{})
			},
		},
		{
			name: "StorageVersion",
			printer: func() ([]metav1.TableRow, error) {
				return printStorageVersion(&apiserverinternal.StorageVersion{}, printers.GenerateOptions{})
			},
		},
		{
			name: "PriorityLevelConfiguration",
			printer: func() ([]metav1.TableRow, error) {
				return printPriorityLevelConfiguration(&flowcontrol.PriorityLevelConfiguration{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Scale",
			printer: func() ([]metav1.TableRow, error) {
				return printScale(&autoscaling.Scale{}, printers.GenerateOptions{})
			},
		},
		{
			name: "Status",
			printer: func() ([]metav1.TableRow, error) {
				return printStatus(&metav1.Status{}, printers.GenerateOptions{})
			},
		},
		{
			name: "DeviceClass",
			printer: func() ([]metav1.TableRow, error) {
				return printDeviceClass(&resourceapis.DeviceClass{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ResourceClaim",
			printer: func() ([]metav1.TableRow, error) {
				return printResourceClaim(&resourceapis.ResourceClaim{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ResourceClaimTemplate",
			printer: func() ([]metav1.TableRow, error) {
				return printResourceClaimTemplate(&resourceapis.ResourceClaimTemplate{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ResourceSlice",
			printer: func() ([]metav1.TableRow, error) {
				return printResourceSlice(&resourceapis.ResourceSlice{}, printers.GenerateOptions{})
			},
		},
		{
			name: "LeaseCandidate",
			printer: func() ([]metav1.TableRow, error) {
				return printLeaseCandidate(&coordination.LeaseCandidate{}, printers.GenerateOptions{})
			},
		},
		{
			name: "ClusterTrustBundle",
			printer: func() ([]metav1.TableRow, error) {
				return printClusterTrustBundle(&certificates.ClusterTrustBundle{}, printers.GenerateOptions{})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows, err := test.printer()
			if err != nil {
				t.Fatalf("expected no error, but got: %#v", err)
			}
			if len(rows) <= 0 {
				t.Fatalf("expected to have at least one TableRow, but got: %d", len(rows))
			}

			func() {
				defer func() {
					if err := recover(); err != nil {
						// 与标准库 HTTP 服务代码一致。手动分配栈跟踪缓冲区大小，
						// 以防日志过大
						const size = 64 << 10
						buf := make([]byte, size)
						buf = buf[:runtime.Stack(buf, false)]
						err = fmt.Errorf("%q stack:\n%s", err, buf)

						t.Errorf("Expected no panic, but got: %v", err)
					}
				}()

				// 不应发生 panic
				rows[0].DeepCopy()
			}()

		})
	}
}

func TestPrintIPAddress(t *testing.T) {
	ip := networking.IPAddress{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "192.168.2.2",
			CreationTimestamp: metav1.Time{Time: time.Now().Add(time.Duration(-10 * 365 * 24 * time.Hour))},
		},
		Spec: networking.IPAddressSpec{
			ParentRef: &networking.ParentReference{
				Group:     "mygroup",
				Resource:  "myresource",
				Namespace: "mynamespace",
				Name:      "myname",
			},
		},
	}
	// 列： Name, ParentRef, Age
	expected := []metav1.TableRow{{Cells: []interface{}{"192.168.2.2", "myresource.mygroup/mynamespace/myname", "10y"}}}

	rows, err := printIPAddress(&ip, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error generating table rows for IPAddress: %#v", err)
	}
	rows[0].Object.Object = nil
	if !reflect.DeepEqual(expected, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expected, rows))
	}
}

func TestPrintIPAddressList(t *testing.T) {
	ipList := networking.IPAddressList{
		Items: []networking.IPAddress{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "192.168.2.2",
					CreationTimestamp: metav1.Time{},
				},
				Spec: networking.IPAddressSpec{
					ParentRef: &networking.ParentReference{
						Group:     "mygroup",
						Resource:  "myresource",
						Namespace: "mynamespace",
						Name:      "myname",
					},
				},
			}, {
				ObjectMeta: metav1.ObjectMeta{
					Name:              "2001:db8::2",
					CreationTimestamp: metav1.Time{},
				},
				Spec: networking.IPAddressSpec{
					ParentRef: &networking.ParentReference{
						Group:     "mygroup2",
						Resource:  "myresource2",
						Namespace: "mynamespace2",
						Name:      "myname2",
					},
				},
			},
		},
	}
	// 列： Name, ParentRef, Age
	expected := []metav1.TableRow{
		{Cells: []interface{}{"192.168.2.2", "myresource.mygroup/mynamespace/myname", "<unknown>"}},
		{Cells: []interface{}{"2001:db8::2", "myresource2.mygroup2/mynamespace2/myname2", "<unknown>"}},
	}

	rows, err := printIPAddressList(&ipList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error generating table rows for IPAddress: %#v", err)
	}
	for i := range rows {
		rows[i].Object.Object = nil

	}
	if !reflect.DeepEqual(expected, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expected, rows))
	}

}

func TestPrintServiceCIDR(t *testing.T) {
	ipv4CIDR := "10.1.0.0/16"
	ipv6CIDR := "fd00:1:1::/64"

	tests := []struct {
		ccc      networking.ServiceCIDR
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		{
			// 测试名称，仅 IPv4。
			ccc: networking.ServiceCIDR{
				ObjectMeta: metav1.ObjectMeta{Name: "test1"},
				Spec: networking.ServiceCIDRSpec{
					CIDRs: []string{ipv4CIDR},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, IPv4, IPv6, Age.
			expected: []metav1.TableRow{{Cells: []interface{}{"test1", ipv4CIDR, "<unknown>"}}},
		},
		{
			// 测试名称，仅 IPv6。
			ccc: networking.ServiceCIDR{
				ObjectMeta: metav1.ObjectMeta{Name: "test5"},
				Spec: networking.ServiceCIDRSpec{
					CIDRs: []string{ipv6CIDR},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, PerNodeHostBits, IPv4, IPv6, Age
			expected: []metav1.TableRow{{Cells: []interface{}{"test5", ipv6CIDR, "<unknown>"}}},
		},
		{
			// 测试名称，DualStack。
			ccc: networking.ServiceCIDR{
				ObjectMeta: metav1.ObjectMeta{Name: "test9"},
				Spec: networking.ServiceCIDRSpec{
					CIDRs: []string{ipv4CIDR, ipv6CIDR},
				},
			},
			options: printers.GenerateOptions{},
			// 列： Name, PerNodeHostBits, IPv4, IPv6, Age.
			expected: []metav1.TableRow{{Cells: []interface{}{"test9", ipv4CIDR + "," + ipv6CIDR, "<unknown>"}}},
		},
	}

	for i, test := range tests {
		rows, err := printServiceCIDR(&test.ccc, test.options)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintServiceCIDRList(t *testing.T) {
	cccList := networking.ServiceCIDRList{
		Items: []networking.ServiceCIDR{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "ccc1"},
				Spec: networking.ServiceCIDRSpec{
					CIDRs: []string{"10.1.0.0/16", "fd00:1:1::/64"},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "ccc2"},
				Spec: networking.ServiceCIDRSpec{
					CIDRs: []string{"10.2.0.0/16", "fd00:2:1::/64"},
				},
			},
		},
	}

	tests := []struct {
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		{
			// 测试名称，带节点选择器的 DualStack，Wide。
			options: printers.GenerateOptions{Wide: false},
			expected: []metav1.TableRow{
				// 列： Name, IPv4, IPv6, Age.
				{Cells: []interface{}{"ccc1", "10.1.0.0/16,fd00:1:1::/64", "<unknown>"}},
				{Cells: []interface{}{"ccc2", "10.2.0.0/16,fd00:2:1::/64", "<unknown>"}},
			},
		},
		{
			// 测试名称，带节点选择器的 DualStack，Wide。
			options: printers.GenerateOptions{Wide: true},
			expected: []metav1.TableRow{
				// 列： Name, CIDRs, Age.
				{Cells: []interface{}{"ccc1", "10.1.0.0/16,fd00:1:1::/64", "<unknown>"}},
				{Cells: []interface{}{"ccc2", "10.2.0.0/16,fd00:2:1::/64", "<unknown>"}},
			},
		},
	}

	for _, test := range tests {
		rows, err := printServiceCIDRList(&cccList, test.options)
		if err != nil {
			t.Fatalf("Error printing service list: %#v", err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("mismatch: %s", cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintStorageVersionMigration(t *testing.T) {
	storageVersionMigration := storagemigration.StorageVersionMigration{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StorageVersionMigration",
			APIVersion: "storagemigration.k8s.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "print-test",
		},
		Spec: storagemigration.StorageVersionMigrationSpec{
			Resource: metav1.GroupResource{
				Group:    "test-group",
				Resource: "test-resource",
			},
		},
		Status: storagemigration.StorageVersionMigrationStatus{
			Conditions: []metav1.Condition{
				{
					Type:   string(storagemigration.MigrationRunning),
					Status: metav1.ConditionFalse,
				},
				{
					Type:   string(storagemigration.MigrationSucceeded),
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	// 列： Name, Resource, Status
	expected := []metav1.TableRow{{Cells: []interface{}{"print-test", "test-resource.test-group", "Succeeded"}}}

	rows, err := printStorageVersionMigration(&storageVersionMigration, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error generating table rows for StorageVersionMigration: %#v", err)
	}
	rows[0].Object.Object = nil
	if !reflect.DeepEqual(expected, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expected, rows))
	}
}

func TestPrintStorageVersionMigrationList(t *testing.T) {
	storageVersionMigrationList := storagemigration.StorageVersionMigrationList{
		Items: []storagemigration.StorageVersionMigration{
			{
				TypeMeta: metav1.TypeMeta{
					Kind:       "StorageVersionMigration",
					APIVersion: "storagemigration.k8s.io/v1alpha1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "print-test",
				},
				Spec: storagemigration.StorageVersionMigrationSpec{
					Resource: metav1.GroupResource{
						Group:    "",
						Resource: "test-resource",
					},
				},
				Status: storagemigration.StorageVersionMigrationStatus{
					Conditions: []metav1.Condition{
						{
							Type:   string(storagemigration.MigrationRunning),
							Status: metav1.ConditionFalse,
						},
						{
							Type:   string(storagemigration.MigrationSucceeded),
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
			{
				TypeMeta: metav1.TypeMeta{
					Kind:       "StorageVersionMigration",
					APIVersion: "storagemigration.k8s.io/v1alpha1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "print-test2",
				},
				Spec: storagemigration.StorageVersionMigrationSpec{
					Resource: metav1.GroupResource{
						Group:    "test-group2",
						Resource: "test-resource2",
					},
				},
				Status: storagemigration.StorageVersionMigrationStatus{
					Conditions: []metav1.Condition{
						{
							Type:   string(storagemigration.MigrationRunning),
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
		},
	}

	// 列： Name, Resource, Status
	expected := []metav1.TableRow{
		{Cells: []interface{}{"print-test", "test-resource", "Succeeded"}},
		{Cells: []interface{}{"print-test2", "test-resource2.test-group2", "Running"}},
	}

	rows, err := printStorageVersionMigrationList(&storageVersionMigrationList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error generating table rows for StorageVersionMigration: %#v", err)
	}

	for i := range rows {
		rows[i].Object.Object = nil
	}

	if !reflect.DeepEqual(expected, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expected, rows))
	}
}

func TestPrintWorkload(t *testing.T) {
	workload := &scheduling.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workload1",
			Namespace: "ns1",
		},
		Spec: scheduling.WorkloadSpec{
			PodGroupTemplates: []scheduling.PodGroupTemplate{
				{
					Name: "foo",
					SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
						Gang: &scheduling.GangSchedulingPolicy{
							MinCount: 5,
						},
					},
				},
			},
		},
	}

	// 列： Name, Age
	expected := []metav1.TableRow{{Cells: []interface{}{"workload1", "<unknown>"}}}

	rows, err := printWorkload(workload, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error generating table rows for Workload: %#v", err)
	}
	rows[0].Object.Object = nil
	if !reflect.DeepEqual(expected, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expected, rows))
	}
}

func TestPrintWorkloadList(t *testing.T) {
	workloadList := &scheduling.WorkloadList{
		Items: []scheduling.Workload{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "workload1",
					Namespace: "ns1",
				},
				Spec: scheduling.WorkloadSpec{
					PodGroupTemplates: []scheduling.PodGroupTemplate{
						{
							Name: "foo",
							SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
								Gang: &scheduling.GangSchedulingPolicy{
									MinCount: 5,
								},
							},
						},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "workload2",
					Namespace:         "ns1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: scheduling.WorkloadSpec{
					PodGroupTemplates: []scheduling.PodGroupTemplate{
						{
							Name: "bar",
							SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
								Gang: &scheduling.GangSchedulingPolicy{
									MinCount: 5,
								},
							},
						},
					},
				},
			},
		},
	}

	// 列： Name, Age
	expected := []metav1.TableRow{
		{Cells: []interface{}{"workload1", "<unknown>"}},
		{Cells: []interface{}{"workload2", "0s"}},
	}

	rows, err := printWorkloadList(workloadList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error generating table rows for WorkloadList: %#v", err)
	}

	for i := range rows {
		rows[i].Object.Object = nil
	}

	if !reflect.DeepEqual(expected, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expected, rows))
	}
}

func TestPrintPodGroup(t *testing.T) {
	podGroup := &scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "podgroup1",
			Namespace: "ns1",
		},
		Spec: scheduling.PodGroupSpec{
			WorkloadRef: &scheduling.WorkloadReference{
				WorkloadName: "w",
				TemplateName: "t1",
			},
			SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
				Gang: &scheduling.GangSchedulingPolicy{
					MinCount: 5,
				},
			},
		},
	}

	// 列： Name, Policy, Workload, Status, Age
	expected := []metav1.TableRow{{Cells: []interface{}{"podgroup1", "Gang", "w", "Pending", "<unknown>"}}}

	rows, err := printPodGroup(podGroup, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error generating table rows for PodGroup: %#v", err)
	}
	rows[0].Object.Object = nil
	if !reflect.DeepEqual(expected, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expected, rows))
	}
}

func TestPrintPodGroupList(t *testing.T) {
	podGroupList := &scheduling.PodGroupList{
		Items: []scheduling.PodGroup{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "podgroup1",
					Namespace: "ns1",
				},
				Spec: scheduling.PodGroupSpec{
					WorkloadRef: &scheduling.WorkloadReference{
						WorkloadName: "w1",
						TemplateName: "t1",
					},
					SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
						Gang: &scheduling.GangSchedulingPolicy{
							MinCount: 5,
						},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "podgroup2",
					Namespace:         "ns1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: scheduling.PodGroupSpec{
					WorkloadRef: &scheduling.WorkloadReference{
						WorkloadName: "w1",
						TemplateName: "t2",
					},
					SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
						Gang: &scheduling.GangSchedulingPolicy{
							MinCount: 5,
						},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "podgroup3",
					Namespace:         "ns1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: scheduling.PodGroupSpec{
					WorkloadRef: &scheduling.WorkloadReference{
						WorkloadName: "w2",
						TemplateName: "t1",
					},
					SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
						Basic: &scheduling.BasicSchedulingPolicy{},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "podgroup4",
					Namespace:         "ns1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: scheduling.PodGroupSpec{
					SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
						Basic: &scheduling.BasicSchedulingPolicy{},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "podgroup5",
					Namespace:         "ns1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: scheduling.PodGroupSpec{
					SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
						Basic: &scheduling.BasicSchedulingPolicy{},
					},
				},
				Status: scheduling.PodGroupStatus{
					Conditions: []metav1.Condition{
						{
							Type:   scheduling.PodGroupInitiallyScheduled,
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "podgroup6",
					Namespace:         "ns1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: scheduling.PodGroupSpec{
					SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
						Basic: &scheduling.BasicSchedulingPolicy{},
					},
				},
				Status: scheduling.PodGroupStatus{
					Conditions: []metav1.Condition{
						{
							Type:   scheduling.PodGroupInitiallyScheduled,
							Status: metav1.ConditionFalse,
							Reason: scheduling.PodGroupReasonUnschedulable,
						},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "podgroup7",
					Namespace:         "ns1",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1.9e9)},
				},
				Spec: scheduling.PodGroupSpec{
					SchedulingPolicy: scheduling.PodGroupSchedulingPolicy{
						Basic: &scheduling.BasicSchedulingPolicy{},
					},
				},
				Status: scheduling.PodGroupStatus{
					Conditions: []metav1.Condition{
						{
							Type:   scheduling.PodGroupInitiallyScheduled,
							Status: metav1.ConditionTrue,
						},
						{
							Type:   scheduling.DisruptionTarget,
							Status: metav1.ConditionTrue,
							Reason: scheduling.PodGroupReasonPreemptionByScheduler,
						},
					},
				},
			},
		},
	}

	// 列： Name, Policy, Workload, Status, Age
	expected := []metav1.TableRow{
		{Cells: []interface{}{"podgroup1", "Gang", "w1", "Pending", "<unknown>"}},
		{Cells: []interface{}{"podgroup2", "Gang", "w1", "Pending", "0s"}},
		{Cells: []interface{}{"podgroup3", "Basic", "w2", "Pending", "0s"}},
		{Cells: []interface{}{"podgroup4", "Basic", "<none>", "Pending", "0s"}},
		{Cells: []interface{}{"podgroup5", "Basic", "<none>", "Scheduled", "0s"}},
		{Cells: []interface{}{"podgroup6", "Basic", "<none>", "Unschedulable", "0s"}},
		{Cells: []interface{}{"podgroup7", "Basic", "<none>", "PreemptionByScheduler", "0s"}},
	}

	rows, err := printPodGroupList(podGroupList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error generating table rows for PodGroupList: %#v", err)
	}

	for i := range rows {
		rows[i].Object.Object = nil
	}

	if !reflect.DeepEqual(expected, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expected, rows))
	}
}

func TestPrintResourcePoolStatusRequest(t *testing.T) {
	tests := []struct {
		name     string
		request  resourceapis.ResourcePoolStatusRequest
		expected []metav1.TableRow
	}{
		{
			name: "ResourcePoolStatusRequest with Pending status",
			request: resourceapis.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-request",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: resourceapis.ResourcePoolStatusRequestSpec{
					Driver: "driver.example.com",
				},
			},
			// 列： Name, Driver, Total, Available, Allocated, Unavailable, Errors, Pools, Status, Completed
			expected: []metav1.TableRow{{Cells: []interface{}{"test-request", "driver.example.com", "-", "-", "-", "-", "-", int32(0), "Pending", "<none>"}}},
		},
		{
			name: "ResourcePoolStatusRequest with Complete status",
			request: resourceapis.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-request",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: resourceapis.ResourcePoolStatusRequestSpec{
					Driver: "driver.example.com",
				},
				Status: &resourceapis.ResourcePoolStatusRequestStatus{
					PoolCount: ptr.To[int32](3),
					Pools: []resourceapis.PoolStatus{
						{
							Driver:             "driver.example.com",
							PoolName:           "pool-1",
							Generation:         1,
							TotalDevices:       ptr.To[int32](10),
							AvailableDevices:   ptr.To[int32](5),
							AllocatedDevices:   ptr.To[int32](3),
							UnavailableDevices: ptr.To[int32](2),
						},
						{
							Driver:             "driver.example.com",
							PoolName:           "pool-2",
							Generation:         1,
							TotalDevices:       ptr.To[int32](8),
							AvailableDevices:   ptr.To[int32](4),
							AllocatedDevices:   ptr.To[int32](2),
							UnavailableDevices: ptr.To[int32](2),
						},
						{
							Driver:             "driver.example.com",
							PoolName:           "pool-3",
							Generation:         1,
							TotalDevices:       ptr.To[int32](6),
							AvailableDevices:   ptr.To[int32](3),
							AllocatedDevices:   ptr.To[int32](1),
							UnavailableDevices: ptr.To[int32](2),
						},
					},
					Conditions: []metav1.Condition{
						{
							Type:               resourceapis.ResourcePoolStatusRequestConditionComplete,
							Status:             metav1.ConditionTrue,
							LastTransitionTime: metav1.Time{Time: time.Now().Add(-3e11)},
						},
					},
				},
			},
			// 列： Name, Driver, Total, Available, Allocated, Unavailable, Errors, Pools, Status, Completed
			expected: []metav1.TableRow{{Cells: []interface{}{"test-request", "driver.example.com", "24", "12", "6", "6", "0", int32(3), "Complete", "5m"}}},
		},
		{
			name: "ResourcePoolStatusRequest with Complete status and truncated pools",
			request: resourceapis.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-request",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: resourceapis.ResourcePoolStatusRequestSpec{
					Driver: "driver.example.com",
				},
				Status: &resourceapis.ResourcePoolStatusRequestStatus{
					PoolCount: ptr.To[int32](5),
					Pools: []resourceapis.PoolStatus{
						{
							Driver:             "driver.example.com",
							PoolName:           "pool-1",
							Generation:         1,
							TotalDevices:       ptr.To[int32](10),
							AvailableDevices:   ptr.To[int32](5),
							AllocatedDevices:   ptr.To[int32](3),
							UnavailableDevices: ptr.To[int32](2),
						},
						{
							Driver:             "driver.example.com",
							PoolName:           "pool-2",
							Generation:         1,
							TotalDevices:       ptr.To[int32](8),
							AvailableDevices:   ptr.To[int32](4),
							AllocatedDevices:   ptr.To[int32](2),
							UnavailableDevices: ptr.To[int32](2),
						},
					},
					Conditions: []metav1.Condition{
						{
							Type:               resourceapis.ResourcePoolStatusRequestConditionComplete,
							Status:             metav1.ConditionTrue,
							LastTransitionTime: metav1.Time{Time: time.Now().Add(-3e11)},
						},
					},
				},
			},
			// 列： Name, Driver, Total, Available, Allocated, Unavailable, Errors, Pools, Status, Completed
			// 状态显示截断：由于 len(Pools)=2 < PoolCount=5，显示 "Complete (2/5 pools)"
			expected: []metav1.TableRow{{Cells: []interface{}{"test-request", "driver.example.com", "18", "9", "5", "4", "0", int32(5), "Complete (2/5 pools)", "5m"}}},
		},
		{
			name: "ResourcePoolStatusRequest with Failed status",
			request: resourceapis.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-request",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: resourceapis.ResourcePoolStatusRequestSpec{
					Driver: "driver.example.com",
				},
				Status: &resourceapis.ResourcePoolStatusRequestStatus{
					Conditions: []metav1.Condition{
						{
							Type:   resourceapis.ResourcePoolStatusRequestConditionFailed,
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
			// 列： Name, Driver, Total, Available, Allocated, Unavailable, Errors, Pools, Status, Completed
			expected: []metav1.TableRow{{Cells: []interface{}{"test-request", "driver.example.com", "-", "-", "-", "-", "-", int32(0), "Failed", "<none>"}}},
		},
		{
			name: "Complete with validation error pool",
			request: resourceapis.ResourcePoolStatusRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-request",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(-3e11)},
				},
				Spec: resourceapis.ResourcePoolStatusRequestSpec{
					Driver: "driver.example.com",
				},
				Status: &resourceapis.ResourcePoolStatusRequestStatus{
					PoolCount: ptr.To[int32](2),
					Pools: []resourceapis.PoolStatus{
						{
							Driver:             "driver.example.com",
							PoolName:           "pool-healthy",
							Generation:         1,
							TotalDevices:       ptr.To[int32](10),
							AvailableDevices:   ptr.To[int32](5),
							AllocatedDevices:   ptr.To[int32](3),
							UnavailableDevices: ptr.To[int32](2),
						},
						{
							Driver:          "driver.example.com",
							PoolName:        "pool-error",
							Generation:      2,
							ValidationError: ptr.To("pool data could not be validated"),
						},
					},
					Conditions: []metav1.Condition{
						{
							Type:               resourceapis.ResourcePoolStatusRequestConditionComplete,
							Status:             metav1.ConditionTrue,
							LastTransitionTime: metav1.Time{Time: time.Now().Add(-3e11)},
						},
					},
				},
			},
			// 列： Name, Driver, Total, Available, Allocated, Unavailable, Errors, Pools, Status, Completed
			// 仅汇总健康池的设备计数；错误池的计数为 nil。
			expected: []metav1.TableRow{{Cells: []interface{}{"test-request", "driver.example.com", "10", "5", "3", "2", "1", int32(2), "Complete", "5m"}}},
		},
	}

	for i, test := range tests {
		rows, err := printResourcePoolStatusRequest(&test.request, printers.GenerateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			rows[i].Object.Object = nil
		}
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("%d mismatch: %s", i, cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintEvictionRequest(t *testing.T) {
	now := time.Now().UTC().AddDate(0, 0, -3)
	twoDaysAgo := now.AddDate(0, 0, -2)

	eviction := lifecycle.EvictionRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-1-pod1",
			Namespace:         "ns1",
			CreationTimestamp: metav1.Time{Time: twoDaysAgo},
		},
		Spec: lifecycle.EvictionRequestSpec{
			Target: lifecycle.EvictionRequestTarget{
				Pod: &lifecycle.EvictionRequestPodReference{
					Name: "pod1",
					UID:  "3d7fdff1-3fe5-48b9-b106-1ee24b0277f6",
				},
			},
			Requester: "drain.foo.com/bar",
			Intent:    lifecycle.EvictionRequestIntentEviction,
		},
		Status: lifecycle.EvictionRequestStatus{
			ObservedGeneration: new(int64(1)),
			Conditions:         []metav1.Condition{},
		},
	}

	tests := []struct {
		expected []metav1.TableRow
	}{
		{
			expected: []metav1.TableRow{
				// 列： Name, Target, Target Type, Status, Requester, Intent, Age
				{Cells: []interface{}{"pod-1-pod1", "pod1", "Pod", "Progressing", "drain.foo.com/bar", "Eviction", "5d"}},
			},
		},
	}

	for _, test := range tests {
		rows, err := printEvictionRequest(&eviction, printers.GenerateOptions{})
		if err != nil {
			t.Fatalf("Error generating table rows for EvictionRequest: %#v", err)
		}
		rows[0].Object.Object = nil
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("mismatch: %s", cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintEvictionRequestList(t *testing.T) {
	now := time.Now().UTC().AddDate(0, 0, -3)
	dayAgo := now.AddDate(0, 0, -1)
	twoDaysAgo := now.AddDate(0, 0, -2)
	evictionRequestList := lifecycle.EvictionRequestList{
		Items: []lifecycle.EvictionRequest{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod1",
					Namespace:         "ns1",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionRequestSpec{
					Target: lifecycle.EvictionRequestTarget{
						Pod: &lifecycle.EvictionRequestPodReference{
							Name: "pod1",
							UID:  "bc134542-aa19-4361-8fe2-cf18f78848c0",
						},
					},
					Requester: "drain.foo.com/bar",
					Intent:    lifecycle.EvictionRequestIntentEviction,
				},
				Status: lifecycle.EvictionRequestStatus{
					ObservedGeneration: new(int64(1)),
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionTargetEvicted), Status: metav1.ConditionTrue, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: dayAgo}, Reason: string(lifecycle.EvictionConditionReasonPodDeleted), Message: "Pod was successfully deleted."},
					},
				},
			},

			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod2",
					Namespace:         "ns2",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionRequestSpec{
					Target: lifecycle.EvictionRequestTarget{
						Pod: &lifecycle.EvictionRequestPodReference{
							Name: "pod2",
							UID:  "5e153f5b-e420-41b4-9040-8f3b51cf2162",
						},
					},
					Requester: "drain.foo.com/bar",
					Intent:    lifecycle.EvictionRequestIntentEviction,
				},
				Status: lifecycle.EvictionRequestStatus{
					ObservedGeneration: new(int64(1)),
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionFailed), Status: metav1.ConditionTrue, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: string(lifecycle.EvictionConditionReasonNoFurtherResponder), Message: "Canceled because there is no further responder."},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod3",
					Namespace:         "ns3",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionRequestSpec{
					Target: lifecycle.EvictionRequestTarget{
						Pod: &lifecycle.EvictionRequestPodReference{
							Name: "pod3",
							UID:  "68018c4c-bcca-4465-964a-66d8a1626686",
						},
					},
					Requester: "drain.foo.com/bar",
					Intent:    lifecycle.EvictionRequestIntentWithdrawn,
				},
				Status: lifecycle.EvictionRequestStatus{
					ObservedGeneration: new(int64(1)),
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionFailed), Status: metav1.ConditionTrue, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: string(lifecycle.EvictionConditionReasonCanceledDueToNoRequesters), Message: "Canceled due to no requesters."},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod4",
					Namespace:         "ns4",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionRequestSpec{
					Target: lifecycle.EvictionRequestTarget{
						Pod: &lifecycle.EvictionRequestPodReference{
							Name: "pod4",
							UID:  "3d7fdff1-3fe5-48b9-b106-1ee24b0277f6",
						},
					},
					Requester: "drain.foo.com/bar",
					Intent:    lifecycle.EvictionRequestIntentEviction,
				},
				Status: lifecycle.EvictionRequestStatus{
					ObservedGeneration: new(int64(1)),
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionTargetEvicted), Status: metav1.ConditionFalse, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: "EvictionInProgress", Message: "Pod is being evicted."},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod5",
					Namespace:         "ns5",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionRequestSpec{
					Target: lifecycle.EvictionRequestTarget{
						Pod: &lifecycle.EvictionRequestPodReference{
							Name: "pod5",
							UID:  "9f4a1620-0e97-4246-b304-5969b2104377",
						},
					},
					Requester: "drain.foo.com/bar",
					Intent:    lifecycle.EvictionRequestIntentEviction,
				},
				Status: lifecycle.EvictionRequestStatus{
					ObservedGeneration: new(int64(1)),
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionTargetEvicted), Status: metav1.ConditionFalse, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: "Canceled", Message: "Eviction failed due to cancelation."},
						{Type: string(lifecycle.EvictionConditionFailed), Status: metav1.ConditionTrue, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: string(lifecycle.EvictionConditionReasonEvictionInvalid), Message: "Pod pod6 was not found."},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod6",
					Namespace:         "ns6",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionRequestSpec{
					Target: lifecycle.EvictionRequestTarget{
						Pod: &lifecycle.EvictionRequestPodReference{
							Name: "pod6",
							UID:  "e968f679-523b-4e63-9417-0cd9ae13a0b2",
						},
					},
					Requester: "drain.foo.com/bar",
					Intent:    lifecycle.EvictionRequestIntentEviction,
				},
			},
		},
	}

	// 列： Name, Target, Target Type, Status, Requester, Intent, Age
	expected := []metav1.TableRow{
		{Cells: []interface{}{"pod-1-pod1", "pod1", "Pod", "TargetEvicted (PodDeleted)", "drain.foo.com/bar", "Eviction", "5d"}},
		{Cells: []interface{}{"pod-1-pod2", "pod2", "Pod", "Failed (NoFurtherResponder)", "drain.foo.com/bar", "Eviction", "5d"}},
		{Cells: []interface{}{"pod-1-pod3", "pod3", "Pod", "Failed (CanceledDueToNoRequesters)", "drain.foo.com/bar", "Withdrawn", "5d"}},
		{Cells: []interface{}{"pod-1-pod4", "pod4", "Pod", "Progressing", "drain.foo.com/bar", "Eviction", "5d"}},
		{Cells: []interface{}{"pod-1-pod5", "pod5", "Pod", "Failed (EvictionInvalid)", "drain.foo.com/bar", "Eviction", "5d"}},
		{Cells: []interface{}{"pod-1-pod6", "pod6", "Pod", "Pending", "drain.foo.com/bar", "Eviction", "5d"}},
	}

	rows, err := printEvictionRequestList(&evictionRequestList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error generating table rows for EvictionRequestList: %#v", err)
	}
	for i := range rows {
		rows[i].Object.Object = nil
	}

	if !reflect.DeepEqual(expected, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expected, rows))
	}
}

func TestPrintEviction(t *testing.T) {
	now := time.Now().UTC().AddDate(0, 0, -3)
	daysLater := now.AddDate(0, 0, 5).Add(time.Minute)
	dayAgo := now.AddDate(0, 0, -1)
	twoDaysAgo := now.AddDate(0, 0, -2)

	eviction := lifecycle.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-1-pod1",
			Namespace:         "ns1",
			CreationTimestamp: metav1.Time{Time: twoDaysAgo},
		},
		Spec: lifecycle.EvictionSpec{
			Target: lifecycle.EvictionTarget{
				Pod: &lifecycle.EvictionPodReference{
					Name: "pod1",
					UID:  "3d7fdff1-3fe5-48b9-b106-1ee24b0277f6",
				},
			},
		},
		Status: lifecycle.EvictionStatus{
			ObservedGeneration: new(int64(1)),
			TargetResponders: []lifecycle.TargetResponder{
				{Name: "responder1", State: lifecycle.ResponderStateCompleted},
				{Name: "responder2", State: lifecycle.ResponderStateActive},
			},
			Responders: []lifecycle.ResponderStatus{
				{Name: "responder1", HeartbeatTime: &metav1.Time{Time: dayAgo}, ExpectedCompletionTime: nil, StartTime: &metav1.Time{Time: twoDaysAgo}, CompletionTime: &metav1.Time{Time: dayAgo}, Message: new("completed message")},
				{Name: "responder2", HeartbeatTime: &metav1.Time{Time: now}, ExpectedCompletionTime: &metav1.Time{Time: daysLater}, StartTime: &metav1.Time{Time: now}, CompletionTime: nil, Message: new("migrating pod1 message")},
			},
			Requesters: []lifecycle.Requester{
				{
					Name:   "drain.foo.com/bar",
					Intent: lifecycle.RequesterIntentEviction,
				},
			},
			Conditions: []metav1.Condition{},
		},
	}

	tests := []struct {
		options  printers.GenerateOptions
		expected []metav1.TableRow
	}{
		{
			options: printers.GenerateOptions{Wide: false},
			expected: []metav1.TableRow{
				// 列： Name, Target, Target Type, Status, Active Responder, Responder Status, Responder Expected Finish, Requesters, Age
				{Cells: []interface{}{"pod-1-pod1", "pod1", "Pod", "Progressing", "responder2 (2/2)", "Started (3d ago)", "in 2d", "drain.foo.com/bar", "5d"}},
			},
		},
		{
			options: printers.GenerateOptions{Wide: true},
			expected: []metav1.TableRow{
				// 列： Name, Target, Target Type, Status, Active Responder, Responder Status, Responder Expected Finish, Requesters, Age, Responder Status Message, Responder Heartbeat
				{Cells: []interface{}{"pod-1-pod1", "pod1", "Pod", "Progressing", "responder2 (2/2)", "Started (3d ago)", "in 2d", "drain.foo.com/bar", "5d", "3d ago", "migrating pod1 message"}},
			},
		},
	}

	for _, test := range tests {
		rows, err := printEviction(&eviction, test.options)
		if err != nil {
			t.Fatalf("Error generating table rows for Eviction: %#v", err)
		}
		rows[0].Object.Object = nil
		if !reflect.DeepEqual(test.expected, rows) {
			t.Errorf("mismatch: %s", cmp.Diff(test.expected, rows))
		}
	}
}

func TestPrintEvictionList(t *testing.T) {
	now := time.Now().UTC().AddDate(0, 0, -3)
	daysLater := now.AddDate(0, 0, 5).Add(time.Minute)
	dayAgo := now.AddDate(0, 0, -1)
	twoDaysAgo := now.AddDate(0, 0, -2)
	evictionList := lifecycle.EvictionList{
		Items: []lifecycle.Eviction{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod1",
					Namespace:         "ns1",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionSpec{
					Target: lifecycle.EvictionTarget{
						Pod: &lifecycle.EvictionPodReference{
							Name: "pod1",
							UID:  "bc134542-aa19-4361-8fe2-cf18f78848c0",
						},
					},
				},
				Status: lifecycle.EvictionStatus{
					ObservedGeneration: new(int64(1)),
					Requesters: []lifecycle.Requester{
						{
							Name:   "drain.foo.com/bar",
							Intent: lifecycle.RequesterIntentEviction,
						},
					},
					TargetResponders: []lifecycle.TargetResponder{
						{Name: "responder1", State: lifecycle.ResponderStateCompleted},
					},
					Responders: []lifecycle.ResponderStatus{
						{Name: "responder1", HeartbeatTime: &metav1.Time{Time: dayAgo}, ExpectedCompletionTime: &metav1.Time{Time: dayAgo}, StartTime: &metav1.Time{Time: twoDaysAgo}, CompletionTime: &metav1.Time{Time: dayAgo}, Message: new("migrated1")},
					},
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionTargetEvicted), Status: metav1.ConditionTrue, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: dayAgo}, Reason: string(lifecycle.EvictionConditionReasonPodDeleted), Message: "Pod was successfully deleted."},
					},
				},
			},

			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod2",
					Namespace:         "ns2",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionSpec{
					Target: lifecycle.EvictionTarget{
						Pod: &lifecycle.EvictionPodReference{
							Name: "pod2",
							UID:  "5e153f5b-e420-41b4-9040-8f3b51cf2162",
						},
					},
				},
				Status: lifecycle.EvictionStatus{
					ObservedGeneration: new(int64(1)),
					Requesters: []lifecycle.Requester{
						{
							Name:   "drain.foo.com/bar",
							Intent: lifecycle.RequesterIntentEviction,
						},
					},
					TargetResponders: []lifecycle.TargetResponder{
						{Name: "responder1", State: lifecycle.ResponderStateInterrupted},
					},
					Responders: []lifecycle.ResponderStatus{
						{Name: "responder1", HeartbeatTime: &metav1.Time{Time: dayAgo}, ExpectedCompletionTime: &metav1.Time{Time: dayAgo}, StartTime: &metav1.Time{Time: twoDaysAgo}, CompletionTime: nil, Message: new("running for a long time")}},
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionFailed), Status: metav1.ConditionTrue, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: string(lifecycle.EvictionConditionReasonNoFurtherResponder), Message: "Canceled because there is no further responder."},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod3",
					Namespace:         "ns3",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionSpec{
					Target: lifecycle.EvictionTarget{
						Pod: &lifecycle.EvictionPodReference{
							Name: "pod3",
							UID:  "a4ea9433-da6a-4bfc-9033-296488ba12be",
						},
					},
				},
				Status: lifecycle.EvictionStatus{
					ObservedGeneration: new(int64(1)),
					Requesters: []lifecycle.Requester{
						{
							Name:   "drain.foo.com/bar",
							Intent: lifecycle.RequesterIntentEviction,
						},
					},
					TargetResponders: []lifecycle.TargetResponder{
						{Name: "responder1", State: lifecycle.ResponderStateCompleted},
					},
					Responders: []lifecycle.ResponderStatus{
						{Name: "responder1", HeartbeatTime: &metav1.Time{Time: dayAgo}, ExpectedCompletionTime: nil, StartTime: &metav1.Time{Time: twoDaysAgo}, CompletionTime: &metav1.Time{Time: dayAgo}, Message: new("completed message")}},
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionFailed), Status: metav1.ConditionTrue, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: string(lifecycle.EvictionConditionReasonNoFurtherResponder), Message: "Canceled because there is no further responder."},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod4",
					Namespace:         "ns4",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionSpec{
					Target: lifecycle.EvictionTarget{
						Pod: &lifecycle.EvictionPodReference{
							Name: "pod4",
							UID:  "68018c4c-bcca-4465-964a-66d8a1626686",
						},
					},
				},
				Status: lifecycle.EvictionStatus{
					ObservedGeneration: new(int64(1)),
					Requesters: []lifecycle.Requester{
						{
							Name:   "drain.foo.com/bar",
							Intent: lifecycle.RequesterIntentWithdrawn,
						},
					},
					TargetResponders: []lifecycle.TargetResponder{
						{Name: "responder3", State: lifecycle.ResponderStateCanceled},
						{Name: "responder4", State: lifecycle.ResponderStateInactive},
					},
					Responders: []lifecycle.ResponderStatus{
						{Name: "responder3", HeartbeatTime: &metav1.Time{Time: now}, ExpectedCompletionTime: nil, StartTime: &metav1.Time{Time: now}, CompletionTime: nil, Message: new("running3")},
						{Name: "responder4", HeartbeatTime: nil, ExpectedCompletionTime: nil, StartTime: nil, CompletionTime: nil, Message: nil},
					},
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionFailed), Status: metav1.ConditionTrue, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: string(lifecycle.EvictionConditionReasonCanceledDueToNoRequesters), Message: "Canceled due to no requesters."},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod5",
					Namespace:         "ns5",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionSpec{
					Target: lifecycle.EvictionTarget{
						Pod: &lifecycle.EvictionPodReference{
							Name: "pod5",
							UID:  "3d7fdff1-3fe5-48b9-b106-1ee24b0277f6",
						},
					},
				},
				Status: lifecycle.EvictionStatus{
					ObservedGeneration: new(int64(1)),
					Requesters: []lifecycle.Requester{
						{
							Name:   "drain.foo.com/bar",
							Intent: lifecycle.RequesterIntentEviction,
						},
					},
					TargetResponders: []lifecycle.TargetResponder{
						{Name: "responder1", State: lifecycle.ResponderStateCompleted},
						{Name: "responder2", State: lifecycle.ResponderStateActive},
					},
					Responders: []lifecycle.ResponderStatus{
						{Name: "responder1", HeartbeatTime: &metav1.Time{Time: dayAgo}, ExpectedCompletionTime: nil, StartTime: &metav1.Time{Time: twoDaysAgo}, CompletionTime: &metav1.Time{Time: dayAgo}, Message: new("completed2")},
						{Name: "responder2", HeartbeatTime: &metav1.Time{Time: now}, ExpectedCompletionTime: &metav1.Time{Time: daysLater}, StartTime: &metav1.Time{Time: now}, CompletionTime: nil, Message: new("running2")},
					},
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionTargetEvicted), Status: metav1.ConditionFalse, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: "EvictionInProgress", Message: "Pod is being evicted."},
					},
				},
			},

			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod6",
					Namespace:         "ns6",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionSpec{
					Target: lifecycle.EvictionTarget{
						Pod: &lifecycle.EvictionPodReference{
							Name: "pod6",
							UID:  "aac2a982-2176-4770-8c8c-bcdf51b24fde",
						},
					},
				},
				Status: lifecycle.EvictionStatus{
					ObservedGeneration: new(int64(1)),
					Requesters: []lifecycle.Requester{
						{
							Name:   "drain.foo.com/bar",
							Intent: lifecycle.RequesterIntentEviction,
						},
					},
					TargetResponders: []lifecycle.TargetResponder{
						{Name: "responder1", State: lifecycle.ResponderStateCompleted},
						{Name: "responder2", State: lifecycle.ResponderStateActive},
					},
					Responders: []lifecycle.ResponderStatus{
						{Name: "responder1", HeartbeatTime: &metav1.Time{Time: dayAgo}, ExpectedCompletionTime: nil, StartTime: &metav1.Time{Time: twoDaysAgo}, CompletionTime: &metav1.Time{Time: dayAgo}, Message: new("completed2")},
						{Name: "responder2", HeartbeatTime: &metav1.Time{Time: now}, ExpectedCompletionTime: &metav1.Time{Time: dayAgo}, StartTime: &metav1.Time{Time: now}, CompletionTime: nil, Message: new("running2")},
					},
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionTargetEvicted), Status: metav1.ConditionFalse, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: "EvictionInProgress", Message: "Pod is being evicted."},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod7",
					Namespace:         "ns7",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionSpec{
					Target: lifecycle.EvictionTarget{
						Pod: &lifecycle.EvictionPodReference{
							Name: "pod7",
							UID:  "707ba618-ffd6-4797-b503-17af1c7f4d98",
						},
					},
				},
				Status: lifecycle.EvictionStatus{
					ObservedGeneration: new(int64(1)),
					Requesters: []lifecycle.Requester{
						{
							Name:   "rescheduler.bar.com",
							Intent: lifecycle.RequesterIntentEviction,
						},
						{
							Name:   "drain.foo.com/bar",
							Intent: lifecycle.RequesterIntentEviction,
						},
					},
					TargetResponders: []lifecycle.TargetResponder{
						{Name: "responder3", State: lifecycle.ResponderStateActive},
						{Name: "responder4", State: lifecycle.ResponderStateInactive},
					},
					Responders: []lifecycle.ResponderStatus{
						{Name: "responder3", HeartbeatTime: &metav1.Time{Time: now}, ExpectedCompletionTime: nil, StartTime: &metav1.Time{Time: now}, CompletionTime: nil, Message: new("running3")},
						{Name: "responder4", HeartbeatTime: nil, ExpectedCompletionTime: nil, StartTime: nil, CompletionTime: nil, Message: nil},
					},

					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionTargetEvicted), Status: metav1.ConditionFalse, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: "EvictionInProgress", Message: "Pod is being evicted."},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod8",
					Namespace:         "ns8",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionSpec{
					Target: lifecycle.EvictionTarget{
						Pod: &lifecycle.EvictionPodReference{
							Name: "pod8",
							UID:  "30741366-8d1f-4385-84ff-0f2e7ba0305f",
						},
					},
				},
				Status: lifecycle.EvictionStatus{
					ObservedGeneration: new(int64(1)),
					Requesters: []lifecycle.Requester{
						{
							Name:   "drain.foo.com/bar",
							Intent: lifecycle.RequesterIntentEviction,
						},
					},
					TargetResponders: []lifecycle.TargetResponder{
						{Name: "responder3", State: lifecycle.ResponderStateInactive},
						{Name: "responder4", State: lifecycle.ResponderStateInactive},
					},
					Responders: []lifecycle.ResponderStatus{
						{Name: "responder3", HeartbeatTime: nil, ExpectedCompletionTime: nil, StartTime: nil, CompletionTime: nil, Message: nil},
						{Name: "responder4", HeartbeatTime: nil, ExpectedCompletionTime: nil, StartTime: nil, CompletionTime: nil, Message: nil},
					},
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionTargetEvicted), Status: metav1.ConditionFalse, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: "WaitingForResponder", Message: "Waiting for an responder."},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod9",
					Namespace:         "ns9",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionSpec{
					Target: lifecycle.EvictionTarget{
						Pod: &lifecycle.EvictionPodReference{
							Name: "pod9",
							UID:  "9f4a1620-0e97-4246-b304-5969b2104377",
						},
					},
				},
				Status: lifecycle.EvictionStatus{
					ObservedGeneration: new(int64(1)),
					Requesters: []lifecycle.Requester{
						{
							Name:   "drain.foo.com/bar",
							Intent: lifecycle.RequesterIntentEviction,
						},
					},
					TargetResponders: []lifecycle.TargetResponder{
						{Name: "responder3", State: lifecycle.ResponderStateInactive},
						{Name: "responder4", State: lifecycle.ResponderStateInactive},
					},
					Responders: []lifecycle.ResponderStatus{
						{Name: "responder3", HeartbeatTime: nil, ExpectedCompletionTime: nil, StartTime: nil, CompletionTime: nil, Message: nil},
						{Name: "responder4", HeartbeatTime: nil, ExpectedCompletionTime: nil, StartTime: nil, CompletionTime: nil, Message: nil},
					},
					Conditions: []metav1.Condition{
						{Type: string(lifecycle.EvictionConditionTargetEvicted), Status: metav1.ConditionFalse, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: "Canceled", Message: "Eviction failed due to cancelation."},
						{Type: string(lifecycle.EvictionConditionFailed), Status: metav1.ConditionTrue, ObservedGeneration: 1, LastTransitionTime: metav1.Time{Time: now}, Reason: string(lifecycle.EvictionConditionReasonEvictionInvalid), Message: "Pod pod6 was not found."},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pod-1-pod10",
					Namespace:         "ns10",
					CreationTimestamp: metav1.Time{Time: twoDaysAgo},
				},
				Spec: lifecycle.EvictionSpec{
					Target: lifecycle.EvictionTarget{
						Pod: &lifecycle.EvictionPodReference{
							Name: "pod10",
							UID:  "e968f679-523b-4e63-9417-0cd9ae13a0b2",
						},
					},
				},
			},
		},
	}

	// 列： Name, Target, Target Type, Status, Active Responder, Responder Status, Responder Expected Finish, Requesters, Age
	expected := []metav1.TableRow{
		{Cells: []interface{}{"pod-1-pod1", "pod1", "Pod", "TargetEvicted (PodDeleted)", "responder1 (1/1)", "Completed (4d ago)", "-", "drain.foo.com/bar", "5d"}},
		{Cells: []interface{}{"pod-1-pod2", "pod2", "Pod", "Failed (NoFurtherResponder)", "responder1 (1/1)", "Interrupted", "-", "drain.foo.com/bar", "5d"}},
		{Cells: []interface{}{"pod-1-pod3", "pod3", "Pod", "Failed (NoFurtherResponder)", "responder1 (1/1)", "Completed (4d ago)", "-", "drain.foo.com/bar", "5d"}},
		{Cells: []interface{}{"pod-1-pod4", "pod4", "Pod", "Failed (CanceledDueToNoRequesters)", "responder3 (1/2)", "Canceled", "-", "<withdrawn>", "5d"}},
		{Cells: []interface{}{"pod-1-pod5", "pod5", "Pod", "Progressing", "responder2 (2/2)", "Started (3d ago)", "in 2d", "drain.foo.com/bar", "5d"}},
		{Cells: []interface{}{"pod-1-pod6", "pod6", "Pod", "Progressing", "responder2 (2/2)", "Started (3d ago)", "4d ago", "drain.foo.com/bar", "5d"}},
		{Cells: []interface{}{"pod-1-pod7", "pod7", "Pod", "Progressing", "responder3 (1/2)", "Started (3d ago)", "<unknown>", "drain.foo.com/bar + 1 more...", "5d"}},
		{Cells: []interface{}{"pod-1-pod8", "pod8", "Pod", "Progressing", "<unset> (0/2)", "<unset>", "<unknown>", "drain.foo.com/bar", "5d"}},
		{Cells: []interface{}{"pod-1-pod9", "pod9", "Pod", "Failed (EvictionInvalid)", "<unset> (0/2)", "<unset>", "<unknown>", "drain.foo.com/bar", "5d"}},
		{Cells: []interface{}{"pod-1-pod10", "pod10", "Pod", "Pending", "<unset> (0/0)", "<unset>", "<unknown>", "<none>", "5d"}},
	}

	rows, err := printEvictionList(&evictionList, printers.GenerateOptions{})
	if err != nil {
		t.Fatalf("Error generating table rows for EvictionList: %#v", err)
	}
	for i := range rows {
		rows[i].Object.Object = nil
	}

	if !reflect.DeepEqual(expected, rows) {
		t.Errorf("mismatch: %s", cmp.Diff(expected, rows))
	}
}
