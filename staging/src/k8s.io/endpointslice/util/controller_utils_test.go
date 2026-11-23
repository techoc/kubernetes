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

package util

import (
	"fmt"
	"hash/adler32"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/dump"
	"k8s.io/utils/ptr"
)

// TestDetermineNeededServiceUpdates 验证 determineNeededServiceUpdates 在 podChanged 为 false/true 时，
// 分别返回新旧 Service 集合的对称差集与并集。
func TestDetermineNeededServiceUpdates(t *testing.T) {
	testCases := []struct {
		// 表驱动用例：a、b 为新旧 Service 集合，xor/union 为两种模式下的期望结果。
		name  string
		a     sets.Set[string]
		b     sets.Set[string]
		union sets.Set[string]
		xor   sets.Set[string]
	}{
		{
			name:  "no services changed",
			a:     sets.New[string]("a", "b", "c"),
			b:     sets.New[string]("a", "b", "c"),
			xor:   sets.New[string](),
			union: sets.New[string]("a", "b", "c"),
		},
		{
			name:  "all old services removed, new services added",
			a:     sets.New[string]("a", "b", "c"),
			b:     sets.New[string]("d", "e", "f"),
			xor:   sets.New[string]("a", "b", "c", "d", "e", "f"),
			union: sets.New[string]("a", "b", "c", "d", "e", "f"),
		},
		{
			name:  "all old services removed, no new services added",
			a:     sets.New[string]("a", "b", "c"),
			b:     sets.New[string](),
			xor:   sets.New[string]("a", "b", "c"),
			union: sets.New[string]("a", "b", "c"),
		},
		{
			name:  "no old services, but new services added",
			a:     sets.New[string](),
			b:     sets.New[string]("a", "b", "c"),
			xor:   sets.New[string]("a", "b", "c"),
			union: sets.New[string]("a", "b", "c"),
		},
		{
			name:  "one service removed, one service added, two unchanged",
			a:     sets.New[string]("a", "b", "c"),
			b:     sets.New[string]("b", "c", "d"),
			xor:   sets.New[string]("a", "d"),
			union: sets.New[string]("a", "b", "c", "d"),
		},
		{
			name:  "no services",
			a:     sets.New[string](),
			b:     sets.New[string](),
			xor:   sets.New[string](),
			union: sets.New[string](),
		},
	}

	for _, testCase := range testCases {
		// 遍历用例，分别在 Pod 未变化和已变化时断言结果。
		t.Run(testCase.name, func(t *testing.T) {
			// podChanged=false：期望对称差集。
			retval := determineNeededServiceUpdates(testCase.a, testCase.b, false)
			if !retval.Equal(testCase.xor) {
				t.Errorf("%s (with podChanged=false): expected: %v  got: %v", testCase.name, sets.List(testCase.xor), sets.List(retval))
			}

			retval = determineNeededServiceUpdates(testCase.a, testCase.b, true)
			// podChanged=true：期望并集。
			if !retval.Equal(testCase.union) {
				t.Errorf("%s (with podChanged=true): expected: %v  got: %v", testCase.name, sets.List(testCase.union), sets.List(retval))
			}
		})
	}
}

// TestShouldPodBeInEndpoints 覆盖终态、无 IP、终止中、多 IP 等 Pod，
// 验证 ShouldPodBeInEndpoints 是否应将其纳入 endpoint。
func TestShouldPodBeInEndpoints(t *testing.T) {
	testCases := []struct {
		// 用例包含 Pod 对象、includeTerminating 开关与期望结果。
		name               string
		pod                *v1.Pod
		expected           bool
		includeTerminating bool
	}{
		// Pod should not be in endpoints:
		// 以下场景不应出现在 endpoint 中。
		{
			name: "Failed pod with Never RestartPolicy",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyNever,
				},
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					PodIP: "1.2.3.4",
				},
			},
			expected: false,
		},
		{
			name: "Succeeded pod with Never RestartPolicy",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyNever,
				},
				Status: v1.PodStatus{
					Phase: v1.PodSucceeded,
					PodIP: "1.2.3.4",
				},
			},
			expected: false,
		},
		{
			name: "Succeeded pod with OnFailure RestartPolicy",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyOnFailure,
				},
				Status: v1.PodStatus{
					Phase: v1.PodSucceeded,
					PodIP: "1.2.3.4",
				},
			},
			expected: false,
		},
		{
			name: "Empty Pod IPs, Running pod with OnFailure RestartPolicy",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyNever,
				},
				Status: v1.PodStatus{
					Phase:  v1.PodRunning,
					PodIP:  "",
					PodIPs: []v1.PodIP{},
				},
			},
			expected: false,
		},
		{
			name: "Terminating Pod with includeTerminating=false",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{
						Time: time.Now(),
					},
				},
				Spec: v1.PodSpec{},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					PodIP: "1.2.3.4",
				},
			},
			expected: false,
		},
		{
			name: "Failed pod with Always RestartPolicy",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyAlways,
				},
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					PodIP: "1.2.3.4",
				},
			},
			expected: false,
		},
		// Pod should be in endpoints:
		// 以下场景应出现在 endpoint 中。
		{
			name: "Pending pod with Never RestartPolicy",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyNever,
				},
				Status: v1.PodStatus{
					Phase: v1.PodPending,
					PodIP: "1.2.3.4",
				},
			},
			expected: true,
		},
		{
			name: "Unknown pod with OnFailure RestartPolicy",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyOnFailure,
				},
				Status: v1.PodStatus{
					Phase: v1.PodUnknown,
					PodIP: "1.2.3.4",
				},
			},
			expected: true,
		},
		{
			name: "Running pod with Never RestartPolicy",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyNever,
				},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					PodIP: "1.2.3.4",
				},
			},
			expected: true,
		},
		{
			name: "Multiple Pod IPs, Running pod with OnFailure RestartPolicy",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyNever,
				},
				Status: v1.PodStatus{
					Phase:  v1.PodRunning,
					PodIPs: []v1.PodIP{{IP: "1.2.3.4"}, {IP: "1234::5678:0000:0000:9abc:def0"}},
				},
			},
			expected: true,
		},
		{
			name: "Terminating Pod with includeTerminating=true",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{
						Time: time.Now(),
					},
				},
				Spec: v1.PodSpec{},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					PodIP: "1.2.3.4",
				},
			},
			expected:           true,
			includeTerminating: true,
		},
	}

	for _, test := range testCases {
		// 逐例调用被测函数并比对期望布尔值。
		t.Run(test.name, func(t *testing.T) {
			result := ShouldPodBeInEndpoints(test.pod, test.includeTerminating)
			if result != test.expected {
				t.Errorf("expected: %t, got: %t", test.expected, result)
			}
		})
	}
}

// TestShouldSetHostname 验证 hostname 设置条件：Pod 有 hostname、
// subdomain 与 Service 名称相同且命名空间一致。
func TestShouldSetHostname(t *testing.T) {
	testCases := map[string]struct {
		// 以名称索引的用例，包含 Pod、Service 与期望值。
		pod      *v1.Pod
		service  *v1.Service
		expected bool
	}{
		"all matching": {
			pod:      genSimplePod("ns", "foo", "svc-name"),
			service:  genSimpleSvc("ns", "svc-name"),
			expected: true,
		},
		"all matching, hostname not set": {
			pod:      genSimplePod("ns", "", "svc-name"),
			service:  genSimpleSvc("ns", "svc-name"),
			expected: false,
		},
		"all set, different name/subdomain": {
			pod:      genSimplePod("ns", "hostname", "subdomain"),
			service:  genSimpleSvc("ns", "name"),
			expected: false,
		},
		"all set, different namespace": {
			pod:      genSimplePod("ns1", "hostname", "svc-name"),
			service:  genSimpleSvc("ns2", "svc-name"),
			expected: false,
		},
	}

	for name, testCase := range testCases {
		// 逐例调用并断言。
		t.Run(name, func(t *testing.T) {
			result := ShouldSetHostname(testCase.pod, testCase.service)
			if result != testCase.expected {
				t.Errorf("expected: %t, got: %t", testCase.expected, result)
			}
		})
	}
}

// genSimplePod 构造仅含命名空间、hostname 与 subdomain 的简单 Pod，供 hostname 测试使用。
func genSimplePod(namespace, hostname, subdomain string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
		},
		Spec: v1.PodSpec{
			Hostname:  hostname,
			Subdomain: subdomain,
		},
	}
}

// genSimpleSvc 构造仅含命名空间与名称的简单 Service。
func genSimpleSvc(namespace, name string) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

// TestGetPodServicesToUpdate 用带选择器的 Service 与不同标签的 Pod，
// 验证 GetPodUpdateProjectionKey 与 GetServicesToUpdate 能找到匹配的 Service。
func TestGetPodServicesToUpdate(t *testing.T) {
	// 创建 fake informer 工厂用于提供 Service lister。
	fakeInformerFactory := informers.NewSharedInformerFactory(&fake.Clientset{}, 0*time.Second)
	for i := 0; i < 3; i++ {
		// 预置 3 个 Service，分别按 app=test-i 选择 Pod。
		service := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("service-%d", i),
				Namespace: "test",
			},
			Spec: v1.ServiceSpec{
				Selector: map[string]string{
					"app": fmt.Sprintf("test-%d", i),
				},
			},
		}
		fakeInformerFactory.Core().V1().Services().Informer().GetStore().Add(service)
	}
	var pods []*v1.Pod
	// 预置 5 个 Pod，其中前 3 个能匹配到 Service。
	for i := 0; i < 5; i++ {
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "test",
				Name:      fmt.Sprintf("test-pod-%d", i),
				Labels: map[string]string{
					"app":   fmt.Sprintf("test-%d", i),
					"label": fmt.Sprintf("label-%d", i),
				},
			},
		}
		pods = append(pods, pod)
	}

	tests := []struct {
		// 期望前 3 个 Pod 各匹配一个 Service，后 2 个为空集。
		name   string
		pod    *v1.Pod
		expect sets.Set[string]
	}{
		{
			name:   "get servicesMemberships for pod-0",
			pod:    pods[0],
			expect: sets.New[string]("test/service-0"),
		},
		{
			name:   "get servicesMemberships for pod-1",
			pod:    pods[1],
			expect: sets.New[string]("test/service-1"),
		},
		{
			name:   "get servicesMemberships for pod-2",
			pod:    pods[2],
			expect: sets.New[string]("test/service-2"),
		},
		{
			name:   "get servicesMemberships for pod-3",
			pod:    pods[3],
			expect: sets.New[string](),
		},
		{
			name:   "get servicesMemberships for pod-4",
			pod:    pods[4],
			expect: sets.New[string](),
		},
	}
	for _, test := range tests {
		// 逐例生成投影键并查询需要更新的 Service。
		t.Run(test.name, func(t *testing.T) {
			services, err := GetServicesToUpdate(fakeInformerFactory.Core().V1().Services().Lister(), GetPodUpdateProjectionKey(test.pod, nil))
			if err != nil {
				t.Errorf("Error from cache.GetServicesToUpdate: %v", err)
			} else if !services.Equal(test.expect) {
				t.Errorf("Expect service %v, but got %v", test.expect, services)
			}
		})
	}
}

// BenchmarkGetPodServicesToUpdate 在 1000 个 Service 的场景下，
// 测量查询 Pod 所属 Service 的性能。
func BenchmarkGetPodServicesToUpdate(b *testing.B) {
	// init fake service informer.
	// 初始化带 1000 个 Service 的 fake informer。
	fakeInformerFactory := informers.NewSharedInformerFactory(&fake.Clientset{}, 0*time.Second)
	for i := 0; i < 1000; i++ {
		service := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("service-%d", i),
				Namespace: "test",
			},
			Spec: v1.ServiceSpec{
				Selector: map[string]string{
					"app": fmt.Sprintf("test-%d", i),
				},
			},
		}
		fakeInformerFactory.Core().V1().Services().Informer().GetStore().Add(service)
	}

	pod := &v1.Pod{
		// 构造只匹配 service-0 的 Pod。
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test",
			Name:      "test-pod-0",
			Labels: map[string]string{
				"app": "test-0",
			},
		},
	}

	// 期望结果只包含 test/service-0。
	expect := sets.NewString("test/service-0")
	b.ResetTimer()
	// 重置计时器，排除初始化开销。
	for i := 0; i < b.N; i++ {
		// 压测循环：查询结果数量应与期望一致。
		services, err := GetServicesToUpdate(fakeInformerFactory.Core().V1().Services().Lister(), GetPodUpdateProjectionKey(pod, nil))
		if err != nil {
			b.Fatalf("Error from GetServicesToUpdate(): %v", err)
		}
		if len(services) != len(expect) {
			b.Errorf("Expect services size %d, but got: %v", len(expect), len(services))
		}
	}
}

// Test_podChanged 用 modifier 修改原始 Pod 的副本，
// 验证 podEndpointsChanged 对 Pod 字段与标签变化的判定。
func Test_podChanged(t *testing.T) {
	testCases := []struct {
		// 每条用例通过 modifier 施加一种变化，并给出两个期望布尔值。
		testName      string
		modifier      func(*v1.Pod, *v1.Pod)
		podChanged    bool
		labelsChanged bool
	}{
		{
			testName:      "no changes",
			modifier:      func(old, new *v1.Pod) {},
			podChanged:    false,
			labelsChanged: false,
		}, {
			testName: "change NodeName",
			modifier: func(old, new *v1.Pod) {
				new.Spec.NodeName = "changed"
			},
			// NodeName can only change before the pod has an IP, and we don't care about the
			// pod yet at that point so we ignore this change
			podChanged:    false,
			labelsChanged: false,
		}, {
			testName: "change ResourceVersion",
			modifier: func(old, new *v1.Pod) {
				new.ObjectMeta.ResourceVersion = "changed"
			},
			// ResourceVersion is intentionally ignored if nothing else changed
			podChanged:    false,
			labelsChanged: false,
		}, {
			testName: "add primary IPv4",
			modifier: func(old, new *v1.Pod) {
				new.Status.PodIP = "1.2.3.4"
				new.Status.PodIPs = []v1.PodIP{{IP: "1.2.3.4"}}
			},
			podChanged:    true,
			labelsChanged: false,
		}, {
			testName: "modify primary IPv4",
			modifier: func(old, new *v1.Pod) {
				old.Status.PodIP = "1.2.3.4"
				old.Status.PodIPs = []v1.PodIP{{IP: "1.2.3.4"}}
				new.Status.PodIP = "2.3.4.5"
				new.Status.PodIPs = []v1.PodIP{{IP: "2.3.4.5"}}
			},
			podChanged:    true,
			labelsChanged: false,
		}, {
			testName: "add primary IPv6",
			modifier: func(old, new *v1.Pod) {
				new.Status.PodIP = "fd00:10:96::1"
				new.Status.PodIPs = []v1.PodIP{{IP: "fd00:10:96::1"}}
			},
			podChanged:    true,
			labelsChanged: false,
		}, {
			testName: "modify primary IPv6",
			modifier: func(old, new *v1.Pod) {
				old.Status.PodIP = "fd00:10:96::1"
				old.Status.PodIPs = []v1.PodIP{{IP: "fd00:10:96::1"}}
				new.Status.PodIP = "fd00:10:96::2"
				new.Status.PodIPs = []v1.PodIP{{IP: "fd00:10:96::2"}}
			},
			podChanged:    true,
			labelsChanged: false,
		}, {
			testName: "add secondary IP",
			modifier: func(old, new *v1.Pod) {
				old.Status.PodIP = "1.2.3.4"
				old.Status.PodIPs = []v1.PodIP{{IP: "1.2.3.4"}}
				new.Status.PodIP = "1.2.3.4"
				new.Status.PodIPs = []v1.PodIP{{IP: "1.2.3.4"}, {IP: "fd00:10:96::1"}}
			},
			podChanged:    true,
			labelsChanged: false,
		}, {
			testName: "modify secondary IP",
			modifier: func(old, new *v1.Pod) {
				old.Status.PodIP = "1.2.3.4"
				old.Status.PodIPs = []v1.PodIP{{IP: "1.2.3.4"}, {IP: "fd00:10:96::1"}}
				new.Status.PodIP = "1.2.3.4"
				new.Status.PodIPs = []v1.PodIP{{IP: "1.2.3.4"}, {IP: "fd00:10:96::2"}}
			},
			podChanged:    true,
			labelsChanged: false,
		}, {
			testName: "remove secondary IP",
			modifier: func(old, new *v1.Pod) {
				old.Status.PodIP = "1.2.3.4"
				old.Status.PodIPs = []v1.PodIP{{IP: "1.2.3.4"}, {IP: "fd00:10:96::1"}}
				new.Status.PodIP = "1.2.3.4"
				new.Status.PodIPs = []v1.PodIP{{IP: "1.2.3.4"}}
			},
			podChanged:    true,
			labelsChanged: false,
		}, {
			testName: "change readiness",
			modifier: func(old, new *v1.Pod) {
				new.Status.Conditions[0].Status = v1.ConditionTrue
			},
			podChanged:    true,
			labelsChanged: false,
		}, {
			testName: "mark for deletion",
			modifier: func(old, new *v1.Pod) {
				now := metav1.NewTime(time.Now().UTC())
				new.ObjectMeta.DeletionTimestamp = &now
			},
			podChanged:    true,
			labelsChanged: false,
		}, {
			testName: "add label",
			modifier: func(old, new *v1.Pod) {
				new.Labels["label"] = "new"
			},
			podChanged:    false,
			labelsChanged: true,
		}, {
			testName: "modify label",
			modifier: func(old, new *v1.Pod) {
				old.Labels["label"] = "old"
				new.Labels["label"] = "new"
			},
			podChanged:    false,
			labelsChanged: true,
		}, {
			testName: "remove label",
			modifier: func(old, new *v1.Pod) {
				old.Labels["label"] = "old"
			},
			podChanged:    false,
			labelsChanged: true,
		},
	}

	orig := &v1.Pod{
		// 原始 Pod 带有标签、Ready=False 条件，作为各用例的基准。
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test",
			Name:      "pod",
			Labels:    map[string]string{"foo": "bar"},
		},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{Type: v1.PodReady, Status: v1.ConditionFalse},
			},
		},
	}

	for _, tc := range testCases {
		// 深拷贝基准对象后施加变更，再调用被测函数比对两个返回值。
		t.Run(tc.testName, func(t *testing.T) {
			old := orig.DeepCopy()
			new := old.DeepCopy()
			tc.modifier(old, new)

			podChanged, labelsChanged := podEndpointsChanged(old, new)
			if podChanged != tc.podChanged {
				t.Errorf("Expected podChanged to be %t, got %t", tc.podChanged, podChanged)
			}
			if labelsChanged != tc.labelsChanged {
				t.Errorf("Expected labelsChanged to be %t, got %t", tc.labelsChanged, labelsChanged)
			}
		})
	}
}

// TestEndpointsEqualBeyondHash 验证节点名、区域、条件、TargetRef 变化对相等判定的影响，
// 并确认 TargetRef 的 ResourceVersion 差异被忽略。
func TestEndpointsEqualBeyondHash(t *testing.T) {
	tests := []struct {
		// 用例包含两个 Endpoint 与期望的相等结果。
		name     string
		ep1      *discovery.Endpoint
		ep2      *discovery.Endpoint
		expected bool
	}{
		{
			name: "No change",
			ep1: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				NodeName:  ptr.To("node-1"),
			},
			ep2: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				NodeName:  ptr.To("node-1"),
			},
			expected: true,
		},
		{
			name: "NodeName changed",
			ep1: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				NodeName:  ptr.To("node-1"),
			},
			ep2: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				NodeName:  ptr.To("node-2"),
			},
			expected: false,
		},
		{
			name: "Zone changed",
			ep1: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				Zone:      ptr.To("zone-1"),
			},
			ep2: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				Zone:      ptr.To("zone-2"),
			},
			expected: false,
		},
		{
			name: "Ready condition changed",
			ep1: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			ep2: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(false),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			expected: false,
		},
		{
			name: "Serving condition changed from nil to true",
			ep1: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     nil,
					Terminating: nil,
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			ep2: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			expected: false,
		},
		{
			name: "Serving condition changed from false to true",
			ep1: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(false),
					Terminating: ptr.To(false),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			ep2: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			expected: false,
		},
		{
			name: "Pod name changed",
			ep1: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0"},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			ep2: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod1"},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			expected: false,
		},
		{
			name: "Pod resourceVersion changed",
			ep1: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0", ResourceVersion: "1"},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			ep2: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0", ResourceVersion: "2"},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			expected: true,
		},
		{
			name: "Pod resourceVersion removed",
			ep1: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0", ResourceVersion: "1"},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			ep2: &discovery.Endpoint{
				Conditions: discovery.EndpointConditions{
					Ready: ptr.To(true),
				},
				Addresses: []string{"10.0.0.1"},
				TargetRef: &v1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "pod0", ResourceVersion: ""},
				Zone:      ptr.To("zone-1"),
				NodeName:  ptr.To("node-1"),
			},
			expected: true,
		},
	}
	for _, tt := range tests {
		// 逐例调用被测函数并断言。
		t.Run(tt.name, func(t *testing.T) {
			if got := EndpointsEqualBeyondHash(tt.ep1, tt.ep2); got != tt.expected {
				t.Errorf("EndpointsEqualBeyondHash() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// A 是用于深度哈希测试的简单结构体（值类型字段）。
type A struct {
	x int
	y string
}

// B 是包含切片与映射字段的深度哈希测试结构体。
type B struct {
	x []int
	y map[string]bool
}

// C 与 A 字段相同但实现了 String 方法，用于观察格式化行为对哈希的影响。
type C struct {
	x int
	y string
}

// String 返回 C 的自定义字符串表示。
func (c C) String() string {
	return fmt.Sprintf("%d:%s", c.x, c.y)
}

// TestDeepHashObject 用多种类型（标量、切片、映射、指针、自定义类型）
// 验证 deepHashObject 的确定性与可重复性。
func TestDeepHashObject(t *testing.T) {
	successCases := []func() interface{}{
		// 覆盖各类可哈希对象的构造函数。
		func() interface{} { return 8675309 },
		func() interface{} { return "Jenny, I got your number" },
		func() interface{} { return []string{"eight", "six", "seven"} },
		func() interface{} { return [...]int{5, 3, 0, 9} },
		func() interface{} { return map[int]string{8: "8", 6: "6", 7: "7"} },
		func() interface{} { return map[string]int{"5": 5, "3": 3, "0": 0, "9": 9} },
		func() interface{} { return A{867, "5309"} },
		func() interface{} { return &A{867, "5309"} },
		func() interface{} {
			return B{[]int{8, 6, 7}, map[string]bool{"5": true, "3": true, "0": true, "9": true}}
		},
		func() interface{} { return map[A]bool{{8675309, "Jenny"}: true, {9765683, "!Jenny"}: false} },
		func() interface{} { return map[C]bool{{8675309, "Jenny"}: true, {9765683, "!Jenny"}: false} },
		func() interface{} { return map[*A]bool{{8675309, "Jenny"}: true, {9765683, "!Jenny"}: false} },
		func() interface{} { return map[*C]bool{{8675309, "Jenny"}: true, {9765683, "!Jenny"}: false} },
	}

	for _, tc := range successCases {
		// 对每个对象多次哈希，断言同一输入始终得到相同结果。
		hasher1 := adler32.New()
		deepHashObject(hasher1, tc())
		hash1 := hasher1.Sum32()
		deepHashObject(hasher1, tc())
		hash2 := hasher1.Sum32()
		if hash1 != hash2 {
			t.Fatalf("hash of the same object (%q) produced different results: %d vs %d", toString(tc()), hash1, hash2)
		}
		for i := 0; i < 100; i++ {
			// 用两个不同的 hasher 交叉验证 100 次。
			hasher2 := adler32.New()

			deepHashObject(hasher1, tc())
			hash1a := hasher1.Sum32()
			deepHashObject(hasher2, tc())
			hash2a := hasher2.Sum32()

			if hash1a != hash1 {
				t.Errorf("repeated hash of the same object (%q) produced different results: %d vs %d", toString(tc()), hash1, hash1a)
			}
			if hash2a != hash2 {
				t.Errorf("repeated hash of the same object (%q) produced different results: %d vs %d", toString(tc()), hash2, hash2a)
			}
			if hash1a != hash2a {
				t.Errorf("hash of the same object produced (%q) different results: %d vs %d", toString(tc()), hash1a, hash2a)
			}
		}
	}
}

// toString 返回对象的格式化字符串，仅用于测试失败信息。
func toString(obj interface{}) string {
	return dump.Pretty(obj)
}

// wheel 是 TestDeepObjectPointer 使用的轮子对象。
type wheel struct {
	radius uint32
}

// unicycle 包含指针与映射字段，用于验证深度哈希会展开指针指向的值。
// primaryWheel 为指向轮子的指针；licencePlateID 为车牌标识；tags 为附加标签。
type unicycle struct {
	primaryWheel   *wheel
	licencePlateID string
	tags           map[string]string
}

// TestDeepObjectPointer 验证 deepHashObject 会解引用指针：
// 不同指针但相同值得到相同哈希，值不同则哈希不同。
func TestDeepObjectPointer(t *testing.T) {
	// Arrange
	// 准备三个对象，其中两个轮子值相同但指针不同。
	wheel1 := wheel{radius: 17}
	wheel2 := wheel{radius: 22}
	wheel3 := wheel{radius: 17}

	myUni1 := unicycle{licencePlateID: "blah", primaryWheel: &wheel1, tags: map[string]string{"color": "blue", "name": "john"}}
	myUni2 := unicycle{licencePlateID: "blah", primaryWheel: &wheel2, tags: map[string]string{"color": "blue", "name": "john"}}
	myUni3 := unicycle{licencePlateID: "blah", primaryWheel: &wheel3, tags: map[string]string{"color": "blue", "name": "john"}}

	// Run it more than once to verify determinism of hasher.
	// 重复 100 次以验证哈希器的确定性。
	for i := 0; i < 100; i++ {
		hasher1 := adler32.New()
		hasher2 := adler32.New()
		hasher3 := adler32.New()
		// Act
		// 分别计算三个对象的哈希。
		deepHashObject(hasher1, myUni1)
		hash1 := hasher1.Sum32()
		deepHashObject(hasher1, myUni1)
		hash1a := hasher1.Sum32()
		deepHashObject(hasher2, myUni2)
		hash2 := hasher2.Sum32()
		deepHashObject(hasher3, myUni3)
		hash3 := hasher3.Sum32()

		// Assert
		// 断言重复哈希一致、不同值的哈希不同、相同值的哈希相同。
		if hash1 != hash1a {
			t.Errorf("repeated hash of the same object produced different results: %d vs %d", hash1, hash1a)
		}

		if hash1 == hash2 {
			t.Errorf("hash1 (%d) and hash2(%d) must be different because they have different values for wheel size", hash1, hash2)
		}

		if hash1 != hash3 {
			t.Errorf("hash1 (%d) and hash3(%d) must be the same because although they point to different objects, they have the same values for wheel size", hash1, hash3)
		}
	}
}

// TestNewPortMapKey_NilVsEmptySlice 验证 nil 切片与空切片生成相同的 PortMapKey。
func TestNewPortMapKey_NilVsEmptySlice(t *testing.T) {
	var nilPorts []discovery.EndpointPort
	// 分别构造 nil 与空端口切片。
	emptyPorts := []discovery.EndpointPort{}

	// 生成两个键并断言相等。
	nilKey := NewPortMapKey(nilPorts)
	emptyKey := NewPortMapKey(emptyPorts)

	if nilKey != emptyKey {
		t.Errorf("NewPortMapKey should return the same key for nil and empty slice, got nil=%q empty=%q", nilKey, emptyKey)
	}
}
