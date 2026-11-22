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

package reconcilers

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/apitesting"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

func init() {
	// 测试用编解码器只注册 corev1，与 peer endpoint lease 实际存储的 Endpoints 类型一致。
	var scheme = runtime.NewScheme()

	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(corev1.SchemeGroupVersion))

	codecs = serializer.NewCodecFactory(scheme)
}

var codecs serializer.CodecFactory

// serverInfo 描述测试中一个 API server 的 lease 状态：
// existingIP 为初始登记的地址，newIP 非空表示测试期间发生地址变更。
type serverInfo struct {
	existingIP     string
	id             string
	ports          []corev1.EndpointPort
	newIP          string
	removeLease    bool
	expectEndpoint string
}

// NewFakePeerEndpointReconciler 构造直接使用 storage.Interface 的 reconciler，
// 跳过真实 client，便于在单测中精确控制 lease 内容。
func NewFakePeerEndpointReconciler(t *testing.T, baseKey string, s storage.Interface) peerEndpointLeaseReconciler {
	return peerEndpointLeaseReconciler{serverLeases: &peerEndpointLeases{
		storage:   s,
		destroyFn: func() {},
		baseKey:   baseKey,
		leaseTime: 1 * time.Minute, // avoid the lease to timeout on tests
	}}
}

// SetKeys 为每个测试 server 写入初始 lease。
func (f *peerEndpointLeaseReconciler) SetKeys(servers []serverInfo) error {
	for _, server := range servers {
		if err := f.UpdateLease(server.id, server.existingIP, server.ports); err != nil {
			return err
		}
	}
	return nil
}

// TestPeerEndpointLeaseReconciler 验证 lease 地址的选取规则：
// 已有地址优先沿用，新地址出现时以新地址为准，无已有地址时直接登记新地址。
func TestPeerEndpointLeaseReconciler(t *testing.T) {
	// enable feature flags
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.APIServerIdentity: true,
		features.StorageVersionAPI: true,
	})

	server, sc := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	t.Cleanup(func() { server.Terminate(t) })

	newFunc := func() runtime.Object { return &corev1.Endpoints{} }
	newListFunc := func() runtime.Object { return &corev1.EndpointsList{} }
	sc.Codec = apitesting.TestStorageCodec(codecs, corev1.SchemeGroupVersion)

	tests := []struct {
		testName     string
		servers      []serverInfo
		expectLeases []string
	}{
		{
			// 已有地址未变化时应原样保留，不因每次调谐而重写。
			testName: "existing IP satisfy",
			servers: []serverInfo{{
				existingIP:     "4.3.2.1",
				id:             "server-1",
				ports:          []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
				expectEndpoint: "4.3.2.1:8080",
			}, {
				existingIP:     "1.2.3.4",
				id:             "server-2",
				ports:          []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
				expectEndpoint: "1.2.3.4:8080",
			}},
			expectLeases: []string{"4.3.2.1", "1.2.3.4"},
		},
		{
			// 地址变更（例如 API server 重建）后应以新地址覆盖旧值。
			testName: "existing IP + new IP = should return the new IP",
			servers: []serverInfo{{
				existingIP:     "4.3.2.2",
				id:             "server-1",
				ports:          []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
				newIP:          "4.3.2.1",
				expectEndpoint: "4.3.2.1:8080",
			}, {
				existingIP:     "1.2.3.4",
				id:             "server-2",
				ports:          []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
				newIP:          "1.1.1.1",
				expectEndpoint: "1.1.1.1:8080",
			}},
			expectLeases: []string{"4.3.2.1", "1.1.1.1"},
		},
		{
			// 首次登记场景：没有历史 lease，直接采用当前地址。
			testName: "no existing IP, should return new IP",
			servers: []serverInfo{{
				id:             "server-1",
				ports:          []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
				newIP:          "1.2.3.4",
				expectEndpoint: "1.2.3.4:8080",
			}},
			expectLeases: []string{"1.2.3.4"},
		},
	}
	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			// use the same base key used by the controlplane, but add a random
			// prefix so we can reuse the etcd instance for subtests independently.
			baseKey := "/" + uuid.New().String() + "/peerserverleases/"
			s, dFunc, err := factory.Create(*sc.ForResource(schema.GroupResource{Resource: "endpoints"}), newFunc, newListFunc, baseKey)
			if err != nil {
				t.Fatalf("Error creating storage: %v", err)
			}
			t.Cleanup(dFunc)

			fakeReconciler := NewFakePeerEndpointReconciler(t, baseKey, s)
			err = fakeReconciler.SetKeys(test.servers)
			if err != nil {
				t.Errorf("unexpected error creating keys: %v", err)
			}

			for _, server := range test.servers {
				if server.newIP != "" {
					err = fakeReconciler.UpdateLease(server.id, server.newIP, server.ports)
					if err != nil {
						t.Errorf("unexpected error reconciling: %v", err)
					}
				}
			}

			leases, err := fakeReconciler.ListLeases()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// sort for comparison
			sort.Strings(leases)
			sort.Strings(test.expectLeases)
			if !reflect.DeepEqual(leases, test.expectLeases) {
				t.Errorf("expected %v got: %v", test.expectLeases, leases)
			}

			for _, server := range test.servers {
				endpoint, err := fakeReconciler.GetLease(server.id)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if endpoint != server.expectEndpoint {
					t.Errorf("expected %v got: %v", server.expectEndpoint, endpoint)
				}
			}
		})
	}
}

// TestPeerLeaseRemoveEndpoints 验证 lease 删除语义：正常启动时只删除目标 lease，
// 已停止调谐的实例不再执行删除。
func TestPeerLeaseRemoveEndpoints(t *testing.T) {
	// enable feature flags
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.APIServerIdentity: true,
		features.StorageVersionAPI: true,
	})

	server, sc := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	t.Cleanup(func() { server.Terminate(t) })

	newFunc := func() runtime.Object { return &corev1.Endpoints{} }
	newListFunc := func() runtime.Object { return &corev1.EndpointsList{} }
	sc.Codec = apitesting.TestStorageCodec(codecs, corev1.SchemeGroupVersion)

	stopTests := []struct {
		testName         string
		servers          []serverInfo
		expectLeases     []string
		apiServerStartup bool
	}{
		{
			// API server 启动时清理自身旧 lease，其他实例的 lease 必须保留。
			testName: "successful remove previous endpoints before apiserver starts",
			servers: []serverInfo{
				{
					existingIP:  "1.2.3.4",
					id:          "test-server-1",
					ports:       []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
					removeLease: true,
				},
				{
					existingIP: "2.4.6.8",
					id:         "test-server-2",
					ports:      []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
				}},
			expectLeases:     []string{"2.4.6.8"},
			apiServerStartup: true,
		},
		{
			// 已 StopReconciling 的实例即使传入新 IP 也不应改写 lease。
			testName: "stop reconciling with new IP not in existing ip list",
			servers: []serverInfo{{
				existingIP: "1.2.3.4",
				newIP:      "4.6.8.9",
				id:         "test-server-1",
				ports:      []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			},
				{
					existingIP:  "2.4.6.8",
					id:          "test-server-2",
					ports:       []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
					removeLease: true,
				}},
			expectLeases: []string{"1.2.3.4"},
		},
	}
	for _, test := range stopTests {
		t.Run(test.testName, func(t *testing.T) {
			// use the same base key used by the controlplane, but add a random
			// prefix so we can reuse the etcd instance for subtests independently.
			baseKey := "/" + uuid.New().String() + "/peerserverleases/"
			s, dFunc, err := factory.Create(*sc.ForResource(schema.GroupResource{Resource: "pods"}), newFunc, newListFunc, baseKey)
			if err != nil {
				t.Fatalf("Error creating storage: %v", err)
			}
			t.Cleanup(dFunc)

			fakeReconciler := NewFakePeerEndpointReconciler(t, baseKey, s)
			err = fakeReconciler.SetKeys(test.servers)
			if err != nil {
				t.Errorf("unexpected error creating keys: %v", err)
			}
			if !test.apiServerStartup {
				fakeReconciler.StopReconciling()
			}
			for _, server := range test.servers {
				if server.removeLease {
					err = fakeReconciler.RemoveLease(server.id)
					// if the ip is not on the endpoints, it must return an storage error and stop reconciling
					if err != nil {
						t.Errorf("unexpected error reconciling: %v", err)
					}
				}
			}

			leases, err := fakeReconciler.ListLeases()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// sort for comparison
			sort.Strings(leases)
			sort.Strings(test.expectLeases)
			if !reflect.DeepEqual(leases, test.expectLeases) {
				t.Errorf("expected %v got: %v", test.expectLeases, leases)
			}
		})
	}
}
