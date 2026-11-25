/*
Copyright 2024 The Kubernetes Authors.

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

package apiserver

import (
	"bytes"
	"context"
	"fmt"
	"mime"
	"net/http"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/utils/ptr"
)

// TestNondeterministicResponseEncoding 验证响应体编码为 CBOR 时
// 不是确定性的。即使在某些情况下确定性编码没有额外开销，
// 也会引入一些随机性，以防止客户端在不能保证确定性编码时
// 无意中依赖它。
func TestNondeterministicResponseEncoding(t *testing.T) {
	// 非确定性的 map 键顺序并不保证以相同概率选取每一种可能的顺序。
	// 实际上，由于 metav1.WatchEvent 只有两个字段，确实如此，
	// 两种可能的 map 键顺序的概率各约为 50%。该测试因为每次试验中 watch event
	// 都以相同方式编码而出现不稳定的概率约为 2^-NTrials，
	// 因此 NTrials 需要足够大，以确保不会发生这种情况。
	const NTrials = 40

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.CBORServingAndStorage, true)
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.ClientsAllowCBOR, true)
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.ClientsPreferCBOR, true)

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	t.Cleanup(server.TearDownFn)

	config := rest.CopyConfig(server.ClientConfig)
	config.AcceptContentTypes = "application/cbor"
	client, err := corev1client.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	namespace, err := client.Namespaces().Create(context.TODO(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-nondeterministic-response-encoding",
			Annotations: map[string]string{"hello": "world"},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 比较相同 resource version 下的成对 "get" 请求，观察编码差异。
	responseDiff := false
	for i := 0; i < NTrials && !responseDiff; i++ {
		request := client.RESTClient().Get().Resource("namespaces").Name(namespace.GetName())

		// 在最新的 resource version 上执行 get
		result := request.Do(context.TODO())
		raw1, err := result.Raw()
		if err != nil {
			t.Fatal(err)
		}
		if err := result.Into(namespace); err != nil {
			t.Fatal(err)
		}

		// 在相同的 resource version 上再次执行 get
		trial := request.VersionedParams(&metav1.GetOptions{ResourceVersion: namespace.ResourceVersion}, scheme.ParameterCodec).Do(context.TODO())
		var trialObject corev1.Namespace
		if err := trial.Into(&trialObject); err != nil {
			if apierrors.IsResourceExpired(err) {
				t.Logf("retrying: %v", err)
				continue
			}
			t.Fatal(err)
		}
		if !equality.Semantic.DeepEqual(namespace, &trialObject) {
			t.Fatalf("objects differed semantically between runs:\n%s", cmp.Diff(namespace, trialObject))
		}
		raw2, err := trial.Raw()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw1, raw2) {
			// 观察到响应体与第一个响应体
			// 并非逐字节相同。
			responseDiff = true
		}
	}
	if !responseDiff {
		t.Errorf("performed %d consecutive get requests to the same resource and observed identical response bodies each time", NTrials)
	}

	// 触发一个 watch event，然后比较成对的
	// watch 请求中该事件的编码。
	eventDiff := false
	objDiff := false
	for i := 0; i < NTrials && (!eventDiff || !objDiff); i++ {
		// 先获取最新版本，以得到一个有效的 resource version 作为 watch 起点。
		namespace, err := client.Namespaces().Get(context.TODO(), namespace.GetName(), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		// 执行 patch，使 watcher 能看到一个 "modified" 事件。
		patched, err := client.Namespaces().Patch(context.TODO(), namespace.GetName(), types.JSONPatchType, []byte(fmt.Sprintf(`[{"op":"add","path":"/metadata/annotations/foo","value":"%d"}]`, i)), metav1.PatchOptions{})
		if err != nil {
			t.Fatal(err)
		}

		request := client.RESTClient().Get().Resource("namespaces")

		// 获取由 patch 触发的 watch event 的原始字节，
		// 以及其中嵌入对象的原始字节。
		getRawEventAndRawObject := func() ([]byte, []byte, error) {
			ctx, cancel := context.WithTimeout(context.TODO(), 6*time.Second)
			defer cancel()
			rc, err := request.
				VersionedParams(&metav1.ListOptions{
					ResourceVersion: namespace.ResourceVersion,
					Watch:           true,
					TimeoutSeconds:  ptr.To(int64(5)),
					FieldSelector:   fmt.Sprintf("metadata.name=%s", namespace.GetName()),
				}, scheme.ParameterCodec).
				Stream(ctx)
			if err != nil {
				return nil, nil, err
			}
			defer func() {
				if err := rc.Close(); err != nil {
					t.Error(err)
				}
			}()
			d := &rawCapturingDecoder{delegate: cbor.NewSerializer(scheme.Scheme, scheme.Scheme, cbor.Transcode(false))}
			sd := streaming.NewDecoder(rc, d)
			for {
				var event metav1.WatchEvent
				got, _, err := sd.Decode(nil, &event)
				if err != nil {
					// 如果始终没有收到预期事件，
					// 服务端超时或客户端 context 超时
					// 都会在此产生 EOF，从而终止循环。
					t.Fatal(err)
				}
				if got != &event {
					t.Fatalf("returned new object %#v (%T) instead of decoding into %T", got, got, &event)
				}
				var u map[string]interface{}
				if err := direct.Unmarshal(event.Object.Raw, &u); err != nil {
					t.Fatal(err)
				}
				rv, ok, err := unstructured.NestedString(u, "metadata", "resourceVersion")
				if err != nil {
					t.Fatalf("failed to get resourceVersion from watch event: %v", err)
				}
				if !ok {
					t.Fatal("watch event missing resource version")
				}
				if rv == patched.ResourceVersion {
					return d.raw, event.Object.Raw, nil
				}
			}
		}

		event1, raw1, err := getRawEventAndRawObject()
		if err != nil {
			if apierrors.IsResourceExpired(err) {
				t.Logf("retrying: %v", err)
				continue
			}
			t.Fatal(err)
		}
		var obj1 map[string]interface{}
		if err := direct.Unmarshal(raw1, &obj1); err != nil {
			t.Fatal(err)
		}

		event2, raw2, err := getRawEventAndRawObject()
		if err != nil {
			if apierrors.IsResourceExpired(err) {
				t.Logf("retrying: %v", err)
				continue
			}
			t.Fatal(err)
		}
		var obj2 map[string]interface{}
		if err := direct.Unmarshal(raw2, &obj2); err != nil {
			t.Fatal(err)
		}

		if !equality.Semantic.DeepEqual(obj1, obj2) {
			t.Fatalf("objects differed semantically between runs:\n%s", cmp.Diff(obj1, obj2))
		}

		if !bytes.Equal(raw1, raw2) {
			objDiff = true
		}

		// 去掉嵌入的对象，以便观察是否是 watch event 本身
		// 以非确定方式编码，而不只是嵌入了一个
		// 非确定编码的对象。
		event1 = bytes.Replace(event1, raw1, nil, 1)
		event2 = bytes.Replace(event2, raw2, nil, 1)
		if !bytes.Equal(event1, event2) {
			eventDiff = true
		}
	}
	if !eventDiff {
		t.Errorf("watch event encoded identically over %d consecutive watch requests", NTrials)
	}
	if !objDiff {
		t.Errorf("watch event embedded object encoded identically over %d consecutive watch requests", NTrials)
	}
}

type rawCapturingDecoder struct {
	raw      []byte
	delegate runtime.Decoder
}

func (d *rawCapturingDecoder) Decode(data []byte, defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	d.raw = append([]byte(nil), data...)
	return d.delegate.Decode(data, defaults, into)
}

// TestCBORNonResourceEndpoints 验证在启用 CBORServingAndStorage feature gate 时，
// 非资源结构化端点和结构化错误响应支持 CBOR 编码。这包括旧版和非旧版的发现端点、
// statusz、flagz，以及请求截止时间过滤器针对格式错误的 timeout 查询参数
// 所产生的错误响应。
func TestCBORNonResourceEndpoints(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.CBORServingAndStorage, true)
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.ClientsAllowCBOR, true)

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	t.Cleanup(server.TearDownFn)

	client, err := corev1client.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name       string
		path       string
		accept     string
		params     map[string]string
		wantStatus int
	}{
		{
			name:       "legacy discovery",
			path:       "/api",
			accept:     "application/cbor",
			wantStatus: http.StatusOK,
		},
		{
			name:       "group discovery",
			path:       "/apis",
			accept:     "application/cbor",
			wantStatus: http.StatusOK,
		},
		{
			name:       "group version discovery",
			path:       "/apis/apps",
			accept:     "application/cbor",
			wantStatus: http.StatusOK,
		},
		{
			name:       "aggregated discovery",
			path:       "/apis",
			accept:     "application/cbor;v=v2;g=apidiscovery.k8s.io;as=APIGroupDiscoveryList",
			wantStatus: http.StatusOK,
		},
		{
			name:       "statusz",
			path:       "/statusz",
			accept:     "application/cbor;v=v1beta1;g=config.k8s.io;as=Statusz",
			wantStatus: http.StatusOK,
		},
		{
			name:       "flagz",
			path:       "/flagz",
			accept:     "application/cbor;v=v1beta1;g=config.k8s.io;as=Flagz",
			wantStatus: http.StatusOK,
		},
		{
			name:       "status error from filter",
			path:       "/api/v1/namespaces",
			accept:     "application/cbor",
			params:     map[string]string{"timeout": "invalid"},
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := client.RESTClient().Get().AbsPath(tc.path).SetHeader("Accept", tc.accept)
			for k, v := range tc.params {
				req.Param(k, v)
			}

			result := req.Do(t.Context())

			var status int
			result.StatusCode(&status)
			if status != tc.wantStatus {
				t.Errorf("expected response status %d, got %d", tc.wantStatus, status)
			}

			var contentType string
			result.ContentType(&contentType)
			mediaType, _, err := mime.ParseMediaType(contentType)
			if err != nil {
				t.Fatal(err)
			}
			if mediaType != "application/cbor" {
				t.Errorf("expected Content-Type to be application/cbor, got %q", contentType)
			}

			raw, _ := result.Raw()
			if !bytes.HasPrefix(raw, []byte{0xd9, 0xd9, 0xf7}) {
				t.Errorf("response body does not begin with CBOR self-described tag (d9d9f7), got %x", raw[:min(len(raw), 16)])
			}
		})
	}
}
