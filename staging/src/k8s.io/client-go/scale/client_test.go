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

package scale

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	fakedisco "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/dynamic"
	fakerest "k8s.io/client-go/rest/fake"

	"github.com/stretchr/testify/assert"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	extv1beta1 "k8s.io/api/extensions/v1beta1"
	"k8s.io/client-go/restmapper"
	coretesting "k8s.io/client-go/testing"
)

// bytesBody 把字节切片包装成可关闭的响应体。
func bytesBody(bodyBytes []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(bodyBytes))
}

// defaultHeaders 构造 fake API Server 的标准响应头。
func defaultHeaders() http.Header {
	header := http.Header{}
	header.Set("Content-Type", runtime.ContentTypeJSON)
	return header
}

// fakeScaleClient 组装一个由 fake discovery 驱动的 RESTMapper 和 fake HTTP 客户端，
// 覆盖 core、extensions、apps 以及一个独有的 CRD 风格组，用以证明实现不依赖静态 mapper。
func fakeScaleClient(t *testing.T) (ScalesGetter, []schema.GroupResource) {
	fakeDiscoveryClient := &fakedisco.FakeDiscovery{Fake: &coretesting.Fake{}}
	fakeDiscoveryClient.Resources = []*metav1.APIResourceList{
		{
			GroupVersion: corev1.SchemeGroupVersion.String(),
			APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Kind: "Pod"},
				{Name: "replicationcontrollers", Namespaced: true, Kind: "ReplicationController"},
				{Name: "replicationcontrollers/scale", Namespaced: true, Kind: "Scale", Group: "autoscaling", Version: "v1"},
			},
		},
		{
			GroupVersion: extv1beta1.SchemeGroupVersion.String(),
			APIResources: []metav1.APIResource{
				{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
				{Name: "replicasets/scale", Namespaced: true, Kind: "Scale"},
			},
		},
		{
			GroupVersion: appsv1beta2.SchemeGroupVersion.String(),
			APIResources: []metav1.APIResource{
				{Name: "deployments", Namespaced: true, Kind: "Deployment"},
				{Name: "deployments/scale", Namespaced: true, Kind: "Scale", Group: "apps", Version: "v1beta2"},
			},
		},
		{
			GroupVersion: appsv1beta1.SchemeGroupVersion.String(),
			APIResources: []metav1.APIResource{
				{Name: "statefulsets", Namespaced: true, Kind: "StatefulSet"},
				{Name: "statefulsets/scale", Namespaced: true, Kind: "Scale", Group: "apps", Version: "v1beta1"},
			},
		},
		// 测试一个完全不存在的资源，确保没有隐式依赖任何静态 RESTMapper。
		{
			GroupVersion: "cheese.testing.k8s.io/v27alpha15",
			APIResources: []metav1.APIResource{
				{Name: "cheddars", Namespaced: true, Kind: "Cheddar"},
				{Name: "cheddars/scale", Namespaced: true, Kind: "Scale", Group: "extensions", Version: "v1beta1"},
			},
		},
	}

	restMapperRes, err := restmapper.GetAPIGroupResources(fakeDiscoveryClient)
	if err != nil {
		t.Fatalf("unexpected error while constructing resource list from fake discovery client: %v", err)
	}
	restMapper := restmapper.NewDiscoveryRESTMapper(restMapperRes)

	autoscalingScale := &autoscalingv1.Scale{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Scale",
			APIVersion: autoscalingv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
		Spec: autoscalingv1.ScaleSpec{Replicas: 10},
		Status: autoscalingv1.ScaleStatus{
			Replicas: 10,
			Selector: "foo=bar",
		},
	}
	extScale := &extv1beta1.Scale{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Scale",
			APIVersion: extv1beta1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
		Spec: extv1beta1.ScaleSpec{Replicas: 10},
		Status: extv1beta1.ScaleStatus{
			Replicas:       10,
			TargetSelector: "foo=bar",
		},
	}
	appsV1beta2Scale := &appsv1beta2.Scale{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Scale",
			APIVersion: appsv1beta2.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
		Spec: appsv1beta2.ScaleSpec{Replicas: 10},
		Status: appsv1beta2.ScaleStatus{
			Replicas:       10,
			TargetSelector: "foo=bar",
		},
	}
	appsV1beta1Scale := &appsv1beta1.Scale{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Scale",
			APIVersion: appsv1beta1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
		Spec: appsv1beta1.ScaleSpec{Replicas: 10},
		Status: appsv1beta1.ScaleStatus{
			Replicas:       10,
			TargetSelector: "foo=bar",
		},
	}

	resourcePaths := map[string]runtime.Object{
		"/api/v1/namespaces/default/replicationcontrollers/foo/scale":                  autoscalingScale,
		"/apis/extensions/v1beta1/namespaces/default/replicasets/foo/scale":            extScale,
		"/apis/apps/v1beta1/namespaces/default/statefulsets/foo/scale":                 appsV1beta1Scale,
		"/apis/apps/v1beta2/namespaces/default/deployments/foo/scale":                  appsV1beta2Scale,
		"/apis/cheese.testing.k8s.io/v27alpha15/namespaces/default/cheddars/foo/scale": extScale,
	}

	fakeReqHandler := func(req *http.Request) (*http.Response, error) {
		// 只接受已知的 scale 子资源路径，其他路径直接失败，防止测试悄悄走错分支。
		scale, isScalePath := resourcePaths[req.URL.Path]
		if !isScalePath {
			return nil, fmt.Errorf("unexpected request for URL %q with method %q", req.URL.String(), req.Method)
		}

		switch req.Method {
		case "GET":
			res, err := json.Marshal(scale)
			if err != nil {
				return nil, err
			}
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeaders(), Body: bytesBody(res)}, nil
		case "PUT":
			// 从请求体解码出 Scale，并校验其 GVK 与该资源实际使用的 scale 版本一致，
			// 这正是客户端做版本协商后应发送的格式。
			decoder := codecs.UniversalDeserializer()
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			newScale, newScaleGVK, err := decoder.Decode(body, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("unexpected request body: %v", err)
			}
			if *newScaleGVK != scale.GetObjectKind().GroupVersionKind() {
				return nil, fmt.Errorf("unexpected scale API version %s (expected %s)", newScaleGVK.String(), scale.GetObjectKind().GroupVersionKind().String())
			}
			res, err := json.Marshal(newScale)
			if err != nil {
				return nil, err
			}
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeaders(), Body: bytesBody(res)}, nil
		case "PATCH":
			// patch type 由 Content-Type 决定，MergePatch 与 JSONPatch 分别处理。
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			originScale, err := json.Marshal(scale)
			if err != nil {
				return nil, err
			}
			var res []byte
			contentType := req.Header.Get("Content-Type")
			pt := types.PatchType(contentType)
			switch pt {
			case types.MergePatchType:
				res, err = jsonpatch.MergePatch(originScale, body)
				if err != nil {
					return nil, err
				}
			case types.JSONPatchType:
				patch, err := jsonpatch.DecodePatch(body)
				if err != nil {
					return nil, err
				}
				res, err = patch.Apply(originScale)
				if err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("invalid patch type")
			}
			return &http.Response{StatusCode: http.StatusOK, Header: defaultHeaders(), Body: bytesBody(res)}, nil
		default:
			return nil, fmt.Errorf("unexpected request for URL %q with method %q", req.URL.String(), req.Method)
		}
	}

	fakeClient := &fakerest.RESTClient{
		Client:               fakerest.CreateHTTPClient(fakeReqHandler),
		NegotiatedSerializer: codecs.WithoutConversion(),
		GroupVersion:         schema.GroupVersion{},
		VersionedAPIPath:     "/not/a/real/path",
	}

	resolver := NewDiscoveryScaleKindResolver(fakeDiscoveryClient)
	// VersionedAPIPath 故意设为无效值，确保实际路径完全由 RESTMapper 推导。
	client := New(fakeClient, restMapper, dynamic.LegacyAPIPathResolverFunc, resolver)

	groupResources := []schema.GroupResource{
		{Group: corev1.GroupName, Resource: "replicationcontrollers"},
		{Group: extv1beta1.GroupName, Resource: "replicasets"},
		{Group: appsv1beta2.GroupName, Resource: "deployments"},
		{Group: "cheese.testing.k8s.io", Resource: "cheddars"},
	}

	return client, groupResources
}

// TestGetScale 验证对每个组都能 Get 到 scale，且都归一化为 autoscaling/v1 表示。
func TestGetScale(t *testing.T) {
	scaleClient, groupResources := fakeScaleClient(t)
	expectedScale := &autoscalingv1.Scale{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Scale",
			APIVersion: autoscalingv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
		Spec: autoscalingv1.ScaleSpec{Replicas: 10},
		Status: autoscalingv1.ScaleStatus{
			Replicas: 10,
			Selector: "foo=bar",
		},
	}

	for _, groupResource := range groupResources {
		scale, err := scaleClient.Scales("default").Get(context.TODO(), groupResource, "foo", metav1.GetOptions{})
		if !assert.NoError(t, err, "should have been able to fetch a scale for %s", groupResource.String()) {
			continue
		}
		assert.NotNil(t, scale, "should have returned a non-nil scale for %s", groupResource.String())

		assert.Equal(t, expectedScale, scale, "should have returned the expected scale for %s", groupResource.String())
	}
}

// TestUpdateScale 验证 Update 会按各资源自身的 scale 版本编码请求体，
// 并把响应统一转换回 autoscaling/v1。
func TestUpdateScale(t *testing.T) {
	scaleClient, groupResources := fakeScaleClient(t)
	expectedScale := &autoscalingv1.Scale{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Scale",
			APIVersion: autoscalingv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
		Spec: autoscalingv1.ScaleSpec{Replicas: 10},
		Status: autoscalingv1.ScaleStatus{
			Replicas: 10,
			Selector: "foo=bar",
		},
	}

	for _, groupResource := range groupResources {
		scale, err := scaleClient.Scales("default").Update(context.TODO(), groupResource, expectedScale, metav1.UpdateOptions{})
		if !assert.NoError(t, err, "should have been able to fetch a scale for %s", groupResource.String()) {
			continue
		}
		assert.NotNil(t, scale, "should have returned a non-nil scale for %s", groupResource.String())

		assert.Equal(t, expectedScale, scale, "should have returned the expected scale for %s", groupResource.String())
	}
}

// TestPatchScale 对 MergePatch 与 JSONPatch 两种补丁类型分别验证；
// 注意 spec.replicas 变为 5 而 status.replicas 仍为 10，符合“patch 只改 spec”的预期。
func TestPatchScale(t *testing.T) {
	scaleClient, groupResources := fakeScaleClient(t)
	expectedScale := &autoscalingv1.Scale{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Scale",
			APIVersion: autoscalingv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
		Spec: autoscalingv1.ScaleSpec{Replicas: 5},
		Status: autoscalingv1.ScaleStatus{
			Replicas: 10,
			Selector: "foo=bar",
		},
	}
	gvrs := make([]schema.GroupVersionResource, 0, len(groupResources))
	for _, gr := range groupResources {
		// Patch 需要具体版本才能构造 URL，这里按资源所属组的首选版本补全。
		switch gr.Group {
		case corev1.GroupName:
			gvrs = append(gvrs, gr.WithVersion(corev1.SchemeGroupVersion.Version))
		case extv1beta1.GroupName:
			gvrs = append(gvrs, gr.WithVersion(extv1beta1.SchemeGroupVersion.Version))
		case appsv1beta2.GroupName:
			gvrs = append(gvrs, gr.WithVersion(appsv1beta2.SchemeGroupVersion.Version))
		default:
			// 组 cheese.testing.k8s.io。
			gvrs = append(gvrs, gr.WithVersion("v27alpha15"))
		}
	}

	patch := []byte(`{"spec":{"replicas":5}}`)
	for _, gvr := range gvrs {
		scale, err := scaleClient.Scales("default").Patch(context.TODO(), gvr, "foo", types.MergePatchType, patch, metav1.PatchOptions{})
		if !assert.NoError(t, err, "should have been able to fetch a scale for %s", gvr.String()) {
			continue
		}
		assert.NotNil(t, scale, "should have returned a non-nil scale for %s", gvr.String())
		assert.Equal(t, expectedScale, scale, "should have returned the expected scale for %s", gvr.String())
	}

	patch = []byte(`[{"op":"replace","path":"/spec/replicas","value":5}]`)
	for _, gvr := range gvrs {
		scale, err := scaleClient.Scales("default").Patch(context.TODO(), gvr, "foo", types.JSONPatchType, patch, metav1.PatchOptions{})
		if !assert.NoError(t, err, "should have been able to fetch a scale for %s", gvr.String()) {
			continue
		}
		assert.NotNil(t, scale, "should have returned a non-nil scale for %s", gvr.String())
		assert.Equal(t, expectedScale, scale, "should have returned the expected scale for %s", gvr.String())
	}
}
