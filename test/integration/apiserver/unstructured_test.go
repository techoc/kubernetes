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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"golang.org/x/net/http2"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	appsv1applyconfigurations "k8s.io/client-go/applyconfigurations/apps/v1"
	clientset "k8s.io/client-go/kubernetes"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/framework"
)

// TestRequestObjectConvertibleToUnstructured 测试当请求对象无法转换为 unstructured 时，写请求会失败。
// 对象能够转换为 unstructured，可确保其能被编码为 JSON，
// 并确定 field manager。
func TestRequestObjectConvertibleToUnstructured(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{}, framework.SharedEtcd())
	defer server.TearDownFn()

	for i, raw := range []string{
		``,
		`"`,
		`{`,
		`[`,
		`1z`,
		`z`,
	} {
		// 必须使用 Protobuf 请求编码。无效 JSON 无法嵌入 JSON 对象或数组，
		// 否则外层对象或数组也会变成无效的。
		protoConfig := server.ClientConfig
		protoConfig.ContentConfig.ContentType = runtime.ContentTypeProtobuf
		protoConfig.ContentConfig.AcceptContentTypes = runtime.ContentTypeProtobuf
		protoClient, err := clientset.NewForConfig(protoConfig)
		if err != nil {
			t.Fatalf("unexpected error creating proto client: %v", err)
		}

		createError := new(http2.StreamError)
		if _, err := protoClient.AppsV1().ControllerRevisions("default").Create(context.TODO(), &appsv1.ControllerRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("test-revision-create-%d", i),
			},
			Data: runtime.RawExtension{Raw: []byte(raw)},
		}, metav1.CreateOptions{}); errors.As(err, createError) && createError.Code == http2.ErrCodeInternal {
			t.Logf("create returned internal error as expected with rawextension %#v: %v", raw, err)
		} else {
			t.Errorf("create returned unexpected error: %#v", err)
		}

		var marshalerError *json.MarshalerError
		if _, err := protoClient.AppsV1().ControllerRevisions("default").Apply(context.TODO(), appsv1applyconfigurations.ControllerRevision("test-revision-apply", "default").
			WithData(runtime.RawExtension{Raw: []byte(raw)}),
			metav1.ApplyOptions{}); errors.As(err, &marshalerError) {
			// 在这种情况下，错误目前发生在客户端，因为 apply 请求体必须编码为 JSON。
			// 此处包含该检查，是为了覆盖未来 apply configuration
			// 支持 Protobuf 编码的可能性。
			t.Logf("apply returned client-side marshaler error as expected with rawextension %#v: %v", raw, err)
		} else {
			t.Errorf("apply returned unexpected error: %#v", err)
		}

		// 创建一个待更新的对象。如果对象不存在，则 update 会因 "not found"
		// 提前短路，从而在本测试关心的错误出现之前
		// 就返回。
		existing, err := protoClient.AppsV1().ControllerRevisions("default").Create(context.TODO(), &appsv1.ControllerRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("test-revision-update-%d", i),
			},
			Data: runtime.RawExtension{Raw: []byte(`{}`)},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Errorf("expected nil create error, got: %v", err)
			continue
		}

		updateError := new(http2.StreamError)
		existing.Data = runtime.RawExtension{Raw: []byte(raw)}
		if _, err := protoClient.AppsV1().ControllerRevisions(existing.Namespace).Update(context.TODO(), existing, metav1.UpdateOptions{}); errors.As(err, updateError) && updateError.Code == http2.ErrCodeInternal {
			t.Logf("update returned internal error as expected with rawextension %#v: %v", raw, err)
		} else {
			t.Errorf("update returned unexpected error: %#v", err)
		}
	}
}
