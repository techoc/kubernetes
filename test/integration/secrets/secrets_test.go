/*
Copyright 2015 The Kubernetes Authors.

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

package secrets

// 本文件测试 secrets API 资源的使用。

import (
	"context"
	"testing"

	"encoding/json"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration"
	"k8s.io/kubernetes/test/integration/framework"
)

func deleteSecretOrErrorf(t *testing.T, c clientset.Interface, ns, name string) {
	if err := c.CoreV1().Secrets(ns).Delete(context.TODO(), name, metav1.DeleteOptions{}); err != nil {
		t.Errorf("unable to delete secret %v: %v", name, err)
	}
}

// TestSecrets 测试 apiserver 侧创建 secret 对象以及 Pod 使用 secret 的行为。
func TestSecrets(t *testing.T) {
	// 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client := clientset.NewForConfigOrDie(server.ClientConfig)

	ns := framework.CreateNamespaceOrDie(client, "secret", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	DoTestSecrets(t, client, ns)
	DoTestSecretsImmutableWithEmptyValue(t, client, ns)
}

// DoTestSecretsImmutableWithEmptyValue 测试当 patch secret 时，
// secret data 中 map 的值为空字符串的情况下，apiserver 是否会判定 secret 数据不一致，
// 以及当 secret 的 Immutable 为 true 时，patch 请求是否会被拒绝。
func DoTestSecretsImmutableWithEmptyValue(t *testing.T, client clientset.Interface, ns *v1.Namespace) {
	// 创建一个 secret 对象。secret data 中的 map 包含空字符串值，
	// 并将 Immutable 设为 true，这样如果 apiserver 判定 patch secret 不一致，就会拒绝该 patch。
	trueVal := true
	s := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "secret",
			Namespace: ns.Name,
		},
		Immutable: &trueVal,
		Data: map[string][]byte{
			"emptyData": {},
		},
	}

	if _, err := client.CoreV1().Secrets(s.Namespace).Create(context.TODO(), &s, metav1.CreateOptions{}); err != nil {
		t.Errorf("unable to create test secret: %v", err)
	}
	defer deleteSecretOrErrorf(t, client, s.Namespace, s.Name)

	// 创建一个用于 patch 的 secret 对象。
	patchSecret := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "secret",
			Namespace: ns.Name,
			Labels: map[string]string{
				"foo": "bar",
			},
		},
		Immutable: &trueVal,
		Data: map[string][]byte{
			"emptyData": {},
		},
	}

	secretPatch, err := json.Marshal(patchSecret)
	if err != nil {
		t.Errorf("unable to marshal test secret: %v", err)
	}

	// patch secret 对象，预期 patch 成功，
	// 更详细的讨论见 #119229。
	if _, err := client.CoreV1().Secrets(s.Namespace).Patch(context.TODO(), patchSecret.Name, types.StrategicMergePatchType, secretPatch, metav1.PatchOptions{}); err != nil {
		t.Errorf("unable to patch test secret: %v", err)
	}
}

// DoTestSecrets 针对某一个 api 版本测试 secret。
func DoTestSecrets(t *testing.T, client clientset.Interface, ns *v1.Namespace) {
	// 创建一个 secret 对象。
	s := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "secret",
			Namespace: ns.Name,
		},
		Data: map[string][]byte{
			"data": []byte("value1\n"),
		},
	}

	if _, err := client.CoreV1().Secrets(s.Namespace).Create(context.TODO(), &s, metav1.CreateOptions{}); err != nil {
		t.Errorf("unable to create test secret: %v", err)
	}
	defer deleteSecretOrErrorf(t, client, s.Namespace, s.Name)

	// 使用 secret 的 Pod 模板。
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "XXX",
			Namespace: ns.Name,
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "secvol",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: "secret",
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					Name:  "fake-name",
					Image: "fakeimage",
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "secvol",
							MountPath: "/fake/path",
							ReadOnly:  true,
						},
					},
				},
			},
		},
	}

	// 创建一个使用该 secret 的 Pod。
	pod.ObjectMeta.Name = "uses-secret"
	if _, err := client.CoreV1().Pods(ns.Name).Create(context.TODO(), pod, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create pod: %v", err)
	}
	defer integration.DeletePodOrErrorf(t, client, ns.Name, pod.Name)

	// 创建一个使用不存在 secret 的 Pod。
	pod.ObjectMeta.Name = "uses-non-existent-secret"
	if _, err := client.CoreV1().Pods(ns.Name).Create(context.TODO(), pod, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create pod: %v", err)
	}
	defer integration.DeletePodOrErrorf(t, client, ns.Name, pod.Name)
	// 该 Pod 可能无法运行，但目前我们不会阻止这种情况，
	// 并且本测试也无法检查 kubelet 是否真的拉取了 secret。

	// 校验 volume 的内容不属于 apiserver<->kubelet 集成测试的范畴。
	// 这部分由 e2e 测试覆盖。
}
