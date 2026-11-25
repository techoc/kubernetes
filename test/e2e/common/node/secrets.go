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

package node

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/uuid"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = SIGDescribe("Secrets", func() {
	f := framework.NewDefaultFramework("secrets")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
		Release: v1.9
		Testname: Secrets, pod environment field
		Description: 创建 secret。创建一个 Pod，其容器声明了引用该 secret 的环境变量以提取某个 key 的值。Pod MUST 拥有该环境变量，且其值 MUST 为 secret 中对应 key 的正确值。
	*/
	framework.ConformanceIt("should be consumable from pods in env vars", f.WithNodeConformance(), func(ctx context.Context) {
		name := "secret-test-" + string(uuid.NewUUID())
		secret := secretForTest(f.Namespace.Name, name)

		ginkgo.By(fmt.Sprintf("Creating secret with name %s", secret.Name))
		var err error
		if secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test secret %s: %v", secret.Name, err)
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-secrets-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:    "secret-env-test",
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"sh", "-c", "env"},
						Env: []v1.EnvVar{
							{
								Name: "SECRET_DATA",
								ValueFrom: &v1.EnvVarSource{
									SecretKeyRef: &v1.SecretKeySelector{
										LocalObjectReference: v1.LocalObjectReference{
											Name: name,
										},
										Key: "data-1",
									},
								},
							},
						},
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
			},
		}

		e2epodoutput.TestContainerOutput(ctx, f, "consume secrets", pod, 0, []string{
			"SECRET_DATA=value-1",
		})
	})

	/*
		Release: v1.9
		Testname: Secrets, pod environment from source
		Description: 创建 secret。创建一个 Pod，其容器通过 'EnvFrom' 声明引用该 secret 的环境变量以提取某个 key 的值。Pod MUST 拥有该环境变量，且其值 MUST 为 secret 中对应 key 的正确值。
	*/
	framework.ConformanceIt("should be consumable via the environment", f.WithNodeConformance(), func(ctx context.Context) {
		name := "secret-test-" + string(uuid.NewUUID())
		secret := secretForTest(f.Namespace.Name, name)
		ginkgo.By(fmt.Sprintf("creating secret %v/%v", f.Namespace.Name, secret.Name))
		var err error
		if secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test secret %s: %v", secret.Name, err)
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-configmaps-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:    "env-test",
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"sh", "-c", "env"},
						EnvFrom: []v1.EnvFromSource{
							{
								SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: name}},
							},
							{
								Prefix:    "p-",
								SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: name}},
							},
						},
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
			},
		}

		e2epodoutput.TestContainerOutput(ctx, f, "consume secrets", pod, 0, []string{
			"data-1=value-1", "data-2=value-2", "data-3=value-3",
			"p-data-1=value-1", "p-data-2=value-2", "p-data-3=value-3",
		})
	})

	/*
	   Release: v1.15
	   Testname: Secrets, with empty-key
	   Description: 尝试创建带空 key 的 Secret。创建 MUST 失败。
	*/
	framework.ConformanceIt("should fail to create secret due to empty secret key", func(ctx context.Context) {
		secret, err := createEmptyKeySecretForTest(ctx, f)
		gomega.Expect(err).To(gomega.HaveOccurred(), "created secret %q with empty key in namespace %q", secret.Name, f.Namespace.Name)
	})

	/*
			   Release: v1.18
			   Testname: Secret patching
			   Description: 创建 Secret。
		           列出所有 Secret MUST 返回空列表。
		           对 Secret 打 patch 后再次获取，其字段 MUST 等于新值。
		           通过静态 Label 删除该 Secret。
		           最后再次列出 Secret，列表中 MUST NOT 包含最初创建的 Secret。
	*/
	framework.ConformanceIt("should patch a secret", func(ctx context.Context) {
		ginkgo.By("creating a secret")

		secretTestName := "test-secret-" + string(uuid.NewUUID())

		// 在测试命名空间中创建 secret
		createdSecret, err := f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: secretTestName,
				Labels: map[string]string{
					"testsecret-constant": "true",
				},
			},
			Data: map[string][]byte{
				"key": []byte("value"),
			},
			Type: "Opaque",
		}, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create secret")
		gomega.Expect(createdSecret).To(apimachineryutils.HaveValidResourceVersion())

		ginkgo.By("listing secrets in all namespaces to ensure that there are more than zero")
		// 列出所有命名空间中的 secret，以覆盖跨命名空间 list endpoint
		secretsList, err := f.ClientSet.CoreV1().Secrets("").List(ctx, metav1.ListOptions{
			LabelSelector: "testsecret-constant=true",
		})
		framework.ExpectNoError(err, "failed to list secrets")
		gomega.Expect(secretsList.Items).ToNot(gomega.BeEmpty(), "no secrets found")

		foundCreatedSecret := false
		var secretCreatedName string
		for _, val := range secretsList.Items {
			if val.ObjectMeta.Name == secretTestName && val.ObjectMeta.Namespace == f.Namespace.Name {
				foundCreatedSecret = true
				secretCreatedName = val.ObjectMeta.Name
				break
			}
		}
		if !foundCreatedSecret {
			framework.Failf("unable to find secret %s/%s by name", f.Namespace.Name, secretTestName)
		}

		ginkgo.By("patching the secret")
		// 在测试命名空间中 patch secret
		secretPatchNewData := base64.StdEncoding.EncodeToString([]byte("value1"))
		secretPatch, err := json.Marshal(map[string]interface{}{
			"metadata": map[string]interface{}{
				"labels": map[string]string{"testsecret": "true"},
			},
			"data": map[string][]byte{"key": []byte(secretPatchNewData)},
		})
		framework.ExpectNoError(err, "failed to marshal JSON")
		_, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Patch(ctx, secretCreatedName, types.StrategicMergePatchType, []byte(secretPatch), metav1.PatchOptions{})
		framework.ExpectNoError(err, "failed to patch secret")

		secret, err := f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Get(ctx, secretCreatedName, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to get secret")
		gomega.Expect(resourceversion.CompareResourceVersion(createdSecret.ResourceVersion, secret.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

		secretDecodedstring, err := base64.StdEncoding.DecodeString(string(secret.Data["key"]))
		framework.ExpectNoError(err, "failed to decode secret from Base64")

		gomega.Expect(string(secretDecodedstring)).To(gomega.Equal("value1"), "found secret, but the data wasn't updated from the patch")

		ginkgo.By("deleting the secret using a LabelSelector")
		err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
			LabelSelector: "testsecret=true",
		})
		framework.ExpectNoError(err, "failed to delete patched secret")

		ginkgo.By("listing secrets in all namespaces, searching for label name and value in patch")
		// 列出所有命名空间中的 secret
		secretsList, err = f.ClientSet.CoreV1().Secrets("").List(ctx, metav1.ListOptions{
			LabelSelector: "testsecret-constant=true",
		})
		framework.ExpectNoError(err, "failed to list secrets")

		foundCreatedSecret = false
		for _, val := range secretsList.Items {
			if val.ObjectMeta.Name == secretTestName && val.ObjectMeta.Namespace == f.Namespace.Name {
				foundCreatedSecret = true
				break
			}
		}
		if foundCreatedSecret {
			framework.Failf("secret %s/%s was not deleted successfully", f.Namespace.Name, secretTestName)
		}
	})

	/*
		Release: v1.34
		Testname: Secrets, pod environment from source
		Description: 创建使用 Secret 中的值设置环境变量的 Pod。
		允许用户通过 envFrom 使用除 '=' 外的各种可打印 ASCII 字符作为环境变量名的前缀。
		该测试验证数字、特殊字符和字母等不同前缀都能正确使用。
	*/
	framework.ConformanceIt("should be consumable as environment variable names variable names with various prefixes", func(ctx context.Context) {
		name := "secret-test-" + string(uuid.NewUUID())
		secret := secretForTest(f.Namespace.Name, name)

		ginkgo.By(fmt.Sprintf("creating secret %v/%v", f.Namespace.Name, secret.Name))
		var err error
		if secret, err = f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			framework.Failf("unable to create test secret %s: %v", secret.Name, err)
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-secret-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:    "env-test",
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"sh", "-c", "env"},
						EnvFrom: []v1.EnvFromSource{
							{
								// 无前缀
								SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: name}},
							},
							{
								// 以数字开头的前缀
								Prefix:    "1-",
								SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: name}},
							},
							{
								// 含特殊字符的前缀
								Prefix:    "$_-",
								SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: name}},
							},
							{
								// 含大写字母的前缀
								Prefix:    "ABC_",
								SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: name}},
							},
							{
								// 含符号的前缀
								Prefix:    "#@!",
								SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: name}},
							},
						},
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
			},
		}

		e2epodoutput.TestContainerOutput(ctx, f, "consume secrets", pod, 0, []string{
			// 无前缀的原始值
			"data-1=value-1", "data-2=value-2", "data-3=value-3",
			// 带数字前缀的值
			"1-data-1=value-1", "1-data-2=value-2", "1-data-3=value-3",
			// 带特殊字符前缀的值
			"$_-data-1=value-1", "$_-data-2=value-2", "$_-data-3=value-3",
			// 带大写字母前缀的值
			"ABC_data-1=value-1", "ABC_data-2=value-2", "ABC_data-3=value-3",
			// 带符号前缀的值
			"#@!data-1=value-1", "#@!data-2=value-2", "#@!data-3=value-3",
		})
	})
})

func secretForTest(namespace, name string) *v1.Secret {
	return &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: map[string][]byte{
			"data-1": []byte("value-1\n"),
			"data-2": []byte("value-2\n"),
			"data-3": []byte("value-3\n"),
		},
	}
}

func createEmptyKeySecretForTest(ctx context.Context, f *framework.Framework) (*v1.Secret, error) {
	secretName := "secret-emptykey-test-" + string(uuid.NewUUID())
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: f.Namespace.Name,
			Name:      secretName,
		},
		Data: map[string][]byte{
			"": []byte("value-1\n"),
		},
	}
	ginkgo.By(fmt.Sprintf("Creating projection with secret that has name %s", secret.Name))
	return f.ClientSet.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{})
}
