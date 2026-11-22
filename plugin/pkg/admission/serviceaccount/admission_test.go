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

package serviceaccount

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/admission"
	admissiontesting "k8s.io/apiserver/pkg/admission/testing"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	api "k8s.io/kubernetes/pkg/apis/core"
	v1defaults "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/controller"
	kubelet "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/utils/ptr"
)

// TestIgnoresNonCreate 验证插件只处理创建操作，删除和连接请求不予处理。
func TestIgnoresNonCreate(t *testing.T) {
	for _, op := range []admission.Operation{admission.Delete, admission.Connect} {
		handler := NewServiceAccount()
		if handler.Handles(op) {
			t.Errorf("Expected not to handle operation %s", op)
		}
	}
}

// TestIgnoresNonPodResource 验证非 Pod 资源即使携带 Pod 对象也会被放行，插件按资源类型判定。
func TestIgnoresNonPodResource(t *testing.T) {
	pod := &api.Pod{}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), "myns", "myname", api.Resource("CustomResource").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	handler := admissiontesting.WithReinvocationTesting(t, NewServiceAccount())
	err := handler.Admit(context.TODO(), attrs, nil)
	if err != nil {
		t.Errorf("Expected non-pod resource allowed, got err: %v", err)
	}
}

// TestIgnoresNilObject 验证对象为空时插件放行，不因取不到 Pod 而报错。
func TestIgnoresNilObject(t *testing.T) {
	attrs := admission.NewAttributesRecord(nil, nil, api.Kind("Pod").WithVersion("version"), "myns", "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	handler := admissiontesting.WithReinvocationTesting(t, NewServiceAccount())
	err := handler.Admit(context.TODO(), attrs, nil)
	if err != nil {
		t.Errorf("Expected nil object allowed allowed, got err: %v", err)
	}
}

// TestIgnoresNonPodObject 验证对象类型不是 Pod 时插件放行，不对其做 ServiceAccount 处理。
func TestIgnoresNonPodObject(t *testing.T) {
	obj := &api.Namespace{}
	attrs := admission.NewAttributesRecord(obj, nil, api.Kind("Pod").WithVersion("version"), "myns", "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	handler := admissiontesting.WithReinvocationTesting(t, NewServiceAccount())
	err := handler.Admit(context.TODO(), attrs, nil)
	if err != nil {
		t.Errorf("Expected non pod object allowed, got err: %v", err)
	}
}

// TestIgnoresMirrorPod 验证不带 ServiceAccount 和 Secret 的静态 Pod 被放行。
func TestIgnoresMirrorPod(t *testing.T) {
	pod := &api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				kubelet.ConfigMirrorAnnotationKey: "true",
			},
		},
		Spec: api.PodSpec{
			Volumes: []api.Volume{
				{VolumeSource: api.VolumeSource{}},
			},
		},
	}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), "myns", "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, NewServiceAccount()).Admit(context.TODO(), attrs, nil)
	if err != nil {
		t.Errorf("Expected mirror pod without service account or secrets allowed, got err: %v", err)
	}
}

// TestRejectsMirrorPodWithServiceAccount 验证静态 Pod 引用 ServiceAccount 时被拒绝。
func TestRejectsMirrorPodWithServiceAccount(t *testing.T) {
	pod := &api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				kubelet.ConfigMirrorAnnotationKey: "true",
			},
		},
		Spec: api.PodSpec{
			ServiceAccountName: "default",
		},
	}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), "myns", "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, NewServiceAccount()).Admit(context.TODO(), attrs, nil)
	if err == nil {
		t.Errorf("Expected a mirror pod to be prevented from referencing a service account")
	}
}

// TestRejectsMirrorPodWithSecretVolumes 验证静态 Pod 挂载 Secret 卷时被拒绝。
func TestRejectsMirrorPodWithSecretVolumes(t *testing.T) {
	pod := &api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				kubelet.ConfigMirrorAnnotationKey: "true",
			},
		},
		Spec: api.PodSpec{
			Volumes: []api.Volume{
				{VolumeSource: api.VolumeSource{Secret: &api.SecretVolumeSource{SecretName: "mysecret"}}},
			},
		},
	}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), "myns", "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, NewServiceAccount()).Admit(context.TODO(), attrs, nil)
	if err == nil {
		t.Errorf("Expected a mirror pod to be prevented from referencing a secret volume")
	}
}

// TestRejectsMirrorPodWithServiceAccountTokenVolumeProjections 验证静态 Pod 投影 ServiceAccountToken 卷时被拒绝。
func TestRejectsMirrorPodWithServiceAccountTokenVolumeProjections(t *testing.T) {
	pod := &api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				kubelet.ConfigMirrorAnnotationKey: "true",
			},
		},
		Spec: api.PodSpec{
			Volumes: []api.Volume{
				{VolumeSource: api.VolumeSource{
					Projected: &api.ProjectedVolumeSource{
						Sources: []api.VolumeProjection{{ServiceAccountToken: &api.ServiceAccountTokenProjection{}}},
					},
				},
				},
			},
		},
	}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), "myns", "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, NewServiceAccount()).Admit(context.TODO(), attrs, nil)
	if err == nil {
		t.Errorf("Expected a mirror pod to be prevented from referencing a ServiceAccountToken volume projection")
	}
}

// TestAssignsDefaultServiceAccountAndBoundTokenWithNoSecretTokens 验证无 Secret 令牌的命名空间里 Pod 被分配默认 ServiceAccount，
// 并挂载投影令牌、根证书与命名空间卷，且转换回 v1 后与默认值一致。
func TestAssignsDefaultServiceAccountAndBoundTokenWithNoSecretTokens(t *testing.T) {
	ns := "myns"

	admit := NewServiceAccount()
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)
	admit.MountServiceAccountToken = true

	// 预先把默认 ServiceAccount 放进 informer 缓存，避免插件去查 API 而失败。
	informerFactory.Core().V1().ServiceAccounts().Informer().GetStore().Add(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultServiceAccountName,
			Namespace: ns,
		},
	})

	v1PodIn := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{}},
		},
	}
	v1defaults.SetObjectDefaults_Pod(v1PodIn)
	pod := &api.Pod{}
	if err := v1defaults.Convert_v1_Pod_To_core_Pod(v1PodIn, pod, nil); err != nil {
		t.Fatal(err)
	}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil)
	if err != nil {
		t.Fatalf("Expected success, got: %v", err)
	}

	expectedVolumes := []api.Volume{{
		Name: "cleared",
		VolumeSource: api.VolumeSource{
			Projected: &api.ProjectedVolumeSource{
				Sources: []api.VolumeProjection{
					{ServiceAccountToken: &api.ServiceAccountTokenProjection{ExpirationSeconds: ptr.To[int64](3607), Path: "token"}},
					{ConfigMap: &api.ConfigMapProjection{LocalObjectReference: api.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []api.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
					{DownwardAPI: &api.DownwardAPIProjection{Items: []api.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &api.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
				},
				DefaultMode: ptr.To[int32](0644),
			},
		},
	}}
	expectedVolumeMounts := []api.VolumeMount{{
		Name:      "cleared",
		ReadOnly:  true,
		MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
	}}

	// 卷名由插件生成、每次可能不同，统一抹掉后才能与期望值比较。
	for i := range pod.Spec.Volumes {
		if len(pod.Spec.Volumes[i].Name) > 0 {
			pod.Spec.Volumes[i].Name = "cleared"
		}
	}
	for i := range pod.Spec.Containers[0].VolumeMounts {
		if len(pod.Spec.Containers[0].VolumeMounts[i].Name) > 0 {
			pod.Spec.Containers[0].VolumeMounts[i].Name = "cleared"
		}
	}

	if !reflect.DeepEqual(expectedVolumes, pod.Spec.Volumes) {
		t.Errorf("unexpected volumes: %s", cmp.Diff(expectedVolumes, pod.Spec.Volumes))
	}
	if !reflect.DeepEqual(expectedVolumeMounts, pod.Spec.Containers[0].VolumeMounts) {
		t.Errorf("unexpected volumes: %s", cmp.Diff(expectedVolumeMounts, pod.Spec.Containers[0].VolumeMounts))
	}

	// 结果转回 v1 后应等同于原生默认值对象，确保插件没有打破 v1 默认化语义。
	v1PodOut := &corev1.Pod{}
	if err := v1defaults.Convert_core_Pod_To_v1_Pod(pod, v1PodOut, nil); err != nil {
		t.Fatal(err)
	}
	v1PodOutDefaulted := v1PodOut.DeepCopy()
	v1defaults.SetObjectDefaults_Pod(v1PodOutDefaulted)
	if !reflect.DeepEqual(v1PodOut, v1PodOutDefaulted) {
		t.Error(cmp.Diff(v1PodOut, v1PodOutDefaulted))
	}
}

// TestFetchesUncachedServiceAccount 验证缓存未命中时插件会通过客户端拉取默认 ServiceAccount。
func TestFetchesUncachedServiceAccount(t *testing.T) {
	ns := "myns"

	// 造一个测试客户端，供插件查询缓存里没有的 ServiceAccount。
	client := fake.NewSimpleClientset(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultServiceAccountName,
			Namespace: ns,
		},
	})

	admit := NewServiceAccount()
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)
	admit.client = client

	pod := &api.Pod{}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if pod.Spec.ServiceAccountName != DefaultServiceAccountName {
		t.Errorf("Expected service account %s assigned, got %s", DefaultServiceAccountName, pod.Spec.ServiceAccountName)
	}
}

// TestDeniesInvalidServiceAccount 验证默认 ServiceAccount 不存在时创建 Pod 被拒绝。
func TestDeniesInvalidServiceAccount(t *testing.T) {
	ns := "myns"

	// 造一个测试客户端，供插件查询缓存里没有的 ServiceAccount。
	client := fake.NewSimpleClientset()

	admit := NewServiceAccount()
	admit.SetExternalKubeClientSet(client)
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)

	pod := &api.Pod{}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil)
	if err == nil {
		t.Errorf("Expected error for missing service account, got none")
	}
}

// TestAutomountsAPIToken 验证开启自动挂载时普通容器和初始化容器都会挂载 API 令牌卷。
func TestAutomountsAPIToken(t *testing.T) {

	admit := NewServiceAccount()
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)
	admit.generateName = testGenerateName
	admit.MountServiceAccountToken = true

	ns := "myns"
	serviceAccountName := DefaultServiceAccountName
	serviceAccountUID := "12345"

	tokenName := generatedVolumeName

	expectedVolume := api.Volume{
		Name: tokenName,
		VolumeSource: api.VolumeSource{
			Projected: TokenVolumeSource(),
		},
	}
	expectedVolumeMount := api.VolumeMount{
		Name:      tokenName,
		ReadOnly:  true,
		MountPath: DefaultAPITokenMountPath,
	}
	// 缓存中放入带 UID 的默认 ServiceAccount，插件据此生成令牌卷。
	informerFactory.Core().V1().ServiceAccounts().Informer().GetStore().Add(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceAccountName,
			Namespace: ns,
			UID:       types.UID(serviceAccountUID),
		},
	})

	pod := &api.Pod{
		Spec: api.PodSpec{
			Containers: []api.Container{
				{},
			},
		},
	}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if pod.Spec.ServiceAccountName != DefaultServiceAccountName {
		t.Errorf("Expected service account %s assigned, got %s", DefaultServiceAccountName, pod.Spec.ServiceAccountName)
	}
	if len(pod.Spec.Volumes) != 1 {
		t.Fatalf("Expected 1 volume, got %d", len(pod.Spec.Volumes))
	}
	if !reflect.DeepEqual(expectedVolume, pod.Spec.Volumes[0]) {
		t.Fatalf("Expected\n\t%#v\ngot\n\t%#v", expectedVolume, pod.Spec.Volumes[0])
	}
	if len(pod.Spec.Containers[0].VolumeMounts) != 1 {
		t.Fatalf("Expected 1 volume mount, got %d", len(pod.Spec.Containers[0].VolumeMounts))
	}
	if !reflect.DeepEqual(expectedVolumeMount, pod.Spec.Containers[0].VolumeMounts[0]) {
		t.Fatalf("Expected\n\t%#v\ngot\n\t%#v", expectedVolumeMount, pod.Spec.Containers[0].VolumeMounts[0])
	}

	// 初始化容器同样需要令牌挂载，单独跑一遍确认逻辑一致。
	pod = &api.Pod{
		Spec: api.PodSpec{
			InitContainers: []api.Container{
				{},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if pod.Spec.ServiceAccountName != DefaultServiceAccountName {
		t.Errorf("Expected service account %s assigned, got %s", DefaultServiceAccountName, pod.Spec.ServiceAccountName)
	}
	if len(pod.Spec.Volumes) != 1 {
		t.Fatalf("Expected 1 volume, got %d", len(pod.Spec.Volumes))
	}
	if !reflect.DeepEqual(expectedVolume, pod.Spec.Volumes[0]) {
		t.Fatalf("Expected\n\t%#v\ngot\n\t%#v", expectedVolume, pod.Spec.Volumes[0])
	}
	if len(pod.Spec.InitContainers[0].VolumeMounts) != 1 {
		t.Fatalf("Expected 1 volume mount, got %d", len(pod.Spec.InitContainers[0].VolumeMounts))
	}
	if !reflect.DeepEqual(expectedVolumeMount, pod.Spec.InitContainers[0].VolumeMounts[0]) {
		t.Fatalf("Expected\n\t%#v\ngot\n\t%#v", expectedVolumeMount, pod.Spec.InitContainers[0].VolumeMounts[0])
	}
}

// TestRespectsExistingMount 验证容器已自行挂载令牌路径时插件不再重复创建卷。
func TestRespectsExistingMount(t *testing.T) {
	ns := "myns"
	serviceAccountName := DefaultServiceAccountName
	serviceAccountUID := "12345"

	expectedVolumeMount := api.VolumeMount{
		Name:      "my-custom-mount",
		ReadOnly:  false,
		MountPath: DefaultAPITokenMountPath,
	}

	admit := NewServiceAccount()
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)
	admit.MountServiceAccountToken = true

	// 缓存中放入带 UID 的默认 ServiceAccount，插件据此生成令牌卷。
	informerFactory.Core().V1().ServiceAccounts().Informer().GetStore().Add(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceAccountName,
			Namespace: ns,
			UID:       types.UID(serviceAccountUID),
		},
	})

	// 构造一个容器已在令牌路径挂载卷的 Pod：插件应尊重现有挂载，
	// 且没有容器需要令牌时不创建多余的卷。
	pod := &api.Pod{
		Spec: api.PodSpec{
			Containers: []api.Container{
				{
					VolumeMounts: []api.VolumeMount{
						expectedVolumeMount,
					},
				},
			},
		},
	}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if pod.Spec.ServiceAccountName != DefaultServiceAccountName {
		t.Errorf("Expected service account %s assigned, got %s", DefaultServiceAccountName, pod.Spec.ServiceAccountName)
	}
	if len(pod.Spec.Volumes) != 0 {
		t.Fatalf("Expected 0 volumes (shouldn't create a volume for a secret we don't need), got %d", len(pod.Spec.Volumes))
	}
	if len(pod.Spec.Containers[0].VolumeMounts) != 1 {
		t.Fatalf("Expected 1 volume mount, got %d", len(pod.Spec.Containers[0].VolumeMounts))
	}
	if !reflect.DeepEqual(expectedVolumeMount, pod.Spec.Containers[0].VolumeMounts[0]) {
		t.Fatalf("Expected\n\t%#v\ngot\n\t%#v", expectedVolumeMount, pod.Spec.Containers[0].VolumeMounts[0])
	}

	// 初始化容器也需要同样的尊重现有挂载逻辑。
	pod = &api.Pod{
		Spec: api.PodSpec{
			InitContainers: []api.Container{
				{
					VolumeMounts: []api.VolumeMount{
						expectedVolumeMount,
					},
				},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if pod.Spec.ServiceAccountName != DefaultServiceAccountName {
		t.Errorf("Expected service account %s assigned, got %s", DefaultServiceAccountName, pod.Spec.ServiceAccountName)
	}
	if len(pod.Spec.Volumes) != 0 {
		t.Fatalf("Expected 0 volumes (shouldn't create a volume for a secret we don't need), got %d", len(pod.Spec.Volumes))
	}
	if len(pod.Spec.InitContainers[0].VolumeMounts) != 1 {
		t.Fatalf("Expected 1 volume mount, got %d", len(pod.Spec.InitContainers[0].VolumeMounts))
	}
	if !reflect.DeepEqual(expectedVolumeMount, pod.Spec.InitContainers[0].VolumeMounts[0]) {
		t.Fatalf("Expected\n\t%#v\ngot\n\t%#v", expectedVolumeMount, pod.Spec.InitContainers[0].VolumeMounts[0])
	}
}

// TestAllowsReferencedSecret 验证 ServiceAccount 引用的 Secret 可用于卷、环境变量及临时容器等场景。
func TestAllowsReferencedSecret(t *testing.T) {
	ns := "myns"

	admit := NewServiceAccount()
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)
	admit.LimitSecretReferences = true

	// 缓存中的默认 ServiceAccount 引用了 foo，下面各用例才应被放行。
	informerFactory.Core().V1().ServiceAccounts().Informer().GetStore().Add(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultServiceAccountName,
			Namespace: ns,
		},
		Secrets: []corev1.ObjectReference{
			{Name: "foo"},
		},
	})

	pod1 := &api.Pod{
		Spec: api.PodSpec{
			Volumes: []api.Volume{
				{VolumeSource: api.VolumeSource{Secret: &api.SecretVolumeSource{SecretName: "foo"}}},
			},
		},
	}
	attrs := admission.NewAttributesRecord(pod1, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	pod2 := &api.Pod{
		Spec: api.PodSpec{
			Containers: []api.Container{
				{
					Name: "container-1",
					Env: []api.EnvVar{
						{
							Name: "env-1",
							ValueFrom: &api.EnvVarSource{
								SecretKeyRef: &api.SecretKeySelector{
									LocalObjectReference: api.LocalObjectReference{Name: "foo"},
								},
							},
						},
					},
				},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	pod2 = &api.Pod{
		Spec: api.PodSpec{
			Containers: []api.Container{
				{
					Name: "container-1",
					EnvFrom: []api.EnvFromSource{
						{
							SecretRef: &api.SecretEnvSource{
								LocalObjectReference: api.LocalObjectReference{
									Name: "foo"}}}},
				},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	pod2 = &api.Pod{
		Spec: api.PodSpec{
			InitContainers: []api.Container{
				{
					Name: "container-1",
					Env: []api.EnvVar{
						{
							Name: "env-1",
							ValueFrom: &api.EnvVarSource{
								SecretKeyRef: &api.SecretKeySelector{
									LocalObjectReference: api.LocalObjectReference{Name: "foo"},
								},
							},
						},
					},
				},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	pod2 = &api.Pod{
		Spec: api.PodSpec{
			InitContainers: []api.Container{
				{
					Name: "container-1",
					EnvFrom: []api.EnvFromSource{
						{
							SecretRef: &api.SecretEnvSource{
								LocalObjectReference: api.LocalObjectReference{
									Name: "foo"}}}},
				},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	pod2 = &api.Pod{
		Spec: api.PodSpec{
			ServiceAccountName: DefaultServiceAccountName,
			EphemeralContainers: []api.EphemeralContainer{
				{
					EphemeralContainerCommon: api.EphemeralContainerCommon{
						Name: "container-2",
						Env: []api.EnvVar{
							{
								Name: "env-1",
								ValueFrom: &api.EnvVarSource{
									SecretKeyRef: &api.SecretKeySelector{
										LocalObjectReference: api.LocalObjectReference{Name: "foo"},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	// Validate 在 create 且无子资源、或 update 且子资源为 ephemeralcontainers 时才校验 Secret 引用，此处走后者。
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "ephemeralcontainers", admission.Update, &metav1.UpdateOptions{}, false, nil)
	if err := admit.Validate(context.TODO(), attrs, nil); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	pod2 = &api.Pod{
		Spec: api.PodSpec{
			ServiceAccountName: DefaultServiceAccountName,
			EphemeralContainers: []api.EphemeralContainer{
				{
					EphemeralContainerCommon: api.EphemeralContainerCommon{
						Name: "container-2",
						EnvFrom: []api.EnvFromSource{{
							SecretRef: &api.SecretEnvSource{
								LocalObjectReference: api.LocalObjectReference{
									Name: "foo"}}}},
					},
				},
			},
		},
	}
	// 临时容器的 update 请求同样受 Validate 的 Secret 引用限制约束。
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "ephemeralcontainers", admission.Update, &metav1.UpdateOptions{}, false, nil)
	if err := admit.Validate(context.TODO(), attrs, nil); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestRejectsUnreferencedSecretVolumes 验证使用 ServiceAccount 未引用的 Secret 会被拒绝，
// 且 Admit/Validate 各自只在对应操作与子资源下执行校验。
func TestRejectsUnreferencedSecretVolumes(t *testing.T) {
	ns := "myns"

	admit := NewServiceAccount()
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)
	admit.LimitSecretReferences = true

	// 默认 ServiceAccount 不引用任何 Secret，下面对 foo 的使用都应被拒绝。
	informerFactory.Core().V1().ServiceAccounts().Informer().GetStore().Add(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultServiceAccountName,
			Namespace: ns,
		},
	})

	pod1 := &api.Pod{
		Spec: api.PodSpec{
			Volumes: []api.Volume{
				{VolumeSource: api.VolumeSource{Secret: &api.SecretVolumeSource{SecretName: "foo"}}},
			},
		},
	}
	attrs := admission.NewAttributesRecord(pod1, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err == nil {
		t.Errorf("Expected rejection for using a secret the service account does not reference")
	}

	pod2 := &api.Pod{
		Spec: api.PodSpec{
			Containers: []api.Container{
				{
					Name: "container-1",
					Env: []api.EnvVar{
						{
							Name: "env-1",
							ValueFrom: &api.EnvVarSource{
								SecretKeyRef: &api.SecretKeySelector{
									LocalObjectReference: api.LocalObjectReference{Name: "foo"},
								},
							},
						},
					},
				},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err == nil || !strings.Contains(err.Error(), "with envVar") {
		t.Errorf("Unexpected error: %v", err)
	}

	pod2 = &api.Pod{
		Spec: api.PodSpec{
			Containers: []api.Container{
				{
					Name: "container-1",
					EnvFrom: []api.EnvFromSource{
						{
							SecretRef: &api.SecretEnvSource{
								LocalObjectReference: api.LocalObjectReference{
									Name: "foo"}}}},
				},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err == nil || !strings.Contains(err.Error(), "with envFrom") {
		t.Errorf("Unexpected error: %v", err)
	}

	pod2 = &api.Pod{
		Spec: api.PodSpec{
			ServiceAccountName: DefaultServiceAccountName,
			InitContainers: []api.Container{
				{
					Name: "container-1",
					Env: []api.EnvVar{
						{
							Name: "env-1",
							ValueFrom: &api.EnvVarSource{
								SecretKeyRef: &api.SecretKeySelector{
									LocalObjectReference: api.LocalObjectReference{Name: "foo"},
								},
							},
						},
					},
				},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err != nil {
		t.Errorf("admit only enforces restrictions on secret mounts when operation==create. Unexpected error: %v", err)
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admit.Validate(context.TODO(), attrs, nil); err == nil || !strings.Contains(err.Error(), "with envVar") {
		t.Errorf("validate only enforces restrictions on secret mounts when operation==create and subresource==''. Unexpected error: %v", err)
	}

	pod2 = &api.Pod{
		Spec: api.PodSpec{
			ServiceAccountName: DefaultServiceAccountName,
			InitContainers: []api.Container{
				{
					Name: "container-1",
					EnvFrom: []api.EnvFromSource{
						{
							SecretRef: &api.SecretEnvSource{
								LocalObjectReference: api.LocalObjectReference{
									Name: "foo"}}}},
				},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err != nil {
		t.Errorf("admit only enforces restrictions on secret mounts when operation==create. Unexpected error: %v", err)
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	if err := admit.Validate(context.TODO(), attrs, nil); err == nil || !strings.Contains(err.Error(), "with envFrom") {
		t.Errorf("validate only enforces restrictions on secret mounts when operation==create and subresource==''. Unexpected error: %v", err)
	}

	pod2 = &api.Pod{
		Spec: api.PodSpec{
			ServiceAccountName: DefaultServiceAccountName,
			EphemeralContainers: []api.EphemeralContainer{
				{
					EphemeralContainerCommon: api.EphemeralContainerCommon{
						Name: "container-2",
						Env: []api.EnvVar{
							{
								Name: "env-1",
								ValueFrom: &api.EnvVarSource{
									SecretKeyRef: &api.SecretKeySelector{
										LocalObjectReference: api.LocalObjectReference{Name: "foo"},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{}, false, nil)
	if err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil); err != nil {
		t.Errorf("admit only enforces restrictions on secret mounts when operation==create and subresource==''. Unexpected error: %v", err)
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "ephemeralcontainers", admission.Update, &metav1.UpdateOptions{}, false, nil)
	if err := admit.Validate(context.TODO(), attrs, nil); err == nil || !strings.Contains(err.Error(), "with envVar") {
		t.Errorf("validate enforces restrictions on secret mounts when operation==update and subresource==ephemeralcontainers. Unexpected error: %v", err)
	}

	pod2 = &api.Pod{
		Spec: api.PodSpec{
			ServiceAccountName: DefaultServiceAccountName,
			EphemeralContainers: []api.EphemeralContainer{
				{
					EphemeralContainerCommon: api.EphemeralContainerCommon{
						Name: "container-2",
						EnvFrom: []api.EnvFromSource{{
							SecretRef: &api.SecretEnvSource{
								LocalObjectReference: api.LocalObjectReference{
									Name: "foo"}}}},
					},
				},
			},
		},
	}
	attrs = admission.NewAttributesRecord(pod2, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "ephemeralcontainers", admission.Update, &metav1.UpdateOptions{}, false, nil)
	if err := admit.Validate(context.TODO(), attrs, nil); err == nil || !strings.Contains(err.Error(), "with envFrom") {
		t.Errorf("validate enforces restrictions on secret mounts when operation==update and subresource==ephemeralcontainers. Unexpected error: %v", err)
	}
}

// TestAllowUnreferencedSecretVolumesForPermissiveSAs 验证显式开启强制限制的 ServiceAccount 注解后，
// 即使全局未开启限制，未被引用的 Secret 仍会被拒绝。
func TestAllowUnreferencedSecretVolumesForPermissiveSAs(t *testing.T) {
	ns := "myns"

	admit := NewServiceAccount()
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)
	admit.LimitSecretReferences = false

	// 注解按 SA 粒度开启强制限制，覆盖全局 LimitSecretReferences=false 的配置。
	informerFactory.Core().V1().ServiceAccounts().Informer().GetStore().Add(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:        DefaultServiceAccountName,
			Namespace:   ns,
			Annotations: map[string]string{EnforceMountableSecretsAnnotation: "true"},
		},
	})

	pod := &api.Pod{
		Spec: api.PodSpec{
			Volumes: []api.Volume{
				{VolumeSource: api.VolumeSource{Secret: &api.SecretVolumeSource{SecretName: "foo"}}},
			},
		},
	}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil)
	if err == nil {
		t.Errorf("Expected rejection for using a secret the service account does not reference")
	}
}

// TestAllowsReferencedImagePullSecrets 验证 Pod 使用 ServiceAccount 引用的镜像拉取 Secret 时被放行。
func TestAllowsReferencedImagePullSecrets(t *testing.T) {
	ns := "myns"

	admit := NewServiceAccount()
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)
	admit.LimitSecretReferences = true

	// 缓存中的默认 ServiceAccount 引用了 foo 作为镜像拉取 Secret。
	informerFactory.Core().V1().ServiceAccounts().Informer().GetStore().Add(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultServiceAccountName,
			Namespace: ns,
		},
		ImagePullSecrets: []corev1.LocalObjectReference{
			{Name: "foo"},
		},
	})

	pod := &api.Pod{
		Spec: api.PodSpec{
			ImagePullSecrets: []api.LocalObjectReference{{Name: "foo"}},
		},
	}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestRejectsUnreferencedImagePullSecrets 验证 Pod 使用 ServiceAccount 未引用的镜像拉取 Secret 时被拒绝。
func TestRejectsUnreferencedImagePullSecrets(t *testing.T) {
	ns := "myns"

	admit := NewServiceAccount()
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)
	admit.LimitSecretReferences = true

	// 默认 ServiceAccount 无镜像拉取 Secret，Pod 引用 foo 应被拒绝。
	informerFactory.Core().V1().ServiceAccounts().Informer().GetStore().Add(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultServiceAccountName,
			Namespace: ns,
		},
	})

	pod := &api.Pod{
		Spec: api.PodSpec{
			ImagePullSecrets: []api.LocalObjectReference{{Name: "foo"}},
		},
	}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil)
	if err == nil {
		t.Errorf("Expected rejection for using a secret the service account does not reference")
	}
}

// TestDoNotAddImagePullSecrets 验证 Pod 已指定镜像拉取 Secret 时不会从 ServiceAccount 追加其他项。
func TestDoNotAddImagePullSecrets(t *testing.T) {
	ns := "myns"

	admit := NewServiceAccount()
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)
	admit.LimitSecretReferences = true

	// SA 有 foo 和 bar 两个拉取 Secret，但 Pod 已填了 foo，不应再补 bar。
	informerFactory.Core().V1().ServiceAccounts().Informer().GetStore().Add(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultServiceAccountName,
			Namespace: ns,
		},
		ImagePullSecrets: []corev1.LocalObjectReference{
			{Name: "foo"},
			{Name: "bar"},
		},
	})

	pod := &api.Pod{
		Spec: api.PodSpec{
			ImagePullSecrets: []api.LocalObjectReference{{Name: "foo"}},
		},
	}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(pod.Spec.ImagePullSecrets) != 1 || pod.Spec.ImagePullSecrets[0].Name != "foo" {
		t.Errorf("unexpected image pull secrets: %v", pod.Spec.ImagePullSecrets)
	}
}

// TestAddImagePullSecrets 验证 Pod 未指定时会继承 ServiceAccount 的镜像拉取 Secret，且不改动缓存中的 SA 对象。
func TestAddImagePullSecrets(t *testing.T) {
	ns := "myns"

	admit := NewServiceAccount()
	informerFactory := informers.NewSharedInformerFactory(nil, controller.NoResyncPeriodFunc())
	admit.SetExternalKubeInformerFactory(informerFactory)
	admit.LimitSecretReferences = true

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultServiceAccountName,
			Namespace: ns,
		},
		ImagePullSecrets: []corev1.LocalObjectReference{
			{Name: "foo"},
			{Name: "bar"},
		},
	}
	originalSA := sa.DeepCopy()
	expected := []api.LocalObjectReference{
		{Name: "foo"},
		{Name: "bar"},
	}
	// 放入缓存后插件从 informer 取到的是同一对象，后续修改 Pod 不应污染它。
	informerFactory.Core().V1().ServiceAccounts().Informer().GetStore().Add(sa)

	pod := &api.Pod{}
	attrs := admission.NewAttributesRecord(pod, nil, api.Kind("Pod").WithVersion("version"), ns, "myname", api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.CreateOptions{}, false, nil)
	err := admissiontesting.WithReinvocationTesting(t, admit).Admit(context.TODO(), attrs, nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assert.EqualValues(t, expected, pod.Spec.ImagePullSecrets, "expected %v, got %v", expected, pod.Spec.ImagePullSecrets)

	// 改动 Pod 切片以暴露插件与 SA 共享底层数组的潜在 bug。
	pod.Spec.ImagePullSecrets[1] = api.LocalObjectReference{Name: "baz"}
	if !reflect.DeepEqual(originalSA, sa) {
		t.Errorf("accidentally mutated the ServiceAccount.ImagePullSecrets: %v", sa.ImagePullSecrets)
	}
}

// testGenerateName 生成可预测的卷名，便于断言生成的卷名格式。
func testGenerateName(n string) string {
	return n + "abc123"
}

// generatedVolumeName 是 testGenerateName 对标准卷名前缀的确定性结果，供断言复用。
var generatedVolumeName = testGenerateName(ServiceAccountVolumeName + "-")
