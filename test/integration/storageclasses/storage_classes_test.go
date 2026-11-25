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

package storageclasses

// 本文件包含 storage classes API 资源的测试。

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/framework"
)

const provisionerPluginName = "kubernetes.io/mock-provisioner"

// TestStorageClasses 测试 apiserver 侧创建 storage class 对象以及 PVC 使用它们的行为。
func TestStorageClasses(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=StorageObjectInUseProtection"}, framework.SharedEtcd())
	defer server.TearDownFn()

	client := clientset.NewForConfigOrDie(server.ClientConfig)

	ns := framework.CreateNamespaceOrDie(client, "storageclass", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	DoTestStorageClasses(t, client, ns)
}

// DoTestStorageClasses 测试某个 API 版本的 storage classes。
func DoTestStorageClasses(t *testing.T, client clientset.Interface, ns *v1.Namespace) {
	// 创建一个 storage class 对象。
	s := storage.StorageClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "gold",
		},
		Provisioner: provisionerPluginName,
	}

	if _, err := client.StorageV1().StorageClasses().Create(context.TODO(), &s, metav1.CreateOptions{}); err != nil {
		t.Errorf("unable to create test storage class: %v", err)
	}
	defer deleteStorageClassOrErrorf(t, client, s.Namespace, s.Name)

	// 用于指定 storage class 的 PVC 模板
	classGold := "gold"
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "XXX",
			Namespace: ns.Name,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			Resources:        v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("1G")}},
			AccessModes:      []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			StorageClassName: &classGold,
		},
	}

	pvc.ObjectMeta.Name = "uses-storageclass"
	if _, err := client.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create pvc: %v", err)
	}
	defer deletePersistentVolumeClaimOrErrorf(t, client, ns.Name, pvc.Name)
}

func deleteStorageClassOrErrorf(t *testing.T, c clientset.Interface, ns, name string) {
	if err := c.StorageV1().StorageClasses().Delete(context.TODO(), name, metav1.DeleteOptions{}); err != nil {
		t.Errorf("unable to delete storage class %v: %v", name, err)
	}
}

func deletePersistentVolumeClaimOrErrorf(t *testing.T, c clientset.Interface, ns, name string) {
	if err := c.CoreV1().PersistentVolumeClaims(ns).Delete(context.TODO(), name, metav1.DeleteOptions{}); err != nil {
		t.Errorf("unable to delete persistent volume claim %v: %v", name, err)
	}
}
