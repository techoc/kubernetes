/*
Copyright 2021 The Kubernetes Authors.

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

// Package fixtures 提供集成测试所需的服务器启动、CRD 与实例构造辅助函数。
package fixtures

import (
	"context"
	"encoding/json"
	"path"

	clientv3 "go.etcd.io/etcd/client/v3"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/dynamic"
)

// CreateCRDUsingRemovedAPI 直接通过 etcd 创建 CRD。只能用于验证与已移除数据的兼容性。
// 不要仅仅因为不想把测试改用 v1 就用它；只有在确实必要时才使用。
func CreateCRDUsingRemovedAPI(etcdClient *clientv3.Client, etcdStoragePrefix string, betaCRD *apiextensionsv1beta1.CustomResourceDefinition, apiExtensionsClient clientset.Interface, dynamicClientSet dynamic.Interface) (*apiextensionsv1.CustomResourceDefinition, error) {
	crd, err := CreateCRDUsingRemovedAPIWatchUnsafe(etcdClient, etcdStoragePrefix, betaCRD, apiExtensionsClient)
	if err != nil {
		return nil, err
	}
	return waitForCRDReady(crd, apiExtensionsClient, dynamicClientSet)
}

// CreateCRDUsingRemovedAPIWatchUnsafe 直接通过 etcd 创建 CRD，且不等待其就绪。只能用于验证
// 与已移除数据的兼容性。不要仅仅因为不想把测试改用 v1 就用它；只有在确实必要时才使用。
func CreateCRDUsingRemovedAPIWatchUnsafe(etcdClient *clientv3.Client, etcdStoragePrefix string, betaCRD *apiextensionsv1beta1.CustomResourceDefinition, apiExtensionsClient clientset.Interface) (*apiextensionsv1.CustomResourceDefinition, error) {
	// 尽力做一次默认值填充。
	apiextensionsv1beta1.SetDefaults_CustomResourceDefinition(betaCRD)
	// 写回 v1beta1 的 TypeMeta，模拟旧版本 API 直接落盘的数据。
	betaCRD.Kind = "CustomResourceDefinition"
	betaCRD.APIVersion = apiextensionsv1beta1.SchemeGroupVersion.Group + "/" + apiextensionsv1beta1.SchemeGroupVersion.Version

	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceNone)
	// 键布局必须与 apiextensions-apiserver 的实际存储路径保持一致。
	key := path.Join("/", etcdStoragePrefix, "apiextensions.k8s.io", "customresourcedefinitions", betaCRD.Name)
	val, _ := json.Marshal(betaCRD)
	if _, err := etcdClient.Put(ctx, key, string(val)); err != nil {
		return nil, err
	}

	return apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), betaCRD.Name, metav1.GetOptions{})
}
