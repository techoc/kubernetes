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

package fixtures

import (
	"context"
	"fmt"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/scale"
	"k8s.io/utils/ptr"
)

const (
	// noxuInstanceNum 是用于测试 int64 边界的最大值。
	noxuInstanceNum int64 = 9223372036854775807
)

// AllowAllSchema 返回不施加任何 schema 限制的校验配置。
func AllowAllSchema() *apiextensionsv1.CustomResourceValidation {
	return &apiextensionsv1.CustomResourceValidation{
		OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
			XPreserveUnknownFields: ptr.To(true),
			Type:                   "object",
		},
	}
}

// NewRandomNameV1CustomResourceDefinition 生成随机名称的 CRD，避免 e2e 测试中的名称冲突。
func NewRandomNameV1CustomResourceDefinition(scope apiextensionsv1.ResourceScope) *apiextensionsv1.CustomResourceDefinition {
	// 目前需要保证 singular 不以 s 结尾
	gName := names.SimpleNameGenerator.GenerateName("foo") + "a"
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: gName + "s.mygroup.example.com"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "mygroup.example.com",
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    "v1beta1",
					Served:  true,
					Storage: true,
					Schema:  AllowAllSchema(),
				},
			},
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   gName + "s",
				Singular: gName,
				Kind:     gName,
				ListKind: gName + "List",
			},
			Scope: scope,
		},
	}
}

// NewRandomNameMultipleVersionCustomResourceDefinition 生成随机名称的多版本 CRD，避免 e2e 测试中的名称冲突。
func NewRandomNameMultipleVersionCustomResourceDefinition(scope apiextensionsv1.ResourceScope) *apiextensionsv1.CustomResourceDefinition {
	// 目前需要保证 singular 不以 s 结尾
	gName := names.SimpleNameGenerator.GenerateName("foo") + "a"
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: gName + "s.mygroup.example.com"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "mygroup.example.com",
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    "v1beta1",
					Served:  true,
					Storage: false,
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					Schema: AllowAllSchema(),
				},
				{
					Name:    "v1",
					Served:  true,
					Storage: true,
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					Schema: AllowAllSchema(),
				},
			},
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   gName + "s",
				Singular: gName,
				Kind:     gName,
				ListKind: gName + "List",
			},
			Scope: scope,
		},
	}
}

// NewRandomNameV1CustomResourceDefinitionWithSchema 生成随机名称、使用给定 OpenAPIv3 schema 的 CRD，避免 e2e 测试中的名称冲突。
func NewRandomNameV1CustomResourceDefinitionWithSchema(scope apiextensionsv1.ResourceScope, openAPIV3Schema *apiextensionsv1.JSONSchemaProps, enableStatus bool) *apiextensionsv1.CustomResourceDefinition {
	crd := NewRandomNameV1CustomResourceDefinition(scope)
	for i := range crd.Spec.Versions {
		if enableStatus {
			crd.Spec.Versions[i].Subresources = &apiextensionsv1.CustomResourceSubresources{
				Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
			}
		}
		crd.Spec.Versions[i].Schema = &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: openAPIV3Schema}
	}
	return crd
}

// GetGroupVersionResourcesOfCustomResource 获取该 CustomResourceDefinition 的自定义资源对应的全部 GroupVersionResource。
func GetGroupVersionResourcesOfCustomResource(crd *apiextensionsv1.CustomResourceDefinition) []schema.GroupVersionResource {
	var result []schema.GroupVersionResource
	for _, v := range crd.Spec.Versions {
		result = append(result, schema.GroupVersionResource{
			Group:    crd.Spec.Group,
			Version:  v.Name,
			Resource: crd.Spec.Names.Plural,
		})
	}
	return result
}

// NewNoxuV1CustomResourceDefinition 返回一个 WishIHadChosenNoxu CRD。
func NewNoxuV1CustomResourceDefinition(scope apiextensionsv1.ResourceScope) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "noxus.mygroup.example.com"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "mygroup.example.com",
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    "v1beta1",
				Served:  true,
				Storage: true,
				Schema:  AllowAllSchema(),
			}},
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:     "noxus",
				Singular:   "nonenglishnoxu",
				Kind:       "WishIHadChosenNoxu",
				ShortNames: []string{"foo", "bar", "abc", "def"},
				ListKind:   "NoxuItemList",
				Categories: []string{"all"},
			},
			Scope: scope,
		},
	}
}

// NewVersionedNoxuInstance 返回指定版本的 WishIHadChosenNoxu 实例。
func NewVersionedNoxuInstance(namespace, name, version string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "mygroup.example.com/" + version,
			"kind":       "WishIHadChosenNoxu",
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
			},
			"content": map[string]interface{}{
				"key": "value",
			},
			"num": map[string]interface{}{
				"num1": noxuInstanceNum,
				"num2": 1000000,
			},
		},
	}
}

// NewNoxuInstance 返回 v1beta1 版本的 WishIHadChosenNoxu 实例。
func NewNoxuInstance(namespace, name string) *unstructured.Unstructured {
	return NewVersionedNoxuInstance(namespace, name, "v1beta1")
}

// NewMultipleVersionNoxuCRD 返回多版本的 WishIHadChosenNoxu CRD。
func NewMultipleVersionNoxuCRD(scope apiextensionsv1.ResourceScope) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "noxus.mygroup.example.com"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "mygroup.example.com",
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:     "noxus",
				Singular:   "nonenglishnoxu",
				Kind:       "WishIHadChosenNoxu",
				ShortNames: []string{"foo", "bar", "abc", "def"},
				ListKind:   "NoxuItemList",
				Categories: []string{"all"},
			},
			Scope: scope,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    "v1beta1",
					Served:  true,
					Storage: false,
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					Schema: AllowAllSchema(),
				},
				{
					Name:    "v1beta2",
					Served:  true,
					Storage: true,
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					Schema: AllowAllSchema(),
				},
				{
					Name:    "v0",
					Served:  false,
					Storage: false,
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					Schema: AllowAllSchema(),
				},
			},
		},
	}
}

// NewNoxu2CustomResourceDefinition 返回一个 WishIHadChosenNoxu2 CRD。
func NewNoxu2CustomResourceDefinition(scope apiextensionsv1.ResourceScope) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "noxus2.mygroup.example.com"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "mygroup.example.com",
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    "v1alpha1",
				Served:  true,
				Storage: true,
				Schema:  AllowAllSchema(),
			}},
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:     "noxus2",
				Singular:   "nonenglishnoxu2",
				Kind:       "WishIHadChosenNoxu2",
				ShortNames: []string{"foo", "bar", "abc", "def"},
				ListKind:   "Noxu2ItemList",
			},
			Scope: scope,
		},
	}
}

// NewCurletV1CustomResourceDefinition 返回一个 Curlet CRD。
func NewCurletV1CustomResourceDefinition(scope apiextensionsv1.ResourceScope) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "curlets.mygroup.example.com"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "mygroup.example.com",
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    "v1beta1",
					Served:  true,
					Storage: true,
					Schema:  AllowAllSchema(),
				},
			},
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "curlets",
				Singular: "curlet",
				Kind:     "Curlet",
				ListKind: "CurletList",
			},
			Scope: scope,
		},
	}
}

// NewCurletInstance 返回一个 Curlet 实例。
func NewCurletInstance(namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "mygroup.example.com/v1beta1",
			"kind":       "Curlet",
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
			},
			"content": map[string]interface{}{
				"key": "value",
			},
		},
	}
}

// servedV1Versions 返回 CRD 中标记为 served 的版本名列表。
func servedV1Versions(crd *apiextensionsv1.CustomResourceDefinition) []string {
	if len(crd.Spec.Versions) == 0 {
		return []string{}
	}
	var versions []string
	for _, v := range crd.Spec.Versions {
		if v.Served {
			versions = append(versions, v.Name)
		}
	}
	return versions
}

// existsInDiscoveryV1 报告指定版本的 CRD 资源是否已出现在 discovery 中。
func existsInDiscoveryV1(crd *apiextensionsv1.CustomResourceDefinition, apiExtensionsClient clientset.Interface, version string) (bool, error) {
	groupResource, err := apiExtensionsClient.Discovery().ServerResourcesForGroupVersion(crd.Spec.Group + "/" + version)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	for _, g := range groupResource.APIResources {
		if g.Name == crd.Spec.Names.Plural {
			return true, nil
		}
	}
	return false, nil
}

// waitForCRDReadyWatchUnsafe 创建 CRD 并确认 apiextension apiserver 已安装该 CRD。
// 但此时 watch 该 CR 并不安全。需要 watch CR 时请调用 CreateCRDUsingRemovedAPI。
func waitForCRDReadyWatchUnsafe(crd *apiextensionsv1.CustomResourceDefinition, apiExtensionsClient clientset.Interface) (*apiextensionsv1.CustomResourceDefinition, error) {
	// 等待所有资源出现在 discovery 中
	for _, version := range servedV1Versions(crd) {
		err := wait.PollUntilContextTimeout(context.Background(), 500*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			return existsInDiscoveryV1(crd, apiExtensionsClient, version)
		})
		if err != nil {
			return nil, err
		}
	}

	return crd, nil
}

// waitForCRDReady 创建给定 CRD，并确保服务端 watch cache 已就绪。
func waitForCRDReady(crd *apiextensionsv1.CustomResourceDefinition, apiExtensionsClient clientset.Interface, dynamicClientSet dynamic.Interface) (*apiextensionsv1.CustomResourceDefinition, error) {
	v1CRD, err := waitForCRDReadyWatchUnsafe(crd, apiExtensionsClient)
	if err != nil {
		return nil, err
	}

	// 以下逻辑仅用于测试。需要 watch cache 拥有对测试可用的 resource version。
	// 新建 REST storage 时，CR 的 storage cacher 会异步启动。
	// REST API 操作（如 list）返回的是 etcd 的 RV，但 storage cacher 的 reflector
	// 可能拿到不同的 RV，因为初始 list 操作与 storage cache reflector 启动之间 etcd 可能被改动。
	// 之后用 REST api list.RV 发起 watch，可能早于 storage cacher。
	// 通常的处理模型是：若收到 "resourceVersion too old"，就重新 list 和 watch。
	// 本测试中会循环执行 "list/watch/create/delete"，直到从 list 得到的 RV 能观察到 create 且不报错。
	// 这样所有检查 watch 的测试都不必担心 RV too old 问题，因为此前可能发生过各种意外，
	// 例如创建得到的 RV 可能太旧而无法 watch。
	err = wait.PollUntilContextTimeout(context.Background(), 500*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		return isWatchCachePrimed(v1CRD, dynamicClientSet)
	})
	if err != nil {
		return nil, err
	}
	return v1CRD, nil
}

// CreateNewV1CustomResourceDefinitionWatchUnsafe 创建 CRD 并确认 apiextension apiserver
// 已安装该 CRD。但此时 watch 该 CR 并不安全。需要 watch CR 时请调用
// CreateNewV1CustomResourceDefinition。
func CreateNewV1CustomResourceDefinitionWatchUnsafe(v1CRD *apiextensionsv1.CustomResourceDefinition, apiExtensionsClient clientset.Interface) (*apiextensionsv1.CustomResourceDefinition, error) {
	v1CRD, err := apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), v1CRD, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	// 等待所有资源出现在 discovery 中
	for _, version := range servedV1Versions(v1CRD) {
		err := wait.PollUntilContextTimeout(context.Background(), 500*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			return existsInDiscoveryV1(v1CRD, apiExtensionsClient, version)
		})
		if err != nil {
			return nil, err
		}
	}

	return v1CRD, nil
}

// CreateNewV1CustomResourceDefinition 创建给定 CRD，并确保服务端 watch cache 已就绪。
func CreateNewV1CustomResourceDefinition(v1CRD *apiextensionsv1.CustomResourceDefinition, apiExtensionsClient clientset.Interface, dynamicClientSet dynamic.Interface) (*apiextensionsv1.CustomResourceDefinition, error) {
	v1CRD, err := CreateNewV1CustomResourceDefinitionWatchUnsafe(v1CRD, apiExtensionsClient)
	if err != nil {
		return nil, err
	}

	// 以下逻辑仅用于测试。需要 watch cache 拥有对测试可用的 resource version。
	// 新建 REST storage 时，CR 的 storage cacher 会异步启动。
	// REST API 操作（如 list）返回的是 etcd 的 RV，但 storage cacher 的 reflector
	// 可能拿到不同的 RV，因为初始 list 操作与 storage cache reflector 启动之间 etcd 可能被改动。
	// 之后用 REST api list.RV 发起 watch，可能早于 storage cacher。
	// 通常的处理模型是：若收到 "resourceVersion too old"，就重新 list 和 watch。
	// 本测试中会循环执行 "list/watch/create/delete"，直到从 list 得到的 RV 能观察到 create 且不报错。
	// 这样所有检查 watch 的测试都不必担心 RV too old 问题，因为此前可能发生过各种意外，
	// 例如创建得到的 RV 可能太旧而无法 watch。
	err = wait.PollUntilContextTimeout(context.Background(), 500*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		return isWatchCachePrimed(v1CRD, dynamicClientSet)
	})
	if err != nil {
		return nil, err
	}
	return v1CRD, nil
}

// resourceClientForVersion 返回指定版本的 dynamic 资源客户端，并根据作用域选择是否带命名空间。
func resourceClientForVersion(crd *apiextensionsv1.CustomResourceDefinition, dynamicClientSet dynamic.Interface, namespace, version string) dynamic.ResourceInterface {
	gvr := schema.GroupVersionResource{Group: crd.Spec.Group, Version: version, Resource: crd.Spec.Names.Plural}
	if crd.Spec.Scope != apiextensionsv1.ClusterScoped {
		return dynamicClientSet.Resource(gvr).Namespace(namespace)
	}
	return dynamicClientSet.Resource(gvr)
}

// isWatchCachePrimed 报告 CRD 指定版本的 watch 是否已就绪。
func isWatchCachePrimed(crd *apiextensionsv1.CustomResourceDefinition, dynamicClientSet dynamic.Interface) (bool, error) {
	ns := ""
	if crd.Spec.Scope != apiextensionsv1.ClusterScoped {
		ns = "default"
	}

	versions := servedV1Versions(crd)
	if len(versions) == 0 {
		return true, nil
	}

	resourceClient := resourceClientForVersion(crd, dynamicClientSet, ns, versions[0])
	instanceName := "setup-instance"
	instance := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": crd.Spec.Group + "/" + versions[0],
			"kind":       crd.Spec.Names.Kind,
			"metadata": map[string]interface{}{
				"namespace": ns,
				"name":      instanceName,
			},
			"alpha":   "foo_123",
			"beta":    10,
			"gamma":   "bar",
			"delta":   "hello",
			"epsilon": "foobar",
			"spec":    map[string]interface{}{},
		},
	}
	createdInstance, err := resourceClient.Create(context.TODO(), instance, metav1.CreateOptions{})
	if err != nil {
		return false, err
	}
	err = resourceClient.Delete(context.TODO(), createdInstance.GetName(), metav1.DeleteOptions{})
	if err != nil {
		return false, err
	}

	// 等待所有版本的 watch cache 就绪，并确保已消费所有版本的 DELETE 事件，
	// 这样任何 ResourceVersion=0 的新 watch 都不会收到这些事件。这是部分测试不稳定的来源。
	// 客户端以 resourceVersion=0 创建 watch 时，会收到所有已存在对象的 ADD 事件；
	// 但由于指定了 resourceVersion=0，cache 缓冲区中没有可返回已存在事件的起点，
	// 服务端会从 cache 当前头部返回到末尾。在这里消费所有版本的 delete 事件，
	// 可以确保 cache 头部已越过这些事件，之后的 ResourceVersion=0 watch 不会再收到它们。
	for _, v := range versions {
		noxuWatch, err := resourceClientForVersion(crd, dynamicClientSet, ns, v).Watch(
			context.TODO(),
			metav1.ListOptions{ResourceVersion: createdInstance.GetResourceVersion()})
		if err != nil {
			return false, err
		}
		defer noxuWatch.Stop()

		select {
		case watchEvent := <-noxuWatch.ResultChan():
			if watch.Error == watchEvent.Type {
				return false, nil
			}
			if watch.Deleted != watchEvent.Type {
				return false, fmt.Errorf("expected DELETE, but got %#v", watchEvent)
			}
		case <-time.After(5 * time.Second):
			return false, fmt.Errorf("gave up waiting for watch event")
		}
	}

	return true, nil
}

// DeleteV1CustomResourceDefinition 删除 CRD，并等待其从 discovery 中消失。
func DeleteV1CustomResourceDefinition(crd *apiextensionsv1.CustomResourceDefinition, apiExtensionsClient clientset.Interface) error {
	if err := apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Delete(context.TODO(), crd.Name, metav1.DeleteOptions{}); err != nil {
		return err
	}
	for _, version := range servedV1Versions(crd) {
		err := wait.PollUntilContextTimeout(context.Background(), 500*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			exists, err := existsInDiscoveryV1(crd, apiExtensionsClient, version)
			return !exists, err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteV1CustomResourceDefinitions 删除所有匹配 deleteListOpts 的 CRD，并等待它们全部从 discovery 中消失。
func DeleteV1CustomResourceDefinitions(deleteListOpts metav1.ListOptions, apiExtensionsClient clientset.Interface) error {
	list, err := apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().List(context.TODO(), deleteListOpts)
	if err != nil {
		return err
	}
	if err = apiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, deleteListOpts); err != nil {
		return err
	}
	for _, crd := range list.Items {
		for _, version := range servedV1Versions(&crd) {
			err := wait.PollUntilContextTimeout(context.Background(), 500*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
				exists, err := existsInDiscoveryV1(&crd, apiExtensionsClient, version)
				return !exists, err
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// CreateNewVersionedScaleClient 返回指定版本的 scale 客户端。
func CreateNewVersionedScaleClient(crd *apiextensionsv1.CustomResourceDefinition, config *rest.Config, version string) (scale.ScalesGetter, error) {
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, err
	}
	groupResource, err := discoveryClient.ServerResourcesForGroupVersion(crd.Spec.Group + "/" + version)
	if err != nil {
		return nil, err
	}

	resources := []*restmapper.APIGroupResources{
		{
			Group: metav1.APIGroup{
				Name: crd.Spec.Group,
				Versions: []metav1.GroupVersionForDiscovery{
					{Version: version},
				},
				PreferredVersion: metav1.GroupVersionForDiscovery{Version: version},
			},
			VersionedResources: map[string][]metav1.APIResource{
				version: groupResource.APIResources,
			},
		},
	}

	restMapper := restmapper.NewDiscoveryRESTMapper(resources)
	resolver := scale.NewDiscoveryScaleKindResolver(discoveryClient)

	return scale.NewForConfig(config, restMapper, dynamic.LegacyAPIPathResolverFunc, resolver)
}
