/*
Copyright 2016 The Kubernetes Authors.

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

package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	aggregator "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

const acceptV1JSON = "application/json"
const acceptV2JSON = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"
const acceptV2JSONNoPeer = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList;profile=nopeer"

// Discovery 集成测试经常需要在资源紧张的 CI 节点上等待 aggregator、
// API 注册和 discovery 文档收敛。
const maxTimeout = 120 * time.Second

type testClient interface {
	kubernetes.Interface
	aggregator.Interface
	apiextensions.Interface
	dynamic.Interface
}

// discovery 集成测试的声明式框架
// 每个测试都有元数据和一系列 action，每个 action 都必须通过，
// 测试才算通过
type testCase struct {
	Name    string
	Actions []testAction
}

// 定义一个接口，其中的函数对集成测试 api server 执行某些操作并返回错误。
// 错误非 nil 时测试失败
type testAction interface {
	Do(ctx context.Context, client testClient) error
}

type cleaningAction interface {
	testAction
	Cleanup(ctx context.Context, client testClient) error
}

// 向集群应用一个 apiservice
type applyAPIService apiregistrationv1.APIServiceSpec

type applyCRD apiextensionsv1.CustomResourceDefinitionSpec

type deleteObject struct {
	metav1.GroupVersionResource
	Namespace string
	Name      string
}

// 等待 groupversion 出现在 v1 discovery 中
type waitForGroupVersionsV1 []metav1.GroupVersion

// 等待 groupversion 从 v1 discovery 中消失
type waitForAbsentGroupVersionsV1 []metav1.GroupVersion

// 等待 groupversion 出现在 v2 discovery 中
type waitForGroupVersionsV2 []metav1.GroupVersion

// 等待 groupversion 从 v2 discovery 中消失
type waitForAbsentGroupVersionsV2 []metav1.GroupVersion

type waitForStaleGroupVersionsV2 []metav1.GroupVersion
type waitForFreshGroupVersionsV2 []metav1.GroupVersion

type waitForResourcesV1 []metav1.GroupVersionResource
type waitForResourcesAbsentV1 []metav1.GroupVersionResource

type waitForResourcesV2 []metav1.GroupVersionResource
type waitForResourcesAbsentV2 []metav1.GroupVersionResource

// 断言 v2 discovery 当前状态的某些内容
type inlineAction func(ctx context.Context, client testClient) error

func (a applyAPIService) Do(ctx context.Context, client testClient) error {
	// 使用 dynamic client，因为 typed client 不支持 `Apply`
	// 操作？
	obj := &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: a.Version + "." + a.Group,
		},
		Spec: apiregistrationv1.APIServiceSpec(a),
	}

	unstructuredContent, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}

	unstructedObject := &unstructured.Unstructured{}
	unstructedObject.SetUnstructuredContent(unstructuredContent)
	unstructedObject.SetGroupVersionKind(apiregistrationv1.SchemeGroupVersion.WithKind("APIService"))

	_, err = client.
		Resource(apiregistrationv1.SchemeGroupVersion.WithResource("apiservices")).
		Apply(ctx, obj.Name, unstructedObject, metav1.ApplyOptions{
			FieldManager: "test-manager",
		})

	return err
}

func (a applyAPIService) Cleanup(ctx context.Context, client testClient) error {
	name := a.Version + "." + a.Group

	// 记录现有对象（如果存在）的 UID
	var uid types.UID
	if obj, err := client.ApiregistrationV1().APIServices().Get(ctx, name, metav1.GetOptions{}); err == nil {
		uid = obj.UID
	}

	err := client.ApiregistrationV1().APIServices().Delete(ctx, name, metav1.DeleteOptions{})

	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	err = wait.PollUntilContextTimeout(
		ctx,
		250*time.Millisecond,
		maxTimeout,
		true,
		func(ctx context.Context) (done bool, err error) {
			obj, err := client.ApiregistrationV1().APIServices().Get(ctx, name, metav1.GetOptions{})
			switch {
			case errors.IsNotFound(err):
				// 对象已消失
				return true, nil
			case err == nil && obj.UID != uid:
				// 我们等待的实例已消失
				return true, nil
			case err != nil:
				// 发生了其他错误
				return false, err
			default:
				// 我们等待的实例仍然存在
				return false, nil
			}
		},
	)

	if err != nil {
		return fmt.Errorf("error waiting for APIService %v to clean up: %w", name, err)
	}

	return nil
}

func (a applyCRD) Do(ctx context.Context, client testClient) error {
	// 使用 dynamic client，因为 typed client 不支持 `Apply`
	// 操作？
	name := a.Names.Plural + "." + a.Group
	obj := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec(a),
	}

	if strings.HasSuffix(obj.Name, ".k8s.io") {
		if obj.Annotations == nil {
			obj.Annotations = map[string]string{}
		}
		obj.Annotations["api-approved.kubernetes.io"] = "https://github.com/kubernetes/kubernetes/fake"
	}

	unstructuredContent, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}

	unstructedObject := &unstructured.Unstructured{}
	unstructedObject.SetUnstructuredContent(unstructuredContent)
	unstructedObject.SetGroupVersionKind(apiextensionsv1.SchemeGroupVersion.WithKind("CustomResourceDefinition"))

	_, err = client.
		Resource(apiextensionsv1.SchemeGroupVersion.WithResource("customresourcedefinitions")).
		Apply(ctx, obj.Name, unstructedObject, metav1.ApplyOptions{
			FieldManager: "test-manager",
		})

	return err
}

func (a applyCRD) Cleanup(ctx context.Context, client testClient) error {
	name := a.Names.Plural + "." + a.Group

	// 记录现有对象（如果存在）的 UID
	var uid types.UID
	if obj, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, name, metav1.GetOptions{}); err == nil {
		uid = obj.UID
	}

	err := client.ApiextensionsV1().CustomResourceDefinitions().Delete(ctx, name, metav1.DeleteOptions{})

	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	err = wait.PollUntilContextTimeout(
		ctx,
		250*time.Millisecond,
		maxTimeout,
		true,
		func(ctx context.Context) (done bool, err error) {
			obj, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, name, metav1.GetOptions{})
			switch {
			case errors.IsNotFound(err):
				// 对象已消失
				return true, nil
			case err == nil && obj.UID != uid:
				// 我们等待的实例已消失
				return true, nil
			case err != nil:
				// 发生了其他错误
				return false, err
			default:
				// 我们等待的实例仍然存在
				return false, nil
			}
		},
	)

	if err != nil {
		return fmt.Errorf("error waiting for CRD %v to clean up: %w", name, err)
	}

	return nil
}

func (d deleteObject) Do(ctx context.Context, client testClient) error {
	if d.Namespace == "" {
		return client.Resource(schema.GroupVersionResource(d.GroupVersionResource)).
			Delete(ctx, d.Name, metav1.DeleteOptions{})
	} else {
		return client.Resource(schema.GroupVersionResource(d.GroupVersionResource)).
			Namespace(d.Namespace).
			Delete(ctx, d.Name, metav1.DeleteOptions{})
	}
}

func (w waitForStaleGroupVersionsV2) Do(ctx context.Context, client testClient) error {
	err := WaitForResultWithCondition(ctx, client, func(result apidiscoveryv2.APIGroupDiscoveryList) bool {
		for _, gv := range w {
			if info := FindGroupVersionV2(result, gv); info == nil || info.Freshness != apidiscoveryv2.DiscoveryFreshnessStale {
				return false
			}
		}

		return true
	})

	if err != nil {
		return fmt.Errorf("waiting for stale groupversions v2 (%v): %w", w, err)
	}
	return nil
}

func (w waitForFreshGroupVersionsV2) Do(ctx context.Context, client testClient) error {
	err := WaitForResultWithCondition(ctx, client, func(result apidiscoveryv2.APIGroupDiscoveryList) bool {
		for _, gv := range w {
			if info := FindGroupVersionV2(result, gv); info == nil || info.Freshness != apidiscoveryv2.DiscoveryFreshnessCurrent {
				return false
			}
		}

		return true
	})

	if err != nil {
		return fmt.Errorf("waiting for fresh groupversions v2 (%v): %w", w, err)
	}
	return nil
}

func (w waitForGroupVersionsV2) Do(ctx context.Context, client testClient) error {
	err := WaitForResultWithCondition(ctx, client, func(result apidiscoveryv2.APIGroupDiscoveryList) bool {
		for _, gv := range w {
			if FindGroupVersionV2(result, gv) == nil {
				return false
			}
		}

		return true
	})

	if err != nil {
		return fmt.Errorf("waiting for groupversions v2 (%v): %w", w, err)
	}
	return nil
}

func (w waitForAbsentGroupVersionsV2) Do(ctx context.Context, client testClient) error {
	err := WaitForResultWithCondition(ctx, client, func(result apidiscoveryv2.APIGroupDiscoveryList) bool {
		for _, gv := range w {
			if FindGroupVersionV2(result, gv) != nil {
				return false
			}
		}

		return true
	})

	if err != nil {
		return fmt.Errorf("waiting for absent groupversions v2 (%v): %w", w, err)
	}
	return nil
}

func (w waitForGroupVersionsV1) Do(ctx context.Context, client testClient) error {
	err := WaitForV1GroupsWithCondition(ctx, client, func(result metav1.APIGroupList) bool {
		for _, gv := range w {
			if !FindGroupVersionV1(result, gv) {
				return false
			}
		}

		return true
	})

	if err != nil {
		return fmt.Errorf("waiting for groupversions v1 (%v): %w", w, err)
	}
	return nil
}

func (w waitForAbsentGroupVersionsV1) Do(ctx context.Context, client testClient) error {
	err := WaitForV1GroupsWithCondition(ctx, client, func(result metav1.APIGroupList) bool {
		for _, gv := range w {
			if FindGroupVersionV1(result, gv) {
				return false
			}
		}

		return true
	})

	if err != nil {
		return fmt.Errorf("waiting for absent groupversions v1 (%v): %w", w, err)
	}
	return nil
}

func (w waitForResourcesV1) Do(ctx context.Context, client testClient) error {
	requiredResources := map[metav1.GroupVersion][]string{}

	for _, gvr := range w {
		gv := metav1.GroupVersion{Group: gvr.Group, Version: gvr.Version}
		if existing, ok := requiredResources[gv]; ok {
			requiredResources[gv] = append(existing, gvr.Resource)
		} else {
			requiredResources[gv] = []string{gvr.Resource}
		}
	}

	for gv, resourceNames := range requiredResources {
		err := WaitForV1ResourcesWithCondition(ctx, client, gv, func(result metav1.APIResourceList) bool {
			for _, name := range resourceNames {
				found := false

				for _, resultResource := range result.APIResources {
					if resultResource.Name == name {
						found = true
						break
					}
				}

				if !found {
					return false
				}
			}

			return true
		})

		if err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("waiting for resources v1 (%v): %w", w, err)
		}
	}

	return nil
}

func (w waitForResourcesAbsentV1) Do(ctx context.Context, client testClient) error {
	requiredResources := map[metav1.GroupVersion][]string{}

	for _, gvr := range w {
		gv := metav1.GroupVersion{Group: gvr.Group, Version: gvr.Version}
		if existing, ok := requiredResources[gv]; ok {
			requiredResources[gv] = append(existing, gvr.Resource)
		} else {
			requiredResources[gv] = []string{gvr.Resource}
		}
	}

	for gv, resourceNames := range requiredResources {
		err := WaitForV1ResourcesWithCondition(ctx, client, gv, func(result metav1.APIResourceList) bool {
			for _, name := range resourceNames {
				for _, resultResource := range result.APIResources {
					if resultResource.Name == name {
						return false
					}
				}
			}

			return true
		})

		if err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("waiting for absent resources v1 (%v): %w", w, err)
		}
	}

	return nil
}

func (w waitForResourcesV2) Do(ctx context.Context, client testClient) error {
	err := WaitForResultWithCondition(ctx, client, func(result apidiscoveryv2.APIGroupDiscoveryList) bool {
		for _, gvr := range w {
			if info := FindGroupVersionV2(result, metav1.GroupVersion{Group: gvr.Group, Version: gvr.Version}); info == nil {
				return false
			} else {
				found := false
				for _, resultResoure := range info.Resources {
					if resultResoure.Resource == gvr.Resource {
						found = true
						break
					}
				}

				if !found {
					return false
				}
			}
		}

		return true
	})

	if err != nil {
		return fmt.Errorf("waiting for resources v2 (%v): %w", w, err)
	}
	return nil
}

func (w waitForResourcesAbsentV2) Do(ctx context.Context, client testClient) error {
	err := WaitForResultWithCondition(ctx, client, func(result apidiscoveryv2.APIGroupDiscoveryList) bool {
		for _, gvr := range w {
			if info := FindGroupVersionV2(result, metav1.GroupVersion{Group: gvr.Group, Version: gvr.Version}); info == nil {
				return false
			} else {
				for _, resultResoure := range info.Resources {
					if resultResoure.Resource == gvr.Resource {
						return false
					}
				}
			}
		}

		return true
	})

	if err != nil {
		return fmt.Errorf("waiting for absent resources v2 (%v): %w", w, err)
	}
	return nil
}

func (i inlineAction) Do(ctx context.Context, client testClient) error {
	return i(ctx, client)
}

func FetchV2Discovery(ctx context.Context, client testClient) (apidiscoveryv2.APIGroupDiscoveryList, error) {
	result, err := client.
		Discovery().
		RESTClient().
		Get().
		AbsPath("/apis").
		SetHeader("Accept", acceptV2JSON).
		Do(ctx).
		Raw()

	if err != nil {
		return apidiscoveryv2.APIGroupDiscoveryList{}, fmt.Errorf("failed to fetch v2 discovery: %w", err)
	}

	groupList := apidiscoveryv2.APIGroupDiscoveryList{}
	err = json.Unmarshal(result, &groupList)
	if err != nil {
		return apidiscoveryv2.APIGroupDiscoveryList{}, fmt.Errorf("failed to parse v2 discovery: %w", err)
	}

	return groupList, nil
}

func FetchV1DiscoveryGroups(ctx context.Context, client testClient) (metav1.APIGroupList, error) {
	return FetchV1DiscoveryGroupsAtPath(ctx, client, "/apis")
}

func FetchV1DiscoveryLegacyGroups(ctx context.Context, client testClient) (metav1.APIGroupList, error) {
	return FetchV1DiscoveryGroupsAtPath(ctx, client, "/api")
}

func FetchV1DiscoveryGroupsAtPath(ctx context.Context, client testClient, path string) (metav1.APIGroupList, error) {
	result, err := client.
		Discovery().
		RESTClient().
		Get().
		AbsPath(path).
		SetHeader("Accept", acceptV1JSON).
		Do(ctx).
		Raw()

	if err != nil {
		return metav1.APIGroupList{}, fmt.Errorf("failed to fetch v1 discovery at %v: %w", path, err)
	}

	groupList := metav1.APIGroupList{}
	err = json.Unmarshal(result, &groupList)
	if err != nil {
		return metav1.APIGroupList{}, fmt.Errorf("failed to parse v1 discovery at %v: %w", path, err)
	}

	return groupList, nil
}

func FetchV1DiscoveryResource(ctx context.Context, client testClient, gv metav1.GroupVersion) (metav1.APIResourceList, error) {
	result, err := client.
		Discovery().
		RESTClient().
		Get().
		AbsPath("/apis/"+gv.Group+"/"+gv.Version).
		SetHeader("Accept", acceptV1JSON).
		Do(ctx).
		Raw()

	if err != nil {
		return metav1.APIResourceList{}, err
	}

	groupList := metav1.APIResourceList{}
	err = json.Unmarshal(result, &groupList)
	if err != nil {
		return metav1.APIResourceList{}, err
	}

	return groupList, nil
}

func WaitForGroupsAbsent(ctx context.Context, client testClient, groups ...string) error {
	return WaitForResultWithCondition(ctx, client, func(groupList apidiscoveryv2.APIGroupDiscoveryList) bool {
		for _, searchGroup := range groups {
			for _, docGroup := range groupList.Items {
				if docGroup.Name == searchGroup {
					return false
				}
			}
		}
		return true
	})

}

func WaitForRootPaths(t *testing.T, ctx context.Context, client testClient, requirePaths, forbidPaths sets.Set[string]) error {
	return wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, maxTimeout, true, func(ctx context.Context) (done bool, err error) {
		statusContent, err := client.Discovery().RESTClient().Get().AbsPath("/").SetHeader("Accept", "application/json").DoRaw(ctx)
		if err != nil {
			return false, err
		}
		rootPaths := metav1.RootPaths{}
		if err := json.Unmarshal(statusContent, &rootPaths); err != nil {
			return false, err
		}
		paths := sets.New(rootPaths.Paths...)
		if missing := requirePaths.Difference(paths); len(missing) > 0 {
			t.Logf("missing required root paths %v", sets.List(missing))
			return false, nil
		}
		if present := forbidPaths.Intersection(paths); len(present) > 0 {
			t.Logf("present forbidden root paths %v", sets.List(present))
			return false, nil
		}
		return true, nil
	})
}

func WaitForGroups(ctx context.Context, client testClient, groups ...apidiscoveryv2.APIGroupDiscovery) error {
	return WaitForResultWithCondition(ctx, client, func(groupList apidiscoveryv2.APIGroupDiscoveryList) bool {
		for _, searchGroup := range groups {
			found := false
			for _, docGroup := range groupList.Items {
				if reflect.DeepEqual(searchGroup, docGroup) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	})
}

func WaitForResultWithCondition(ctx context.Context, client testClient, condition func(result apidiscoveryv2.APIGroupDiscoveryList) bool) error {
	// 持续从 aggregator 获取文档。
	// 检查它在合理时间内是否包含我们的 service
	return wait.PollUntilContextTimeout(
		ctx,
		250*time.Millisecond,
		maxTimeout,
		true,
		func(ctx context.Context) (done bool, err error) {
			groupList, err := FetchV2Discovery(ctx, client)
			if err != nil {
				return false, err
			}

			if condition(groupList) {
				return true, nil
			}

			return false, nil
		})
}

func WaitForV1GroupsWithCondition(ctx context.Context, client testClient, condition func(result metav1.APIGroupList) bool) error {
	// 持续从 aggregator 获取文档。
	// 检查它在合理时间内是否包含我们的 service
	return wait.PollUntilContextTimeout(
		ctx,
		250*time.Millisecond,
		maxTimeout,
		true,
		func(ctx context.Context) (done bool, err error) {
			groupList, err := FetchV1DiscoveryGroups(ctx, client)

			if err != nil {
				return false, err
			}

			if condition(groupList) {
				return true, nil
			}

			return false, nil
		})
}

func WaitForV1ResourcesWithCondition(ctx context.Context, client testClient, gv metav1.GroupVersion, condition func(result metav1.APIResourceList) bool) error {
	// 持续从 aggregator 获取文档。
	// 检查它在合理时间内是否包含我们的 service
	return wait.PollUntilContextTimeout(
		ctx,
		250*time.Millisecond,
		maxTimeout,
		true,
		func(ctx context.Context) (done bool, err error) {
			resourceList, err := FetchV1DiscoveryResource(ctx, client, gv)

			if err != nil {
				return false, err
			}

			if condition(resourceList) {
				return true, nil
			}

			return false, nil
		})
}

func FindGroupVersionV1(discovery metav1.APIGroupList, gv metav1.GroupVersion) bool {
	for _, documentGroup := range discovery.Groups {
		if documentGroup.Name != gv.Group {
			continue
		}

		for _, documentVersion := range documentGroup.Versions {
			if documentVersion.Version == gv.Version {
				return true
			}
		}
	}

	return false
}

func FindGroupVersionV2(discovery apidiscoveryv2.APIGroupDiscoveryList, gv metav1.GroupVersion) *apidiscoveryv2.APIVersionDiscovery {
	for _, documentGroup := range discovery.Items {
		if documentGroup.Name != gv.Group {
			continue
		}

		for _, documentVersion := range documentGroup.Versions {
			if documentVersion.Version == gv.Version {
				return &documentVersion
			}
		}
	}

	return nil
}

// FetchNoPeerDiscovery 显式请求 no-peer discovery
func FetchNoPeerDiscovery(ctx context.Context, client testClient) (apidiscoveryv2.APIGroupDiscoveryList, error) {
	result, err := client.
		Discovery().
		RESTClient().
		Get().
		AbsPath("/apis").
		SetHeader("Accept", acceptV2JSONNoPeer).
		Do(ctx).
		Raw()

	if err != nil {
		return apidiscoveryv2.APIGroupDiscoveryList{}, fmt.Errorf("failed to fetch no-peer discovery: %w", err)
	}

	groupList := apidiscoveryv2.APIGroupDiscoveryList{}
	err = json.Unmarshal(result, &groupList)
	if err != nil {
		return apidiscoveryv2.APIGroupDiscoveryList{}, fmt.Errorf("failed to parse no-peer discovery: %w", err)
	}

	return groupList, nil
}

// FetchPeerAggregatedDiscovery 显式请求 peer-aggregated discovery
func FetchPeerAggregatedDiscovery(ctx context.Context, client testClient) (apidiscoveryv2.APIGroupDiscoveryList, error) {
	result, err := client.
		Discovery().
		RESTClient().
		Get().
		AbsPath("/apis").
		SetHeader("Accept", acceptV2JSON).
		Do(ctx).
		Raw()

	if err != nil {
		return apidiscoveryv2.APIGroupDiscoveryList{}, fmt.Errorf("failed to fetch peer-aggregated discovery: %w", err)
	}

	groupList := apidiscoveryv2.APIGroupDiscoveryList{}
	err = json.Unmarshal(result, &groupList)
	if err != nil {
		return apidiscoveryv2.APIGroupDiscoveryList{}, fmt.Errorf("failed to parse peer-aggregated discovery: %w", err)
	}

	return groupList, nil
}

// WaitForPeerAggregatedDiscoveryWithCondition 等待 peer-aggregated discovery 满足条件。
func WaitForPeerAggregatedDiscoveryWithCondition(ctx context.Context, client testClient, condition func(result apidiscoveryv2.APIGroupDiscoveryList) bool) error {
	return wait.PollUntilContextTimeout(
		ctx,
		5*time.Second,
		30*time.Second,
		true,
		func(ctx context.Context) (done bool, err error) {
			groupList, err := FetchPeerAggregatedDiscovery(ctx, client)
			if err != nil {
				return false, err
			}

			if condition(groupList) {
				return true, nil
			}

			return false, nil
		})
}
