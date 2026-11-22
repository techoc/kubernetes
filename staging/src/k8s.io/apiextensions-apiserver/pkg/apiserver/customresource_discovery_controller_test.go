/*
Copyright 2022 The Kubernetes Authors.

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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	"k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
)

// coolFooCRD 是带 status 子资源的测试 CRD。
var coolFooCRD = &v1.CustomResourceDefinition{
	TypeMeta: metav1.TypeMeta{
		APIVersion: "apiextensions.k8s.io/v1",
		Kind:       "CustomResourceDefinition",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name: "coolfoo.stable.example.com",
	},
	Spec: v1.CustomResourceDefinitionSpec{
		Group: "stable.example.com",
		Names: v1.CustomResourceDefinitionNames{
			Plural:     "coolfoos",
			Singular:   "coolfoo",
			ShortNames: []string{"foo"},
			Kind:       "CoolFoo",
			ListKind:   "CoolFooList",
			Categories: []string{"cool"},
		},
		Scope: v1.ClusterScoped,
		Versions: []v1.CustomResourceDefinitionVersion{
			{
				Name:       "v1",
				Served:     true,
				Storage:    true,
				Deprecated: false,
				Subresources: &v1.CustomResourceSubresources{
					// 该 CRD 带有 /status 子资源
					Status: &v1.CustomResourceSubresourceStatus{},
				},
				Schema: &v1.CustomResourceValidation{
					// discovery 不使用该字段
					OpenAPIV3Schema: &v1.JSONSchemaProps{},
				},
			},
		},
		Conversion:            &v1.CustomResourceConversion{},
		PreserveUnknownFields: false,
	},
	Status: v1.CustomResourceDefinitionStatus{
		Conditions: []v1.CustomResourceDefinitionCondition{
			{
				Type:   v1.Established,
				Status: v1.ConditionTrue,
			},
		},
	},
}

// coolBarCRD 是不带子资源的测试 CRD。
var coolBarCRD = &v1.CustomResourceDefinition{
	TypeMeta: metav1.TypeMeta{
		APIVersion: "apiextensions.k8s.io/v1",
		Kind:       "CustomResourceDefinition",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name: "coolbar.stable.example.com",
	},
	Spec: v1.CustomResourceDefinitionSpec{
		Group: "stable.example.com",
		Names: v1.CustomResourceDefinitionNames{
			Plural:     "coolbars",
			Singular:   "coolbar",
			ShortNames: []string{"bar"},
			Kind:       "CoolBar",
			ListKind:   "CoolBarList",
			Categories: []string{"cool"},
		},
		Scope: v1.ClusterScoped,
		Versions: []v1.CustomResourceDefinitionVersion{
			{
				Name:       "v1",
				Served:     true,
				Storage:    true,
				Deprecated: false,
				Schema: &v1.CustomResourceValidation{
					// discovery 不使用该字段
					OpenAPIV3Schema: &v1.JSONSchemaProps{},
				},
			},
		},
		Conversion:            &v1.CustomResourceConversion{},
		PreserveUnknownFields: false,
	},
	Status: v1.CustomResourceDefinitionStatus{
		Conditions: []v1.CustomResourceDefinitionCondition{
			{
				Type:   v1.Established,
				Status: v1.ConditionTrue,
			},
		},
	},
}

// coolFooDiscovery 是 coolFoo CRD 对应的预期发现信息。
var coolFooDiscovery apidiscoveryv2.APIVersionDiscovery = apidiscoveryv2.APIVersionDiscovery{
	Version:   "v1",
	Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
	Resources: []apidiscoveryv2.APIResourceDiscovery{
		{
			Resource:         "coolfoos",
			Scope:            apidiscoveryv2.ScopeCluster,
			SingularResource: "coolfoo",
			Verbs:            []string{"delete", "deletecollection", "get", "list", "patch", "create", "update", "watch"},
			ShortNames:       []string{"foo"},
			Categories:       []string{"cool"},
			ResponseKind: &metav1.GroupVersionKind{
				Group:   "stable.example.com",
				Version: "v1",
				Kind:    "CoolFoo",
			},
			Subresources: []apidiscoveryv2.APISubresourceDiscovery{
				{
					Subresource:   "status",
					Verbs:         []string{"get", "patch", "update"},
					AcceptedTypes: nil, // 这样是否正确？
					ResponseKind: &metav1.GroupVersionKind{
						Group:   "stable.example.com",
						Version: "v1",
						Kind:    "CoolFoo",
					},
				},
			},
		},
	},
}

var mergedDiscovery apidiscoveryv2.APIVersionDiscovery = apidiscoveryv2.APIVersionDiscovery{
	Version:   "v1",
	Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
	Resources: []apidiscoveryv2.APIResourceDiscovery{
		{
			Resource:         "coolbars",
			Scope:            apidiscoveryv2.ScopeCluster,
			SingularResource: "coolbar",
			Verbs:            []string{"delete", "deletecollection", "get", "list", "patch", "create", "update", "watch"},
			ShortNames:       []string{"bar"},
			Categories:       []string{"cool"},
			ResponseKind: &metav1.GroupVersionKind{
				Group:   "stable.example.com",
				Version: "v1",
				Kind:    "CoolBar",
			},
		}, {
			Resource:         "coolfoos",
			Scope:            apidiscoveryv2.ScopeCluster,
			SingularResource: "coolfoo",
			Verbs:            []string{"delete", "deletecollection", "get", "list", "patch", "create", "update", "watch"},
			ShortNames:       []string{"foo"},
			Categories:       []string{"cool"},
			ResponseKind: &metav1.GroupVersionKind{
				Group:   "stable.example.com",
				Version: "v1",
				Kind:    "CoolFoo",
			},
			Subresources: []apidiscoveryv2.APISubresourceDiscovery{
				{
					Subresource:   "status",
					Verbs:         []string{"get", "patch", "update"},
					AcceptedTypes: nil, // 这样是否正确？
					ResponseKind: &metav1.GroupVersionKind{
						Group:   "stable.example.com",
						Version: "v1",
						Kind:    "CoolFoo",
					},
				},
			},
		},
	},
}

func init() {
	// 并非针对真实 apiserver 测试，因此直接假设名称被接受
	coolFooCRD.Status.AcceptedNames = coolFooCRD.Spec.Names
	coolBarCRD.Status.AcceptedNames = coolBarCRD.Spec.Names
}

// testEnvironment 提供 apiextensions-apiserver 客户端以及 discovery 测试所需的组件。
type testEnvironment struct {
	clientset.Interface

	// Discovery 测试相关组件
	versionDiscoveryHandler
	groupDiscoveryHandler

	aggregated.FakeResourceManager
}

// Start 启动 informer 工厂和 discovery controller，并等待其完成同步。
func (env *testEnvironment) Start(ctx context.Context) {
	discoverySyncedCh := make(chan struct{})

	factory := externalversions.NewSharedInformerFactoryWithOptions(
		env.Interface, 30*time.Second)

	discoveryController := NewDiscoveryController(
		factory.Apiextensions().V1().CustomResourceDefinitions(),
		&env.versionDiscoveryHandler,
		&env.groupDiscoveryHandler,
		env.FakeResourceManager,
	)

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	go discoveryController.Run(ctx.Done(), discoverySyncedCh)

	select {
	case <-discoverySyncedCh:
	case <-ctx.Done():
	}
}

// setup 构造一个使用 fake clientset 和 fake ResourceManager 的测试环境。
func setup() *testEnvironment {
	env := &testEnvironment{
		Interface:           fake.NewSimpleClientset(),
		FakeResourceManager: aggregated.NewFakeResourceManager(),
		versionDiscoveryHandler: versionDiscoveryHandler{
			discovery: make(map[schema.GroupVersion]*discovery.APIVersionHandler),
		},
		groupDiscoveryHandler: groupDiscoveryHandler{
			discovery: make(map[string]*discovery.APIGroupHandler),
		},
	}

	return env
}

// TestResourceManagerExistingCRD 验证 controller 启动前已存在的 CRD 会被加入 discovery。
func TestResourceManagerExistingCRD(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := setup()
	_, err := env.Interface.
		ApiextensionsV1().
		CustomResourceDefinitions().
		Create(
			ctx,
			coolFooCRD,
			metav1.CreateOptions{
				FieldManager: "resource-manager-test",
			},
		)

	require.NoError(t, err)

	env.FakeResourceManager.Expect().
		AddGroupVersion(coolFooCRD.Spec.Group, coolFooDiscovery)
	for _, v := range coolFooCRD.Spec.Versions {
		env.FakeResourceManager.Expect().
			SetGroupVersionPriority(metav1.GroupVersion{Group: coolFooCRD.Spec.Group, Version: v.Name}, 1000, 100)
	}

	env.FakeResourceManager.Expect().
		AddGroupVersion(coolFooCRD.Spec.Group, coolFooDiscovery)
	for _, v := range coolFooCRD.Spec.Versions {
		env.FakeResourceManager.Expect().
			SetGroupVersionPriority(metav1.GroupVersion{Group: coolFooCRD.Spec.Group, Version: v.Name}, 1000, 100)
	}

	env.Start(ctx)
	err = env.FakeResourceManager.WaitForActions(ctx, 1*time.Second)
	require.NoError(t, err)
}

// 验证运行时新增的 CRD 会被 discovery controller 写入 discovery 文档。
func TestResourceManagerAddedCRD(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := setup()
	env.FakeResourceManager.Expect().
		AddGroupVersion(coolFooCRD.Spec.Group, coolFooDiscovery)
	for _, v := range coolFooCRD.Spec.Versions {
		env.FakeResourceManager.Expect().
			SetGroupVersionPriority(metav1.GroupVersion{Group: coolFooCRD.Spec.Group, Version: v.Name}, 1000, 100)
	}

	env.Start(ctx)

	// 在 controller 已启动之后再创建 CRD
	_, err := env.Interface.
		ApiextensionsV1().
		CustomResourceDefinitions().
		Create(
			ctx,
			coolFooCRD,
			metav1.CreateOptions{
				FieldManager: "resource-manager-test",
			},
		)

	require.NoError(t, err)

	err = env.FakeResourceManager.WaitForActions(ctx, 1*time.Second)
	require.NoError(t, err)
}

// TestMultipleCRDSameVersion 验证同一版本下存在多个 CRD 时，两者都会加入 discovery。
func TestMultipleCRDSameVersion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := setup()
	env.Start(ctx)

	_, err := env.Interface.
		ApiextensionsV1().
		CustomResourceDefinitions().
		Create(
			ctx,
			coolFooCRD,
			metav1.CreateOptions{
				FieldManager: "resource-manager-test",
			},
		)

	require.NoError(t, err)
	env.FakeResourceManager.Expect().
		AddGroupVersion(coolFooCRD.Spec.Group, coolFooDiscovery)
	for _, versionEntry := range coolFooCRD.Spec.Versions {
		env.FakeResourceManager.Expect().SetGroupVersionPriority(metav1.GroupVersion{Group: coolFooCRD.Spec.Group, Version: versionEntry.Name}, 1000, 100)
	}
	err = env.FakeResourceManager.WaitForActions(ctx, 1*time.Second)
	require.NoError(t, err)

	_, err = env.Interface.
		ApiextensionsV1().
		CustomResourceDefinitions().
		Create(
			ctx,
			coolBarCRD,
			metav1.CreateOptions{
				FieldManager: "resource-manager-test",
			},
		)
	require.NoError(t, err)

	env.FakeResourceManager.Expect().
		AddGroupVersion(coolFooCRD.Spec.Group, mergedDiscovery)
	for _, versionEntry := range coolFooCRD.Spec.Versions {
		env.FakeResourceManager.Expect().SetGroupVersionPriority(metav1.GroupVersion{Group: coolFooCRD.Spec.Group, Version: versionEntry.Name}, 1000, 100)
	}
	err = env.FakeResourceManager.WaitForActions(ctx, 1*time.Second)
	require.NoError(t, err)
}

// 验证运行时删除 CRD 后，discovery controller 会从 ResourceManager 中移除其信息。
func TestDiscoveryControllerResourceManagerRemovedCRD(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := setup()
	env.Start(ctx)

	// 在 controller 已启动之后再创建 CRD
	_, err := env.Interface.
		ApiextensionsV1().
		CustomResourceDefinitions().
		Create(
			ctx,
			coolFooCRD,
			metav1.CreateOptions{},
		)

	require.NoError(t, err)

	// 等待 controller 接收到 Create 事件并将其加入 ResourceManager
	env.FakeResourceManager.Expect().
		AddGroupVersion(coolFooCRD.Spec.Group, coolFooDiscovery)
	for _, versionEntry := range coolFooCRD.Spec.Versions {
		env.FakeResourceManager.Expect().SetGroupVersionPriority(metav1.GroupVersion{Group: coolFooCRD.Spec.Group, Version: versionEntry.Name}, 1000, 100)
	}
	err = env.FakeResourceManager.WaitForActions(ctx, 1*time.Second)
	require.NoError(t, err)

	err = env.Interface.
		ApiextensionsV1().
		CustomResourceDefinitions().
		Delete(ctx, coolFooCRD.Name, metav1.DeleteOptions{})

	require.NoError(t, err)

	// 等待 controller 发现该组已无 CRD 并移除整个组
	env.FakeResourceManager.Expect().RemoveGroup(coolFooCRD.Spec.Group)

	err = env.FakeResourceManager.WaitForActions(ctx, 1*time.Second)
	require.NoError(t, err)
}
