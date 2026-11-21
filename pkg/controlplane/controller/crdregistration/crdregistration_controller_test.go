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

package crdregistration

import (
	"reflect"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdlisters "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	apiregistration "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

// TestHandleVersionUpdate 验证 CRD 版本变更能正确同步 APIService。
func TestHandleVersionUpdate(t *testing.T) {
	tests := []struct {
		name         string
		startingCRDs []*apiextensionsv1.CustomResourceDefinition
		version      schema.GroupVersion

		expectedAdded   []*apiregistration.APIService
		expectedRemoved []string
	}{
		{
			name: "simple add crd",
			startingCRDs: []*apiextensionsv1.CustomResourceDefinition{
				{
					Spec: apiextensionsv1.CustomResourceDefinitionSpec{
						Group: "group.com",
						// Version 字段已弃用，CRD 注册完全不依赖它。
						// 若用户只提供了 version 字段，默认值填充逻辑会补全 Versions。
						Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
							{
								Name:    "v1",
								Served:  true,
								Storage: true,
							},
						},
					},
				},
			},
			version: schema.GroupVersion{Group: "group.com", Version: "v1"},

			expectedAdded: []*apiregistration.APIService{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "v1.group.com"},
					Spec: apiregistration.APIServiceSpec{
						Group:                "group.com",
						Version:              "v1",
						GroupPriorityMinimum: 1000,
						VersionPriority:      100,
					},
				},
			},
		},
		{
			name: "simple remove crd",
			startingCRDs: []*apiextensionsv1.CustomResourceDefinition{
				{
					Spec: apiextensionsv1.CustomResourceDefinitionSpec{
						Group: "group.com",
						Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
							{
								Name:    "v1",
								Served:  true,
								Storage: true,
							},
						},
					},
				},
			},
			version: schema.GroupVersion{Group: "group.com", Version: "v2"},

			expectedRemoved: []string{"v2.group.com"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registration := &fakeAPIServiceRegistration{}
			crdCache := cache.NewIndexer(cache.DeletionHandlingMetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			crdLister := crdlisters.NewCustomResourceDefinitionLister(crdCache)
			c := crdRegistrationController{
				crdLister:              crdLister,
				apiServiceRegistration: registration,
			}
			for i := range test.startingCRDs {
				crdCache.Add(test.startingCRDs[i])
			}

			c.handleVersionUpdate(test.version)

			if !reflect.DeepEqual(test.expectedAdded, registration.added) {
				t.Errorf("%s expected %v, got %v", test.name, test.expectedAdded, registration.added)
			}
			if !reflect.DeepEqual(test.expectedRemoved, registration.removed) {
				t.Errorf("%s expected %v, got %v", test.name, test.expectedRemoved, registration.removed)
			}
		})
	}
}

// fakeAPIServiceRegistration 是测试用的 APIService 注册桩。
type fakeAPIServiceRegistration struct {
	added   []*apiregistration.APIService
	removed []string
}

// AddAPIServiceToSync 记录待注册的 APIService。
func (a *fakeAPIServiceRegistration) AddAPIServiceToSync(in *apiregistration.APIService) {
	a.added = append(a.added, in)
}

// RemoveAPIServiceToSync 记录待移除的 APIService。
func (a *fakeAPIServiceRegistration) RemoveAPIServiceToSync(name string) {
	a.removed = append(a.removed, name)
}
