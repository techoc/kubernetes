/*
Copyright 2025 The Kubernetes Authors.

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

package apis

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kube-openapi/pkg/util"

	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	apiregistrationv1beta1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1beta1"
)

// 把各版本的 SchemeBuilder 汇总到一处，测试就能遍历"所有对外暴露的类型"，
// 而不是靠一份手工维护的类型清单——那样新增类型时很容易漏掉。
var groups = []runtime.SchemeBuilder{
	apiregistrationv1.SchemeBuilder,
	apiregistrationv1beta1.SchemeBuilder,
}

// OpenAPI 模型名是生成代码按"包路径 + 类型名"推导出来的，而这里的期望值
// 用反射重新算了一遍同样的规则。两边一旦不一致，说明某个类型上的
// OpenAPIModelName 实现和实际包路径脱节了，客户端按名字查模型时就会扑空。
// 跳过 internal 版本是因为它不对外暴露，本来就没有 OpenAPI 定义。
func TestOpenAPIDefinitionNames(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, builder := range groups {
		if err := builder.AddToScheme(scheme); err != nil {
			t.Fatalf("unexpected error adding to scheme: %v", err)
		}
	}

	kinds := scheme.AllKnownTypes()
	for gvk := range kinds {
		if gvk.Version == runtime.APIVersionInternal {
			continue
		}
		t.Run(gvk.String(), func(t *testing.T) {
			// 用 scheme.New 造一个空实例，纯粹是为了拿到类型信息和它实现的接口；
			// 这里不关心实例里有没有有效数据。
			example, err := scheme.New(gvk)
			if err != nil {
				t.Fatalf("unexpected error creating example: %v", err)
			}

			// 模型名接口由生成器实现，缺了它意味着这个类型没有进入 OpenAPI 生成流程。
			namer, ok := example.(util.OpenAPIModelNamer)
			if !ok {
				t.Fatalf("type %v does not implement OpenAPICanonicalTypeName\n", gvk)
			}
			lookupName := namer.OpenAPIModelName()

			rtype := reflect.TypeOf(example).Elem()
			reflectName := util.ToRESTFriendlyName(rtype.PkgPath() + "." + rtype.Name())

			if lookupName != reflectName {
				t.Errorf("expected %v, got %v", reflectName, lookupName)
			}
		})
	}
}
