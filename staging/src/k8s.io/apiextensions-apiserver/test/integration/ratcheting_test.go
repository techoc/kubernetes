/*
Copyright 2023 The Kubernetes Authors.

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

// Package integration_test 包含 apiextensions-apiserver 的集成测试，
// 覆盖 CRD 校验 ratcheting：绕过针对未改动字段的校验，只拦截新引入的非法值。
package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	apiextensionsinternal "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apiextensions-apiserver/pkg/features"
	"k8s.io/apiextensions-apiserver/pkg/registry/customresource"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/cel/environment"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	"k8s.io/utils/ptr"
)

// stringSchema 只约束类型为 string，供测试用例复用。
var stringSchema *apiextensionsv1.JSONSchemaProps = &apiextensionsv1.JSONSchemaProps{
	Type: "string",
}

// stringMapSchema 是值为 string 的 map schema。
var stringMapSchema *apiextensionsv1.JSONSchemaProps = &apiextensionsv1.JSONSchemaProps{
	Type: "object",
	AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
		Schema: stringSchema,
	},
}

// numberSchema 只约束类型为 integer。
var numberSchema *apiextensionsv1.JSONSchemaProps = &apiextensionsv1.JSONSchemaProps{
	Type: "integer",
}

// numbersMapSchema 是值为 integer 的 map schema。
var numbersMapSchema *apiextensionsv1.JSONSchemaProps = &apiextensionsv1.JSONSchemaProps{
	Type: "object",
	AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
		Schema: numberSchema,
	},
}

// ratchetingTestContext 为一个测试操作提供客户端与运行模式；
// StatusSubresource 为 true 时，操作走 status 子资源路径。
type ratchetingTestContext struct {
	*testing.T
	DynamicClient       dynamic.Interface
	APIExtensionsClient clientset.Interface
	StatusSubresource   bool
}

// ratchetingTestOperation 是可组合的测试步骤，Do 负责执行，Description 用于日志。
type ratchetingTestOperation interface {
	Do(ctx *ratchetingTestContext) error
	Description() string
}

// expectError 包装一个操作，要求其返回错误；操作成功反而算失败。
type expectError struct {
	op ratchetingTestOperation
}

// Do 执行被包装的操作，并反转对错误的期望。
func (e expectError) Do(ctx *ratchetingTestContext) error {
	err := e.op.Do(ctx)
	if err != nil {
		return nil
	}
	return errors.New("expected error")
}

// Description 在被包装操作的描述前加前缀，便于定位期望失败的步骤。
func (e expectError) Description() string {
	return fmt.Sprintf("Expect Error: %v", e.op.Description())
}

// apiextensions-apiserver 关闭了 discovery，因此这里硬编码 GVR 到 Kind 的映射
var fakeRESTMapper map[schema.GroupVersionResource]string = map[schema.GroupVersionResource]string{
	myCRDV1Beta1: "MyCoolCRD",
}

// FixTabsOrDie 统计给定 yaml 对象第一行前导 tab 的数量，并从每一行移除同样数量的 tab。
// 若某行的 tab 少于此数量则 panic（这是测试辅助函数）。
//
// 目的是让测试代码中的内联 yaml 更易读。
func FixTabsOrDie(in string) string {
	lines := bytes.Split([]byte(in), []byte{'\n'})
	if len(lines[0]) == 0 && len(lines) > 1 {
		lines = lines[1:]
	}
	// 构造需要移除的 tab 前缀。
	var prefix []byte
	for _, c := range lines[0] {
		if c != '\t' {
			break
		}
		prefix = append(prefix, byte('\t'))
	}
	// 从所有行移除该前缀，否则失败。
	for i := range lines {
		line := lines[i]
		// 最后一行可以为空（末尾 \n 所致）
		if i == len(lines)-1 && len(line) <= len(prefix) && bytes.TrimSpace(line) == nil {
			lines[i] = []byte{}
			break
		}
		if !bytes.HasPrefix(line, prefix) {
			panic(fmt.Errorf("line %d doesn't start with expected number (%d) of tabs: %v", i, len(prefix), string(line)))
		}
		lines[i] = line[len(prefix):]
	}
	joined := string(bytes.Join(lines, []byte{'\n'}))

	// 其余 tab 换成空格，因为 yaml 不接受 tab
	// （假设按 2 空格对齐）
	return strings.ReplaceAll(joined, "\t", "  ")
}

// applyPatchOperation 通过 server-side apply 对 CR 打补丁；patch 可以是 map 或 yaml 字符串。
type applyPatchOperation struct {
	description string
	gvr         schema.GroupVersionResource
	name        string
	patch       interface{}
}

// Do 构造 patch 对象并执行 apply；status 模式下改为 apply 到 status 子资源。
func (a applyPatchOperation) Do(ctx *ratchetingTestContext) error {
	// 从 discovery 查找 GVK
	kind, ok := fakeRESTMapper[a.gvr]
	if !ok {
		return fmt.Errorf("no mapping found for Gvr %v, add entry to fakeRESTMapper", a.gvr)
	}

	patch := &unstructured.Unstructured{}
	if obj, ok := a.patch.(map[string]interface{}); ok {
		patch.Object = runtime.DeepCopyJSON(obj)
	} else if str, ok := a.patch.(string); ok {
		str = FixTabsOrDie(str)
		if err := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(str), len(str)).Decode(&patch.Object); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("invalid patch type: %T", a.patch)
	}

	if ctx.StatusSubresource {
		patch.Object = map[string]interface{}{"status": patch.Object}
	}

	patch.SetKind(kind)
	patch.SetAPIVersion(a.gvr.GroupVersion().String())
	patch.SetName(a.name)
	patch.SetNamespace("default")

	c := ctx.DynamicClient.Resource(a.gvr).Namespace(patch.GetNamespace())
	if ctx.StatusSubresource {
		if _, err := c.Get(context.TODO(), patch.GetName(), metav1.GetOptions{}); apierrors.IsNotFound(err) {
			// ApplyStatus 不会自动创建对象，必须先确保对象存在才能对其 apply status。
			_, err := c.Create(context.TODO(), patch, metav1.CreateOptions{})
			if err != nil {
				return err
			}
		}

		_, err := c.ApplyStatus(context.TODO(), patch.GetName(), patch, metav1.ApplyOptions{FieldManager: "manager"})
		return err
	}
	_, err := c.Apply(context.TODO(), patch.GetName(), patch, metav1.ApplyOptions{FieldManager: "manager"})
	return err
}

// Description 返回该步骤在测试日志中的描述。
func (a applyPatchOperation) Description() string {
	return a.description
}

// updateMyCRDV1Beta1Schema 替换 CRD v1beta1 版本的 schema。
type updateMyCRDV1Beta1Schema struct {
	newSchema *apiextensionsv1.JSONSchemaProps
}

// Do 通过重试直到观察到新 schema 生效；用带随机 UUID 的哨兵字段探测生效时点。
func (u updateMyCRDV1Beta1Schema) Do(ctx *ratchetingTestContext) error {
	var myCRD *apiextensionsv1.CustomResourceDefinition
	var err error = apierrors.NewConflict(schema.GroupResource{}, "", nil)
	for apierrors.IsConflict(err) {
		myCRD, err = ctx.APIExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), myCRDV1Beta1.Resource+"."+myCRDV1Beta1.Group, metav1.GetOptions{})
		if err != nil {
			return err
		}

		// 插入一个哨兵属性，用于探测 schema 何时生效
		sch := u.newSchema.DeepCopy()
		if sch.Properties == nil {
			sch.Properties = map[string]apiextensionsv1.JSONSchemaProps{}
		}

		uuidString := string(uuid.NewUUID())
		sentinelName := "__ratcheting_sentinel_field__"
		sch.Properties[sentinelName] = apiextensionsv1.JSONSchemaProps{
			Type: "string",
			Enum: []apiextensionsv1.JSON{{
				Raw: []byte(`"` + uuidString + `"`),
			}},
		}

		if ctx.StatusSubresource {
			sch = &apiextensionsv1.JSONSchemaProps{
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"status": *sch,
				},
			}
		}

		for _, v := range myCRD.Spec.Versions {
			if v.Name != myCRDV1Beta1.Version {
				continue
			}

			v.Schema.OpenAPIV3Schema = sch
		}

		_, err = ctx.APIExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), myCRD, metav1.UpdateOptions{
			FieldManager: "manager",
		})
		if err != nil {
			return err
		}

		// 持续创建非法 CR 实例，直到返回的错误里出现目标信息为止。
		//
		counter := 0
		return wait.PollUntilContextCancel(context.TODO(), 100*time.Millisecond, true, func(_ context.Context) (done bool, err error) {
			counter += 1
			err = applyPatchOperation{
				gvr:  myCRDV1Beta1,
				name: "sentinel-resource",
				patch: map[string]interface{}{
					sentinelName: fmt.Sprintf("invalid-%d", counter),
				}}.Do(ctx)

			if err == nil {
				return false, errors.New("expected error when creating sentinel resource")
			}

			// 检查返回错误是否包含我们的唯一字符串。UUID 足够唯一，只需判断是否存在。
			if strings.Contains(err.Error(), uuidString) {
				return true, nil
			}

			return false, nil
		})
	}
	return err
}

// Description 返回该步骤在测试日志中的描述。
func (u updateMyCRDV1Beta1Schema) Description() string {
	return "Update CRD schema"
}

// patchMyCRDV1Beta1Schema 对现有 schema 做 JSON merge patch，再交给 updateMyCRDV1Beta1Schema。
type patchMyCRDV1Beta1Schema struct {
	description string
	patch       map[string]interface{}
}

// Do 取回当前 schema，merge patch 后走完整替换流程。
func (p patchMyCRDV1Beta1Schema) Do(ctx *ratchetingTestContext) error {
	patch := p.patch
	if ctx.StatusSubresource {
		patch = map[string]interface{}{
			"properties": map[string]interface{}{
				"status": patch,
			},
		}
	}

	var err error
	patchJSON, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	myCRD, err := ctx.APIExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), myCRDV1Beta1.Resource+"."+myCRDV1Beta1.Group, metav1.GetOptions{})
	if err != nil {
		return err
	}

	for _, v := range myCRD.Spec.Versions {
		if v.Name != myCRDV1Beta1.Version {
			continue
		}

		jsonSchema, err := json.Marshal(v.Schema.OpenAPIV3Schema)
		if err != nil {
			return err
		}

		merged, err := jsonpatch.MergePatch(jsonSchema, patchJSON)
		if err != nil {
			return err
		}

		var parsed apiextensionsv1.JSONSchemaProps
		if err := json.Unmarshal(merged, &parsed); err != nil {
			return err
		}

		return updateMyCRDV1Beta1Schema{
			newSchema: &parsed,
		}.Do(&ratchetingTestContext{
			T:                   ctx.T,
			DynamicClient:       ctx.DynamicClient,
			APIExtensionsClient: ctx.APIExtensionsClient,
			StatusSubresource:   false, // status 子资源已在上面处理过了。
		})
	}

	return fmt.Errorf("could not find version %v in CRD %v", myCRDV1Beta1.Version, myCRD.Name)
}

// Description 返回该步骤在测试日志中的描述。
func (p patchMyCRDV1Beta1Schema) Description() string {
	return p.description
}

// ratchetingTestCase 是可复用的测试用例；同一组操作会分别在普通更新与 status 更新下运行。
type ratchetingTestCase struct {
	Name       string
	Disabled   bool
	Operations []ratchetingTestOperation
	SkipStatus bool
}

// runTests 启动一次测试服务器，建立 CRD，然后逐个执行用例；
// 每个用例额外跑一遍 status 子资源变体，除非 SkipStatus。
func runTests(t *testing.T, cases []ratchetingTestCase) {
	tearDown, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	group := myCRDV1Beta1.Group
	version := myCRDV1Beta1.Version
	resource := myCRDV1Beta1.Resource
	kind := fakeRESTMapper[myCRDV1Beta1]

	myCRD := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: resource + "." + group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: group,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    version,
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"content": {
								Type: "object",
								AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
									Schema: &apiextensionsv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"num": {
								Type: "object",
								AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
									Schema: &apiextensionsv1.JSONSchemaProps{
										Type: "integer",
									},
								},
							},
							"status": {
								Type: "object",
							},
						},
					},
				},
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
				},
			}},
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   resource,
				Kind:     kind,
				ListKind: kind + "List",
			},
			Scope: apiextensionsv1.NamespaceScoped,
		},
	}

	_, err = fixtures.CreateNewV1CustomResourceDefinition(myCRD, apiExtensionClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		if c.Disabled {
			continue
		}

		run := func(t *testing.T, ctx *ratchetingTestContext) {
			for i, op := range c.Operations {
				t.Logf("Performing Operation: %v", op.Description())
				if err := op.Do(ctx); err != nil {
					t.Fatalf("failed %T operation %v: %v\n%v", op, i, err, op)
				}
			}

			// 重置资源，避免用例之间互相影响。
			err := ctx.DynamicClient.Resource(myCRDV1Beta1).Namespace("default").DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
		}

		t.Run(c.Name, func(t *testing.T) {
			run(t, &ratchetingTestContext{
				T:                   t,
				DynamicClient:       dynamicClient,
				APIExtensionsClient: apiExtensionClient,
			})
		})

		// 同一组操作再在 status 子资源下跑一遍，确保 ratcheting 对两条写入路径一致。
		if !c.SkipStatus {
			t.Run("Status: "+c.Name, func(t *testing.T) {
				run(t, &ratchetingTestContext{
					T:                   t,
					DynamicClient:       dynamicClient,
					APIExtensionsClient: apiExtensionClient,
					StatusSubresource:   true,
				})
			})
		}
	}
}

// myCRDV1Beta1 是所有用例共享的 CRD 的 GVR。
var myCRDV1Beta1 schema.GroupVersionResource = schema.GroupVersionResource{
	Group:    "mygroup.example.com",
	Version:  "v1beta1",
	Resource: "mycrds",
}

// myCRDInstanceName 是各用例里被反复 patch 的 CR 名称。
var myCRDInstanceName string = "mycrdinstance"

// TestRatchetingFunctionality 覆盖各类 schema 约束在 ratcheting 下的接受/拒绝行为。
func TestRatchetingFunctionality(t *testing.T) {
	cases := []ratchetingTestCase{
		{
			Name: "Minimum Maximum",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"hasMinimum":           *numberSchema,
						"hasMaximum":           *numberSchema,
						"hasMinimumAndMaximum": *numberSchema,
					},
				}},
				applyPatchOperation{
					"Create an object that complies with the schema",
					myCRDV1Beta1,
					myCRDInstanceName,
					map[string]interface{}{
						"hasMinimum":           int64(0),
						"hasMaximum":           int64(1000),
						"hasMinimumAndMaximum": int64(50),
					}},
				patchMyCRDV1Beta1Schema{
					"Add stricter minimums and maximums that violate the previous object",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"hasMinimum": map[string]interface{}{
								"minimum": int64(10),
							},
							"hasMaximum": map[string]interface{}{
								"maximum": int64(20),
							},
							"hasMinimumAndMaximum": map[string]interface{}{
								"minimum": int64(10),
								"maximum": int64(20),
							},
							"noRestrictions": map[string]interface{}{
								"type": "integer",
							},
						},
					}},
				applyPatchOperation{
					"Add new fields that validates successfully without changing old ones",
					myCRDV1Beta1,
					myCRDInstanceName,
					map[string]interface{}{
						"noRestrictions": int64(50),
					}},
				expectError{
					applyPatchOperation{
						"Change a single old field to be invalid",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"hasMinimum": int64(5),
						}},
				},
				expectError{
					applyPatchOperation{
						"Change multiple old fields to be invalid",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"hasMinimum": int64(5),
							"hasMaximum": int64(21),
						}},
				},
				applyPatchOperation{
					"Change single old field to be valid",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"hasMinimum": int64(11),
					}},
				applyPatchOperation{
					"Change multiple old fields to be valid",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"hasMaximum":           int64(19),
						"hasMinimumAndMaximum": int64(15),
					}},
			},
		},
		{
			Name: "Enum",
			Operations: []ratchetingTestOperation{
				// 创建带若干 enum 取值的 schema
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"enumField": *stringSchema,
					},
				}},
				applyPatchOperation{
					"Create an instance with a soon-to-be-invalid value",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"enumField": "okValueNowBadValueLater",
					}},
				patchMyCRDV1Beta1Schema{
					"restrict `enumField` to an enum of A, B, or C",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"enumField": map[string]interface{}{
								"enum": []interface{}{
									"A", "B", "C",
								},
							},
							"otherField": map[string]interface{}{
								"type": "string",
							},
						},
					}},
				applyPatchOperation{
					"An invalid patch with no changes is a noop",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"enumField": "okValueNowBadValueLater",
					}},
				applyPatchOperation{
					"Add a new field, and include old value in our patch",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"enumField":  "okValueNowBadValueLater",
						"otherField": "anythingGoes",
					}},
				expectError{
					applyPatchOperation{
						"Set enumField to invalid value D",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"enumField": "D",
						}},
				},
				applyPatchOperation{
					"Set to a valid value",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"enumField": "A",
					}},
				expectError{
					applyPatchOperation{
						"After setting a valid value, return to the old, accepted value",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"enumField": "okValueNowBadValueLater",
						}},
				},
			},
		},
		{
			Name: "AdditionalProperties",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"nums":    *numbersMapSchema,
						"content": *stringMapSchema,
					},
				}},
				applyPatchOperation{
					"Create an instance",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"nums": map[string]interface{}{
							"num1": int64(1),
							"num2": int64(1000000),
						},
						"content": map[string]interface{}{
							"k1": "some content",
							"k2": "other content",
						},
					}},
				patchMyCRDV1Beta1Schema{
					"set minimum value for fields with additionalProperties",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"nums": map[string]interface{}{
								"additionalProperties": map[string]interface{}{
									"minimum": int64(1000),
								},
							},
						},
					}},
				applyPatchOperation{
					"updating validating field num2 to another validating value, but rachet invalid field num1",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"nums": map[string]interface{}{
							"num1": int64(1),
							"num2": int64(2000),
						},
					}},
				expectError{applyPatchOperation{
					"update field num1 to different invalid value",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"nums": map[string]interface{}{
							"num1": int64(2),
							"num2": int64(2000),
						},
					}}},
			},
		},
		{
			Name: "MinProperties MaxProperties",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"restricted": {
							Type: "object",
							AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
								Schema: stringSchema,
							},
						},
						"unrestricted": {
							Type: "object",
							AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
								Schema: stringSchema,
							},
						},
					},
				}},
				applyPatchOperation{
					"Create instance",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"restricted": map[string]interface{}{
							"key1": "hi",
							"key2": "there",
						},
					}},
				patchMyCRDV1Beta1Schema{
					"set both minProperties and maxProperties to 1 to violate the previous object",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"restricted": map[string]interface{}{
								"minProperties": int64(1),
								"maxProperties": int64(1),
							},
						},
					}},
				applyPatchOperation{
					"ratchet violating object 'restricted' around changes to unrelated field",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"restricted": map[string]interface{}{
							"key1": "hi",
							"key2": "there",
						},
						"unrestricted": map[string]interface{}{
							"key1": "yo",
						},
					}},
				expectError{applyPatchOperation{
					"make invalid changes to previously ratcheted invalid field",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"restricted": map[string]interface{}{
							"key1": "changed",
							"key2": "there",
						},
						"unrestricted": map[string]interface{}{
							"key1": "yo",
						},
					}}},

				patchMyCRDV1Beta1Schema{
					"remove maxProeprties, set minProperties to 2",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"restricted": map[string]interface{}{
								"minProperties": int64(2),
								"maxProperties": nil,
							},
						},
					}},
				applyPatchOperation{
					"a new value",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"restricted": map[string]interface{}{
							"key1": "hi",
							"key2": "there",
							"key3": "buddy",
						},
					}},

				expectError{applyPatchOperation{
					"violate new validation by removing keys",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"restricted": map[string]interface{}{
							"key1": "hi",
							"key2": nil,
							"key3": nil,
						},
					}}},
				patchMyCRDV1Beta1Schema{
					"remove minProperties, set maxProperties to 1",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"restricted": map[string]interface{}{
								"minProperties": nil,
								"maxProperties": int64(1),
							},
						},
					}},
				applyPatchOperation{
					"modify only the other key, ratcheting maxProperties for field `restricted`",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"restricted": map[string]interface{}{
							"key1": "hi",
							"key2": "there",
							"key3": "buddy",
						},
						"unrestricted": map[string]interface{}{
							"key1": "value",
							"key2": "value",
						},
					}},
				expectError{
					applyPatchOperation{
						"modifying one value in the object with maxProperties restriction, but keeping old fields",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"restricted": map[string]interface{}{
								"key1": "hi",
								"key2": "theres",
								"key3": "buddy",
							},
						}}},
			},
		},
		{
			Name: "MinItems",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"field": *stringSchema,
						"array": {
							Type: "array",
							Items: &apiextensionsv1.JSONSchemaPropsOrArray{
								Schema: stringSchema,
							},
						},
					},
				}},
				applyPatchOperation{
					"Create instance",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"array": []interface{}{"value1", "value2", "value3"},
					}},
				patchMyCRDV1Beta1Schema{
					"change minItems on array to 10, invalidates previous object",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"array": map[string]interface{}{
								"minItems": int64(10),
							},
						},
					}},
				applyPatchOperation{
					"keep invalid field `array` unchanged, add new field with ratcheting",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"array": []interface{}{"value1", "value2", "value3"},
						"field": "value",
					}},
				expectError{
					applyPatchOperation{
						"modify array element without satisfying property",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"array": []interface{}{"value2", "value2", "value3"},
						}}},

				expectError{
					applyPatchOperation{
						"add array element without satisfying proeprty",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"array": []interface{}{"value1", "value2", "value3", "value4"},
						}}},

				applyPatchOperation{
					"make array valid",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"array": []interface{}{"value1", "value2", "value3", "4", "5", "6", "7", "8", "9", "10"},
					}},
				expectError{
					applyPatchOperation{
						"revert to original value",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"array": []interface{}{"value1", "value2", "value3"},
						}}},
			},
		},
		{
			Name: "MaxItems",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"field": *stringSchema,
						"array": {
							Type: "array",
							Items: &apiextensionsv1.JSONSchemaPropsOrArray{
								Schema: stringSchema,
							},
						},
					},
				}},
				applyPatchOperation{
					"create instance",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"array": []interface{}{"value1", "value2", "value3"},
					}},
				patchMyCRDV1Beta1Schema{
					"change maxItems on array to 1, invalidates previous object",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"array": map[string]interface{}{
								"maxItems": int64(1),
							},
						},
					}},
				applyPatchOperation{
					"ratchet old value of array through an update to another field",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"array": []interface{}{"value1", "value2", "value3"},
						"field": "value",
					}},
				expectError{
					applyPatchOperation{
						"modify array element without satisfying property",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"array": []interface{}{"value2", "value2", "value3"},
						}}},

				expectError{
					applyPatchOperation{
						"remove array element without satisfying proeprty",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"array": []interface{}{"value1", "value2"},
						}}},

				applyPatchOperation{
					"change array to valid value that satisfies maxItems",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"array": []interface{}{"value1"},
					}},
				expectError{
					applyPatchOperation{
						"revert to previous invalid ratcheted value",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"array": []interface{}{"value1", "value2", "value3"},
						}}},
			},
		},
		{
			Name: "MinLength MaxLength",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"minField":   *stringSchema,
						"maxField":   *stringSchema,
						"otherField": *stringSchema,
					},
				}},
				applyPatchOperation{
					"create instance",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"minField": "value",
						"maxField": "valueThatsVeryLongSee",
					}},
				patchMyCRDV1Beta1Schema{
					"set minField maxLength to 10, and maxField's minLength to 15",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"minField": map[string]interface{}{
								"minLength": int64(10),
							},
							"maxField": map[string]interface{}{
								"maxLength": int64(15),
							},
						},
					}},
				applyPatchOperation{
					"add new field `otherField`, ratcheting `minField` and `maxField`",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"minField":   "value",
						"maxField":   "valueThatsVeryLongSee",
						"otherField": "otherValue",
					}},
				applyPatchOperation{
					"make minField valid, ratcheting old value for maxField",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"minField":   "valuelength13",
						"maxField":   "valueThatsVeryLongSee",
						"otherField": "otherValue",
					}},
				applyPatchOperation{
					"make maxField shorter",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"maxField": "l2",
					}},
				expectError{
					applyPatchOperation{
						"make maxField too long",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"maxField": "valuewithlength17",
						}}},
				expectError{
					applyPatchOperation{
						"revert minFIeld to previously ratcheted value",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"minField": "value",
						}}},
				expectError{
					applyPatchOperation{
						"revert maxField to previously ratcheted value",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"maxField": "valueThatsVeryLongSee",
						}}},
			},
		},
		{
			Name: "Pattern",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"field": *stringSchema,
					},
				}},
				applyPatchOperation{
					"create instance",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": "doesnt abide pattern",
					}},
				patchMyCRDV1Beta1Schema{
					"add pattern validation on `field`",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"field": map[string]interface{}{
								"pattern": "^[1-9]+$",
							},
							"otherField": map[string]interface{}{
								"type": "string",
							},
						},
					}},
				applyPatchOperation{
					"add unrelated field, ratcheting old invalid field",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field":      "doesnt abide pattern",
						"otherField": "added",
					}},
				expectError{applyPatchOperation{
					"change field to invalid value",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field":      "w123",
						"otherField": "added",
					}}},
				applyPatchOperation{
					"change field to a valid value",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field":      "123",
						"otherField": "added",
					}},
			},
		},
		{
			Name: "Format Addition and Change",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"field": *stringSchema,
					},
				}},
				applyPatchOperation{
					"create instance",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": "doesnt abide any format",
					}},
				patchMyCRDV1Beta1Schema{
					"change `field`'s format to `byte",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"field": map[string]interface{}{
								"format": "byte",
							},
							"otherField": map[string]interface{}{
								"type": "string",
							},
						},
					}},
				applyPatchOperation{
					"add unrelated otherField, ratchet invalid old field format",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field":      "doesnt abide any format",
						"otherField": "value",
					}},
				expectError{applyPatchOperation{
					"change field to an invalid string",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": "asd",
					}}},
				applyPatchOperation{
					"change field to a valid byte string",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": "dGhpcyBpcyBwYXNzd29yZA==",
					}},
				patchMyCRDV1Beta1Schema{
					"change `field`'s format to date-time",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"field": map[string]interface{}{
								"format": "date-time",
							},
						},
					}},
				applyPatchOperation{
					"change otherField, ratchet `field`'s invalid byte format",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field":      "dGhpcyBpcyBwYXNzd29yZA==",
						"otherField": "value2",
					}},
				applyPatchOperation{
					"change `field` to a valid value",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field":      "2018-11-13T20:20:39+00:00",
						"otherField": "value2",
					}},
				expectError{
					applyPatchOperation{
						"revert `field` to previously ratcheted value",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"field":      "dGhpcyBpcyBwYXNzd29yZA==",
							"otherField": "value2",
						}}},
				expectError{
					applyPatchOperation{
						"revert `field` to its initial value from creation",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"field": "doesnt abide any format",
						}}},
			},
		},
		{
			Name: "Map Type List Reordering Grandfathers Invalid Key",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"field": {
							Type:         "array",
							XListType:    ptr.To("map"),
							XListMapKeys: []string{"name", "port"},
							Items: &apiextensionsv1.JSONSchemaPropsOrArray{
								Schema: &apiextensionsv1.JSONSchemaProps{
									Type:     "object",
									Required: []string{"name", "port"},
									Properties: map[string]apiextensionsv1.JSONSchemaProps{
										"name":  *stringSchema,
										"port":  *numberSchema,
										"field": *stringSchema,
									},
								},
							},
						},
					},
				}},
				applyPatchOperation{
					"create instance with three soon-to-be-invalid keys",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": []interface{}{
							map[string]interface{}{
								"name":  "nginx",
								"port":  int64(443),
								"field": "value",
							},
							map[string]interface{}{
								"name":  "etcd",
								"port":  int64(2379),
								"field": "value",
							},
							map[string]interface{}{
								"name":  "kube-apiserver",
								"port":  int64(6443),
								"field": "value",
							},
						},
					}},
				patchMyCRDV1Beta1Schema{
					"set `field`'s maxItems to 2, which is exceeded by all of previous object's elements",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"field": map[string]interface{}{
								"maxItems": int64(2),
							},
						},
					}},
				applyPatchOperation{
					"reorder invalid objects which have too many properties, but do not modify them or change keys",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": []interface{}{
							map[string]interface{}{
								"name":  "kube-apiserver",
								"port":  int64(6443),
								"field": "value",
							},
							map[string]interface{}{
								"name":  "nginx",
								"port":  int64(443),
								"field": "value",
							},
							map[string]interface{}{
								"name":  "etcd",
								"port":  int64(2379),
								"field": "value",
							},
						},
					}},
				expectError{
					applyPatchOperation{
						"attempt to change one of the fields of the items which exceed maxItems",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"field": []interface{}{
								map[string]interface{}{
									"name":  "kube-apiserver",
									"port":  int64(6443),
									"field": "value",
								},
								map[string]interface{}{
									"name":  "nginx",
									"port":  int64(443),
									"field": "value",
								},
								map[string]interface{}{
									"name":  "etcd",
									"port":  int64(2379),
									"field": "value",
								},
								map[string]interface{}{
									"name":  "dev",
									"port":  int64(8080),
									"field": "value",
								},
							},
						}}},
				patchMyCRDV1Beta1Schema{
					"Require even numbered port in key, remove maxItems requirement",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"field": map[string]interface{}{
								"maxItems": nil,
								"items": map[string]interface{}{
									"properties": map[string]interface{}{
										"port": map[string]interface{}{
											"multipleOf": int64(2),
										},
									},
								},
							},
						},
					}},

				applyPatchOperation{
					"reorder fields without changing anything",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": []interface{}{
							map[string]interface{}{
								"name":  "nginx",
								"port":  int64(443),
								"field": "value",
							},
							map[string]interface{}{
								"name":  "etcd",
								"port":  int64(2379),
								"field": "value",
							},
							map[string]interface{}{
								"name":  "kube-apiserver",
								"port":  int64(6443),
								"field": "value",
							},
						},
					}},

				applyPatchOperation{
					`use "invalid" keys despite changing order and changing sibling fields to the key`,
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": []interface{}{
							map[string]interface{}{
								"name":  "nginx",
								"port":  int64(443),
								"field": "value",
							},
							map[string]interface{}{
								"name":  "etcd",
								"port":  int64(2379),
								"field": "value",
							},
							map[string]interface{}{
								"name":  "kube-apiserver",
								"port":  int64(6443),
								"field": "this is a changed value for an an invalid but grandfathered key",
							},
							map[string]interface{}{
								"name":  "dev",
								"port":  int64(8080),
								"field": "value",
							},
						},
					}},
			},
		},
		{
			Name: "ArrayItems do not correlate by index",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"values": {
							Type: "array",
							Items: &apiextensionsv1.JSONSchemaPropsOrArray{
								Schema: stringMapSchema,
							},
						},
						"otherField": *stringSchema,
					},
				}},
				applyPatchOperation{
					"create instance with length 5 values",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"values": []interface{}{
							map[string]interface{}{
								"name": "1",
								"key":  "value",
							},
							map[string]interface{}{
								"name": "2",
								"key":  "value",
							},
						},
					}},
				patchMyCRDV1Beta1Schema{
					"Set minimum length of 6 for values of elements in the items array",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"values": map[string]interface{}{
								"items": map[string]interface{}{
									"additionalProperties": map[string]interface{}{
										"minLength": int64(6),
									},
								},
							},
						},
					}},
				expectError{
					applyPatchOperation{
						"change value to one that exceeds minLength",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"values": []interface{}{
								map[string]interface{}{
									"name": "1",
									"key":  "value",
								},
								map[string]interface{}{
									"name": "2",
									"key":  "bad",
								},
							},
						}}},
				applyPatchOperation{
					"add new fields without touching the map",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"values": []interface{}{
							map[string]interface{}{
								"name": "1",
								"key":  "value",
							},
							map[string]interface{}{
								"name": "2",
								"key":  "value",
							},
						},
						"otherField": "hello world",
					}},
				// （本用例说明：由于其 list 是 atomic 的，数组无法按索引与旧值建立对应关系）
				expectError{applyPatchOperation{
					"add new, valid fields to elements of the array, failing to ratchet unchanged old fields within the array elements by correlating by index due to atomic list",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"values": []interface{}{
							map[string]interface{}{
								"name": "1",
								"key":  "value",
							},
							map[string]interface{}{
								"name": "2",
								"key":  "value",
								"key2": "valid value",
							},
						},
					}}},
				expectError{
					applyPatchOperation{
						"reorder the array, preventing index correlation",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"values": []interface{}{
								map[string]interface{}{
									"name": "2",
									"key":  "value",
									"key2": "valid value",
								},
								map[string]interface{}{
									"name": "1",
									"key":  "value",
								},
							},
						}}},
			},
		},
		{
			Name:       "CEL Optional OldSelf",
			SkipStatus: true, // status 更新时 oldSelf 永远不会为 null。
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"field": {
							Type: "string",
							XValidations: []apiextensionsv1.ValidationRule{
								{
									Rule:            "!oldSelf.hasValue()",
									Message:         "oldSelf must be null",
									OptionalOldSelf: ptr.To(true),
								},
							},
						},
					},
				}},

				applyPatchOperation{
					"create instance passes since oldself is null",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": "value",
					}},

				expectError{
					applyPatchOperation{
						"update field fails, since oldself is not null",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"field": "value2",
						},
					},
				},

				expectError{
					applyPatchOperation{
						"noop update field fails, since oldself is not null and transition rules are not ratcheted",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"field": "value",
						},
					},
				},
			},
		},
		// 以下特性不应参与 ratcheting
		{
			Name: "AllOf_should_not_ratchet",
		},
		{
			Name: "OneOf_should_not_ratchet",
		},
		{
			Name: "AnyOf_should_not_ratchet",
		},
		{
			Name: "Not_should_not_ratchet",
		},
		{
			Name: "CEL_transition_rules_should_not_ratchet",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type:                   "object",
					XPreserveUnknownFields: ptr.To(true),
				}},
				applyPatchOperation{
					"create instance with strings that do not start with k8s",
					myCRDV1Beta1, myCRDInstanceName,
					`
						myStringField: myStringValue
						myOtherField: myOtherField
					`,
				},
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type:                   "object",
					XPreserveUnknownFields: ptr.To(true),
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"myStringField": {
							Type: "string",
							XValidations: apiextensionsv1.ValidationRules{
								{
									Rule: "oldSelf != 'myStringValue' || self == 'validstring'",
								},
							},
						},
					},
				}},
				expectError{applyPatchOperation{
					"try to change one field to valid value, but unchanged field fails to be ratcheted by transition rule",
					myCRDV1Beta1, myCRDInstanceName,
					`
						myOtherField: myNewOtherField
						myStringField: myStringValue
					`,
				}},
				applyPatchOperation{
					"change both fields to valid values",
					myCRDV1Beta1, myCRDInstanceName,
					`
						myStringField: validstring
						myOtherField: myNewOtherField
					`,
				},
			},
		},
		// 未来功能，暂时禁用的用例
		{
			Name: "CEL Add Change Rule",
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"field": {
							Type: "object",
							AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
								Schema: &apiextensionsv1.JSONSchemaProps{
									Type: "object",
									Properties: map[string]apiextensionsv1.JSONSchemaProps{
										"stringField":   *stringSchema,
										"intField":      *numberSchema,
										"otherIntField": *numberSchema,
									},
								},
							},
						},
					},
				}},
				applyPatchOperation{
					"create instance with strings that do not start with k8s",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": map[string]interface{}{
							"object1": map[string]interface{}{
								"stringField": "a string",
								"intField":    int64(5),
							},
							"object2": map[string]interface{}{
								"stringField": "another string",
								"intField":    int64(15),
							},
							"object3": map[string]interface{}{
								"stringField": "a third string",
								"intField":    int64(7),
							},
						},
					}},
				patchMyCRDV1Beta1Schema{
					"require that stringField value start with `k8s`",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"field": map[string]interface{}{
								"additionalProperties": map[string]interface{}{
									"properties": map[string]interface{}{
										"stringField": map[string]interface{}{
											"x-kubernetes-validations": []interface{}{
												map[string]interface{}{
													"rule":    "self.startsWith('k8s')",
													"message": "strings must have k8s prefix",
												},
											},
										},
									},
								},
							},
						},
					}},
				applyPatchOperation{
					"add a new entry that follows the new rule, ratchet old values",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": map[string]interface{}{
							"object1": map[string]interface{}{
								"stringField": "a string",
								"intField":    int64(5),
							},
							"object2": map[string]interface{}{
								"stringField": "another string",
								"intField":    int64(15),
							},
							"object3": map[string]interface{}{
								"stringField": "a third string",
								"intField":    int64(7),
							},
							"object4": map[string]interface{}{
								"stringField": "k8s third string",
								"intField":    int64(7),
							},
						},
					}},
				applyPatchOperation{
					"modify a sibling to an invalid value, ratcheting the unchanged invalid value",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": map[string]interface{}{
							"object1": map[string]interface{}{
								"stringField": "a string",
								"intField":    int64(15),
							},
							"object2": map[string]interface{}{
								"stringField":   "another string",
								"intField":      int64(10),
								"otherIntField": int64(20),
							},
						},
					}},
				expectError{
					applyPatchOperation{
						"change a previously ratcheted field to an invalid value",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"field": map[string]interface{}{
								"object2": map[string]interface{}{
									"stringField": "a changed string",
								},
								"object3": map[string]interface{}{
									"stringField": "a changed third string",
								},
							},
						}}},
				patchMyCRDV1Beta1Schema{
					"require that stringField values are also odd length",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"field": map[string]interface{}{
								"additionalProperties": map[string]interface{}{
									"stringField": map[string]interface{}{
										"x-kubernetes-validations": []interface{}{
											map[string]interface{}{
												"rule":    "self.startsWith('k8s')",
												"message": "strings must have k8s prefix",
											},
											map[string]interface{}{
												"rule":    "len(self) % 2 == 1",
												"message": "strings must have odd length",
											},
										},
									},
								},
							},
						},
					}},
				applyPatchOperation{
					"have mixed ratcheting of one or two CEL rules, object4 is ratcheted by one rule, object1 is ratcheting 2 rules",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": map[string]interface{}{
							"object1": map[string]interface{}{
								"stringField": "a string", // 非法：长度为偶数且无 k8s 前缀
								"intField":    int64(1000),
							},
							"object4": map[string]interface{}{
								"stringField": "k8s third string", // 非法：长度为偶数，但已被 ratchet
								"intField":    int64(7000),
							},
						},
					}},
				expectError{
					applyPatchOperation{
						"swap keys between valuesin the map",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"field": map[string]interface{}{
								"object1": map[string]interface{}{
									"stringField": "k8s third string",
									"intField":    int64(1000),
								},
								"object4": map[string]interface{}{
									"stringField": "a string",
									"intField":    int64(7000),
								},
							},
						}}},
				applyPatchOperation{
					"fix keys",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"field": map[string]interface{}{
							"object1": map[string]interface{}{
								"stringField": "k8s a stringy",
								"intField":    int64(1000),
							},
							"object4": map[string]interface{}{
								"stringField": "k8s third stringy",
								"intField":    int64(7000),
							},
						},
					}},
			},
		},
		{
			// 把 list 改为 set 时应允许保留原有元素，但只要改动其中任一元素，set 就必须去重。
			//
			// 这可能是未来可改进的点。目前 SSA 实现与该字段的 ratcheting 不兼容：
			// https://github.com/kubernetes/kubernetes/blob/ec9a8ffb237e391ce9ccc58de93ba4ecc2fabf42/staging/src/k8s.io/apimachinery/pkg/util/managedfields/internal/structuredmerge.go#L146-L149
			//
			// 会把已存在的非法 `liveObj` 当作 set 解释并报错。
			Name:     "Change list to set",
			Disabled: true,
			Operations: []ratchetingTestOperation{
				updateMyCRDV1Beta1Schema{&apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"values": {
							Type: "object",
							AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
								Schema: &apiextensionsv1.JSONSchemaProps{
									Type: "array",
									Items: &apiextensionsv1.JSONSchemaPropsOrArray{
										Schema: numberSchema,
									},
								},
							},
						},
					},
				}},
				applyPatchOperation{
					"reate a list of numbers with duplicates using the old simple schema",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"values": map[string]interface{}{
							"dups": []interface{}{int64(1), int64(2), int64(2), int64(3), int64(1000), int64(2000)},
						},
					}},
				patchMyCRDV1Beta1Schema{
					"change list type to set",
					map[string]interface{}{
						"properties": map[string]interface{}{
							"values": map[string]interface{}{
								"additionalProperties": map[string]interface{}{
									"x-kubernetes-list-type": "set",
								},
							},
						},
					}},
				expectError{
					applyPatchOperation{
						"change original without removing duplicates",
						myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
							"values": map[string]interface{}{
								"dups": []interface{}{int64(1), int64(2), int64(2), int64(3), int64(1000), int64(2000), int64(3)},
							},
						}}},
				expectError{applyPatchOperation{
					"add another list with duplicates",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"values": map[string]interface{}{
							"dups":  []interface{}{int64(1), int64(2), int64(2), int64(3), int64(1000), int64(2000)},
							"dups2": []interface{}{int64(1), int64(2), int64(2), int64(3), int64(1000), int64(2000)},
						},
					}}},
				// 可以新增合法的同级字段
				// ! 等我们支持对 list 类型做 ratcheting 后，移除此 ExpectError
				applyPatchOperation{
					"add a valid sibling field",
					myCRDV1Beta1, myCRDInstanceName, map[string]interface{}{
						"values": map[string]interface{}{
							"dups":       []interface{}{int64(1), int64(2), int64(2), int64(3), int64(1000), int64(2000)},
							"otherField": []interface{}{int64(1), int64(2), int64(3)},
						},
					}},
				// 可以去掉重复项使 list 合法
				// ! 正常情况下这应当合法，但 SSA 无法用新 schema 解释 `liveObj`，因此失败。
				// 把 x-kubernetes-list-type 从任意值改为 set 不受 SSA 支持。
				applyPatchOperation{
					"remove dups to make list valid",
					myCRDV1Beta1,
					myCRDInstanceName,
					map[string]interface{}{
						"values": map[string]interface{}{
							"dups":       []interface{}{int64(1), int64(3), int64(1000), int64(2000)},
							"otherField": []interface{}{int64(1), int64(2), int64(3)},
						},
					}},
			},
		},
	}

	runTests(t, cases)
}

// validator 对一个 update 做校验；不关心结果，只用于 benchmark 制造负载。
type validator func(new, old *unstructured.Unstructured)

// newValidator 按 customResourceStrategy 的方式构造只用于校验的 strategy。
func newValidator(customResourceValidation *apiextensionsinternal.JSONSchemaProps, kind schema.GroupVersionKind, namespaceScoped bool) (validator, error) {
	// 复刻 customResourceStrategy 的校验逻辑
	openapiSchema := &spec.Schema{}
	if customResourceValidation != nil {
		// TODO: 替换为 NewStructural(...).ToGoOpenAPI
		formatPostProcessor := apiservervalidation.StripUnsupportedFormatsPostProcessorForVersion(environment.DefaultCompatibilityVersion())
		if err := apiservervalidation.ConvertJSONSchemaPropsWithPostProcess(customResourceValidation, openapiSchema, formatPostProcessor); err != nil {
			return nil, err
		}
	}

	schemaValidator := apiservervalidation.NewRatchetingSchemaValidator(
		openapiSchema,
		nil,
		"",
		strfmt.Default)
	sts, err := structuralschema.NewStructural(customResourceValidation)
	if err != nil {
		return nil, err
	}

	strategy := customresource.NewStrategy(
		nil, // 不需要 typer，只做校验
		namespaceScoped,
		kind,
		schemaValidator,
		nil, // 不需要 status schema validator
		sts,
		nil, // 不需要 status
		nil, // 不需要 scale
		nil, // 不需要 selectable fields
	)

	return func(new, old *unstructured.Unstructured) {
		_ = strategy.ValidateUpdate(context.TODO(), new, old)
	}, nil
}

// loadObjects 递归遍历目录，把 YAML 文件解析为 unstructured 对象。
// 单个文件包含多个对象时，全部加入返回的切片。
func loadObjects(dir string) []*unstructured.Unstructured {
	result := []*unstructured.Unstructured{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		} else if d.IsDir() {
			return nil
		} else if filepath.Ext(d.Name()) != ".yaml" {
			return nil
		}
		// 读取文件内容为 []byte
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)

		// 按 YAML 文档分隔符拆分数据
		for {
			parsed := &unstructured.Unstructured{}
			if err := decoder.Decode(parsed); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}

			result = append(result, parsed)
		}

		return nil
	})
	if err != nil {
		panic(err)
	}
	return result
}

// BenchmarkRatcheting 用文件里的 CRD 与合法/非法对象两两组合，压测 ratcheting 校验路径。
func BenchmarkRatcheting(b *testing.B) {
	// 遍历存放 CRD 的目录；每个文件可能包含多个 CRD 的 YAML。
	// 用 map 记录每个唯一 GVK 对应的 validator。
	crdObjects := loadObjects("ratcheting_test_cases/crds")
	invalidFiles := loadObjects("ratcheting_test_cases/invalid")
	validFiles := loadObjects("ratcheting_test_cases/valid")

	// 为每个 GVK 建一个 validator。
	validators := map[schema.GroupVersionKind]validator{}
	for _, crd := range crdObjects {
		parsed := apiextensionsv1.CustomResourceDefinition{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(crd.Object, &parsed); err != nil {
			b.Fatalf("Failed to parse CRD %v", err)
			return
		}

		for _, v := range parsed.Spec.Versions {
			gvk := schema.GroupVersionKind{
				Group:   parsed.Spec.Group,
				Version: v.Name,
				Kind:    parsed.Spec.Names.Kind,
			}

			// 从 v.Schema.OpenAPIV3Schema 构造 structural schema
			internalValidation := &apiextensionsinternal.CustomResourceValidation{}
			if err := apiextensionsv1.Convert_v1_CustomResourceValidation_To_apiextensions_CustomResourceValidation(v.Schema, internalValidation, nil); err != nil {
				b.Fatal(fmt.Errorf("failed converting CRD validation to internal version: %v", err))
				return
			}

			validator, err := newValidator(internalValidation.OpenAPIV3Schema, gvk, parsed.Spec.Scope == apiextensionsv1.NamespaceScoped)
			if err != nil {
				b.Fatal(err)
				return
			}
			validators[gvk] = validator
		}

	}

	// 按 GVK 归类所有文件。
	gvksToValidFiles := map[schema.GroupVersionKind][]*unstructured.Unstructured{}
	gvksToInvalidFiles := map[schema.GroupVersionKind][]*unstructured.Unstructured{}

	for _, valid := range validFiles {
		gvk := valid.GroupVersionKind()
		gvksToValidFiles[gvk] = append(gvksToValidFiles[gvk], valid)
	}

	for _, invalid := range invalidFiles {
		gvk := invalid.GroupVersionKind()
		gvksToInvalidFiles[gvk] = append(gvksToInvalidFiles[gvk], invalid)
	}

	// 删掉缺少合法或非法任一侧文件的 GVK。
	for gvk := range gvksToValidFiles {
		if _, ok := gvksToInvalidFiles[gvk]; !ok {
			delete(gvksToValidFiles, gvk)
		}
	}

	for gvk := range gvksToInvalidFiles {
		if _, ok := gvksToValidFiles[gvk]; !ok {
			delete(gvksToInvalidFiles, gvk)
		}
	}

	type pair struct {
		old *unstructured.Unstructured
		new *unstructured.Unstructured
	}

	// 对每个合法文件，与同 GVK 的每个非法文件配对
	validXValidPairs := []pair{}
	validXInvalidPairs := []pair{}
	invalidXInvalidPairs := []pair{}

	for gvk, valids := range gvksToValidFiles {
		for _, validOld := range valids {
			for _, validNew := range gvksToValidFiles[gvk] {
				validXValidPairs = append(validXValidPairs, pair{old: validOld, new: validNew})
			}
		}
	}

	for gvk, valids := range gvksToValidFiles {
		for _, valid := range valids {
			for _, invalid := range gvksToInvalidFiles[gvk] {
				validXInvalidPairs = append(validXInvalidPairs, pair{old: valid, new: invalid})
			}
		}
	}

	// 对每个非法文件，与同 GVK 的每个其他非法文件配对（包括自身）
	for gvk, invalids := range gvksToInvalidFiles {
		for _, invalid := range invalids {
			for _, invalid2 := range gvksToInvalidFiles[gvk] {
				invalidXInvalidPairs = append(invalidXInvalidPairs, pair{old: invalid, new: invalid2})
			}
		}
	}

	// 对每对对象，在 update 上执行 ratcheting 算法。
	//
	for _, ratchetingEnabled := range []bool{true, false} {
		name := "RatchetingEnabled"
		if !ratchetingEnabled {
			name = "RatchetingDisabled"
		}
		b.Run(name, func(b *testing.B) {
			featuregatetesting.SetFeatureGateDuringTest(b, utilfeature.DefaultFeatureGate, features.CRDValidationRatcheting, ratchetingEnabled)
			b.ResetTimer()

			do := func(pairs []pair) {
				for _, pair := range pairs {
					// 为合法对象所属 GVK 取 validator。
					validator, ok := validators[pair.old.GroupVersionKind()]
					if !ok {
						b.Log("No validator for GVK", pair.old.GroupVersionKind())
						continue
					}

					// 在 update 上执行 ratcheting 算法。
					// benchmark 不关心结果
					validator(pair.old, pair.new)
				}
			}

			b.Run("ValidXValid", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					do(validXValidPairs)
				}
			})

			b.Run("ValidXInvalid", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					do(validXInvalidPairs)
				}
			})

			b.Run("InvalidXInvalid", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					do(invalidXInvalidPairs)
				}
			})
		})
	}
}

// TestRatchetingDropFields 验证 feature gate 关闭时会丢弃 OptionalOldSelf 字段，
// 从而让基于 oldSelf 的转换规则不会在关闭 ratcheting 时生效。
func TestRatchetingDropFields(t *testing.T) {
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.32"))
	// 只有 feature 关闭时才会丢弃该字段
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.CRDValidationRatcheting, false)
	tearDown, apiExtensionClient, _, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	group := myCRDV1Beta1.Group
	version := myCRDV1Beta1.Version
	resource := myCRDV1Beta1.Resource
	kind := fakeRESTMapper[myCRDV1Beta1]

	myCRD := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: resource + "." + group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: group,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    version,
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"field": {
										Type: "string",
										XValidations: []apiextensionsv1.ValidationRule{
											{
												// 如果字段未被丢弃，这里会报错
												Rule:            "self == oldSelf",
												OptionalOldSelf: ptr.To(true),
											},
										},
									},
								},
							},
						},
					},
				},
			}},
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   resource,
				Kind:     kind,
				ListKind: kind + "List",
			},
			Scope: apiextensionsv1.NamespaceScoped,
		},
	}

	created, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), myCRD, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if created.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties["field"].XValidations[0].OptionalOldSelf != nil {
		t.Errorf("Expected OpeiontalOldSelf field to be dropped for create when feature gate is disabled")
	}

	var updated *apiextensionsv1.CustomResourceDefinition
	err = wait.PollUntilContextTimeout(context.TODO(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		existing, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), created.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		existing.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties["field"].XValidations[0].OptionalOldSelf = ptr.To(true)
		updated, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), existing, metav1.UpdateOptions{})
		if err != nil {
			if apierrors.IsConflict(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("unexpected error waiting for CRD update: %v", err)
	}

	if updated.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties["field"].XValidations[0].OptionalOldSelf != nil {
		t.Errorf("Expected OpeiontalOldSelf field to be dropped for update when feature gate is disabled")
	}
}
