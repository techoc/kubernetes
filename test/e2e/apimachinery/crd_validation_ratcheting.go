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

// Package apimachinery 包含由 SIG-API-Machinery 负责的 e2e 测试。
package apimachinery

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/utils/crd"
)

var _ = SIGDescribe("CRDValidationRatcheting [Privileged:ClusterAdmin]", framework.WithFeatureGate(apiextensionsfeatures.CRDValidationRatcheting), func() {
	f := framework.NewDefaultFramework("crd-validation-ratcheting")
	var apiExtensionClient *clientset.Clientset
	var dynamicClient dynamic.Interface
	var restmapper meta.RESTMapper
	var ctx context.Context
	var testCRD *crd.TestCrd
	var testCRDGVR schema.GroupVersionResource

	ginkgo.BeforeEach(func() {
		var err error
		ctx = context.TODO()

		apiExtensionClient, err = clientset.NewForConfig(f.ClientConfig())
		framework.ExpectNoError(err, "initializing apiExtensionClient")

		dynamicClient, err = dynamic.NewForConfig(f.ClientConfig())
		framework.ExpectNoError(err, "initializing dynamicClient")

		testCRD, err = crd.CreateTestCRD(f)
		framework.ExpectNoError(err, "creating test CRD")

		testCRDGVR = schema.GroupVersionResource{
			Group:    testCRD.Crd.Spec.Group,
			Version:  testCRD.Crd.Spec.Versions[0].Name,
			Resource: testCRD.Crd.Spec.Names.Plural,
		}

		// 完整 discovery 的 restmapper 对本测试来说太重，
		// 直接用硬编码映射即可
		restmapper = &fakeRESTMapper{
			m: map[schema.GroupVersionResource]schema.GroupVersionKind{
				testCRDGVR: {
					Group:   testCRDGVR.Group,
					Version: testCRDGVR.Version,
					Kind:    testCRD.Crd.Spec.Names.Kind,
				},
			},
		}
	})

	ginkgo.AfterEach(func() {
		framework.ExpectNoError(testCRD.CleanUp(ctx), "cleaning up test CRD")
	})

	// 将给定 patch 应用到给定 GVR。patch 可以是字符串或
	// map[string]interface{}。字符串会按 YAML 或
	// JSON 解析；map 则直接使用。
	applyPatch := func(gvr schema.GroupVersionResource, name string, patchObj map[string]interface{}) error {
		gvk, err := restmapper.KindFor(gvr)
		if err != nil {
			return fmt.Errorf("no mapping for %s", gvr)
		}
		patch := &unstructured.Unstructured{
			Object: patchObj,
		}
		patch = patch.DeepCopy()

		patch.SetKind(gvk.Kind)
		patch.SetAPIVersion(gvk.GroupVersion().Identifier())
		patch.SetName(name)
		patch.SetNamespace("default")

		_, err = dynamicClient.
			Resource(gvr).
			Namespace(patch.GetNamespace()).
			Apply(
				context.TODO(),
				patch.GetName(),
				patch,
				metav1.ApplyOptions{
					FieldManager: "manager",
				})

		return err
	}

	// 更新给定 GVR 的 CRD schema。返回前会通过一次带哨兵错误的
	// 创建请求，等待 CRD 真正更新完成。
	updateCRDSchema := func(gvr schema.GroupVersionResource, props apiextensionsv1.JSONSchemaProps) error {
		myCRD, err := apiExtensionClient.
			ApiextensionsV1().
			CustomResourceDefinitions().
			Get(
				context.TODO(),
				gvr.Resource+"."+gvr.Group,
				metav1.GetOptions{},
			)
		if err != nil {
			return fmt.Errorf("getting CRD %s: %v", gvr, err)
		}

		// 注入一个特殊字段，触发唯一的错误字符串，以便
		// 判断服务端 schema 何时更新完成。
		uniqueErrorUUID := string(uuid.NewUUID())
		sentinelName := "__update_schema_sentinel_field__"
		props.Properties[sentinelName] = apiextensionsv1.JSONSchemaProps{
			Type: "string",
			Enum: []apiextensionsv1.JSON{
				{Raw: []byte(`"` + uniqueErrorUUID + `"`)},
			},
		}

		for i, v := range myCRD.Spec.Versions {
			if v.Name == gvr.Version {
				myCRD.Spec.Versions[i].Schema.OpenAPIV3Schema = &props
			}
		}

		_, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), myCRD, metav1.UpdateOptions{
			FieldManager: "manager",
		})
		if err != nil {
			return fmt.Errorf("updating CRD %s: %v", gvr, err)
		}

		// 持续尝试创建 CRD 的非法实例，
		// 直到拿到包含目标 ResourceVersion 的错误
		//
		counter := 0
		err = wait.PollUntilContextCancel(context.TODO(), 100*time.Millisecond, true, func(_ context.Context) (done bool, err error) {
			counter += 1
			err = applyPatch(gvr, "sentinel-resource", map[string]interface{}{
				"metadata": map[string]interface{}{
					"finalizers": []interface{}{
						"unqualified-finalizer",
					},
					"labels": map[string]interface{}{
						"#inv/($%)/alid=": ">htt$://",
					},
				},
				// 每次换不同的值即可
				sentinelName: fmt.Sprintf("%v", counter),
			})

			if err == nil {
				return false, fmt.Errorf("expected error when creating sentinel resource")
			}
			// 检查返回的错误信息是否包含我们的
			// 唯一字符串。UUID 足够独特，只需
			// 检查错误中是否出现即可。
			if strings.Contains(err.Error(), uniqueErrorUUID) {
				return true, nil
			}
			return false, nil

		})
		if err == nil {
			return nil
		}
		return fmt.Errorf("waiting for CRD %s to be updated: %v", gvr, err)
	}

	ginkgo.It("MUST NOT fail to update a resource due to JSONSchema errors on unchanged correlatable fields", func() {
		sch, err := parseSchema(`
			type: object
			properties:
				field: {type: string, enum: ["notfoo"]}
				struct:
					type: object
					properties:
						field: {type: string, enum: ["notfoo"]}
				list:
					type: array
					x-kubernetes-list-type: map
					x-kubernetes-list-map-keys: ["key"]
					items:
						type: object
						properties:
							key: {type: string}
							field: {type: string, enum: ["notfoo"]}
						required:
						- key
				map:
					type: object
					additionalProperties:
						type: object
						properties:
							field: {type: string, enum: ["notfoo"]}
		`)
		framework.ExpectNoError(err, "parsing schema")

		instance, err := parseUnstructured(`
			field: "foo"
			struct:
				field: "foo"
			list:
			- key: "first"
			  field: "foo"
			map:
				foo:
					field: "foo"
		`)
		framework.ExpectNoError(err, "parsing test resource")

		ginkgo.By("creating test resource with correlatable fields")
		framework.ExpectNoError(applyPatch(testCRDGVR, "test-resource", instance.Object), "failed creating test resource")
		ginkgo.By("updating CRD schema with constraints on correlatable fields to make instance invalid")
		framework.ExpectNoError(updateCRDSchema(testCRDGVR, *sch), "failed to update schema")

		// 修改标签。未改动的字段应允许
		// 通过校验。
		ginkgo.By("updating label on now-invalid test resource")
		instance.SetLabels(map[string]string{
			"foo": "bar",
		})
		framework.ExpectNoError(applyPatch(testCRDGVR, "test-resource", instance.Object), "update label on test resource")
	})

	ginkgo.It("MUST fail to update a resource due to JSONSchema errors on unchanged uncorrelatable fields", func() {
		ginkgo.By("creating test resource with correlatable fields")
		instance, err := parseUnstructured(`
			setArray:
			- "foo"
			- "bar"
			- "baz"
			atomicArray:
			- "foo"
			- "bar"
			- "baz"
		`)
		framework.ExpectNoError(err, "parsing test resource")
		framework.ExpectNoError(applyPatch(testCRDGVR, "test-resource", instance.Object), "failed creating test resource")

		ginkgo.By("updating CRD schema with constraints on uncorrelatable fields to make instance invalid")
		sch, err := parseSchema(`
			type: object
			properties:
				atomicArray:
					type: array
					items:
						type: string
						enum: ["notfoo", "notbar", "notbaz"]
				setArray:
					type: array
					x-kubernetes-list-type: set
					items:
						type: string
						enum: ["notfoo", "notbar", "notbaz"]
		`)
		framework.ExpectNoError(err, "parsing schema")
		framework.ExpectNoError(updateCRDSchema(testCRDGVR, *sch), "failed to update schema")

		ginkgo.By("updating label on now-invalid test resource")
		instance, err = parseUnstructured(`
			setArray:
			- "foo"
			- "bar"
			- "baz"
			- "notfoo"
			atomicArray:
			- "foo"
			- "bar"
			- "baz"
			- "notfoo"
		`)
		framework.ExpectNoError(err, "parsing modified resource")
		instance.SetLabels(map[string]string{
			"foo": "bar",
		})
		err = applyPatch(testCRDGVR, "test-resource", instance.Object)
		gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("atomicArray")))
		gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("setArray")))
	})

	ginkgo.It("MUST fail to update a resource due to JSONSchema errors on changed fields", func() {
		ginkgo.By("creating an initial object with many correlatable fields")
		instance, err := parseUnstructured(`
			field: "foo"
			struct:
				field: "foo"
			list:
			- key: "foo"
			  field: "foo"
			- key: "bar"
			  field: "foo"
			map:
				foo:
					field: "foo"
				bar:
					field: "foo"
		`)
		framework.ExpectNoError(err, "parsing test resource")
		framework.ExpectNoError(applyPatch(testCRDGVR, "test-resource", instance.Object), "failed creating test resource")

		ginkgo.By("updating CRD schema with constraints on correlatable fields to make instance invalid")
		sch, err := parseSchema(`
			type: object
			properties:
				field: {type: string, enum: ["foo"]}
				struct:
					type: object
					properties:
						field: {type: string, enum: ["foo"]}
				list:
					type: array
					x-kubernetes-list-type: map
					x-kubernetes-list-map-keys: ["key"]
					items:
						type: object
						properties:
							key: {type: string}
							field: {type: string, enum: ["foo"]}
						required:
							- key
				map:
					type: object
					additionalProperties:
						type: object
						properties:
							field: {type: string, enum: ["foo"]}
		`)

		framework.ExpectNoError(err, "parsing schema")
		framework.ExpectNoError(updateCRDSchema(testCRDGVR, *sch), "failed to update schema")

		ginkgo.By("changing every field to invalid value")
		modifiedInstance, err := parseUnstructured(`
			field: "notfoo"
			struct:
				field: "notfoo"
			list:
			- key: "foo"
			  field: "notfoo"
			- key: "bar"
			  field: "notfoo"
			map:
				foo:
					field: "notfoo"
				bar:
					field: "notfoo"
		`)
		framework.ExpectNoError(err, "parsing modified resource")
		err = applyPatch(testCRDGVR, "test-resource", modifiedInstance.Object)
		for _, fieldPath := range []string{
			"field",
			"struct.field",
			"list[0].field",
			"list[1].field",
			"map.foo.field",
			"map.bar.field",
		} {
			gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring(fieldPath)))
		}
	})

	ginkgo.It("MUST NOT fail to update a resource due to CRD Validation Rule errors on unchanged correlatable fields", func() {
		ginkgo.By("creating an initial object with many correlatable fields")
		instance, err := parseUnstructured(`
			field: "notfoo"
			struct:
				field: "notfoo"
			list:
			- key: "foo"
			  field: "notfoo"
			- key: "bar"
			  field: "notfoo"
			map:
				foo:
					field: "notfoo"
				bar:
					field: "notfoo"
		`)
		framework.ExpectNoError(err, "parsing test resource")
		framework.ExpectNoError(applyPatch(testCRDGVR, "test-resource", instance.Object), "failed creating test resource")

		ginkgo.By("updating CRD schema with constraints on correlatable fields to make instance invalid")
		sch, err := parseSchema(`
			type: object
			properties:
				field:
					type: string
					x-kubernetes-validations:
					- rule: self == "foo"
				otherField:
					type: string
				struct:
					type: object
					properties:
						field:
							type: string
							x-kubernetes-validations:
							- rule: self == "foo"
						otherField:
							type: string
				list:
					type: array
					x-kubernetes-list-type: map
					x-kubernetes-list-map-keys: ["key"]
					items:
						type: object
						properties:
							key:
								type: string
							field:
								type: string
								x-kubernetes-validations:
								- rule: self == "foo"
							otherField:
								type: string
						required:
						- key
				map:
					type: object
					additionalProperties:
						type: object
						properties:
							field:
								type: string
								x-kubernetes-validations:
								- rule: self == "foo"
							otherField:
								type: string
		`)

		framework.ExpectNoError(err, "parsing schema")
		framework.ExpectNoError(updateCRDSchema(testCRDGVR, *sch), "failed to update schema")

		ginkgo.By("introducing new values, but leaving invalid old correlatable values untouched")
		modifiedInstance, err := parseUnstructured(`
			field: "notfoo"
			otherField: "doesntmatter"
			struct:
				field: "notfoo"
				otherField: "doesntmatter"
			list:
			- key: "foo"
			  field: "notfoo"
			  otherField: "doesntmatter"
			- key: "bar"
			  field: "notfoo"
			  otherField: "doesntmatter"
			- key: "baz"
			  field: "foo"
			  otherField: "doesntmatter"
			map:
				foo:
					field: "notfoo"
					otherField: "doesntmatter"
				bar:
					field: "notfoo"
					otherField: "doesntmatter"
		`)
		framework.ExpectNoError(err, "parsing test resource")
		framework.ExpectNoError(applyPatch(testCRDGVR, "test-resource", modifiedInstance.Object), "failed updating test resource")
	})

	ginkgo.It("MUST fail to update a resource due to CRD Validation Rule errors on unchanged uncorrelatable fields", func() {
		ginkgo.By("creating test resource with correlatable fields")
		instance, err := parseUnstructured(`
			setArray:
			- "foo"
			- "bar"
			- "baz"
			atomicArray:
			- "foo"
			- "bar"
			- "baz"
		`)
		framework.ExpectNoError(err, "parsing test resource")
		framework.ExpectNoError(applyPatch(testCRDGVR, "test-resource", instance.Object), "failed creating test resource")

		ginkgo.By("updating CRD schema with constraints on uncorrelatable fields to make instance invalid")
		sch, err := parseSchema(`
			type: object
			properties:
				atomicArray:
					type: array
					items:
						type: string
						x-kubernetes-validations:
						- rule: self != "foo"
				setArray:
					type: array
					x-kubernetes-list-type: set
					items:
						type: string
						x-kubernetes-validations:
						- rule: self != "foo"
		`)
		framework.ExpectNoError(err, "parsing schema")
		framework.ExpectNoError(updateCRDSchema(testCRDGVR, *sch), "failed to update schema")

		ginkgo.By("updating label and adding valid elements to invalid lists")
		instance, err = parseUnstructured(`
			setArray:
			- "foo"
			- "bar"
			- "baz"
			- "notfoo"
			atomicArray:
			- "foo"
			- "bar"
			- "baz"
			- "notfoo"
		`)
		framework.ExpectNoError(err, "parsing modified resource")
		instance.SetLabels(map[string]string{
			"foo": "bar",
		})
		err = applyPatch(testCRDGVR, "test-resource", instance.Object)
		gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("atomicArray")))
		gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("setArray")))
	})

	ginkgo.It("MUST fail to update a resource due to CRD Validation Rule errors on changed fields", func() {
		ginkgo.By("creating an initial object with many correlatable fields")
		instance, err := parseUnstructured(`
			field: "foo"
			struct:
				field: "foo"
			list:
			- key: "foo"
			  field: "foo"
			- key: "bar"
			  field: "foo"
			map:
				foo:
					field: "foo"
				bar:
					field: "foo"
		`)
		framework.ExpectNoError(err, "parsing test resource")
		framework.ExpectNoError(applyPatch(testCRDGVR, "test-resource", instance.Object), "failed creating test resource")

		ginkgo.By("updating CRD schema with constraints on correlatable fields to make instance invalid")
		sch, err := parseSchema(`
			type: object
			properties:
				field: 
					type: string
					x-kubernetes-validations:
					- rule: self == "foo"
				struct:
					type: object
					properties:
						field:
							type: string
							x-kubernetes-validations:
							- rule: self == "foo"
				list:
					type: array
					x-kubernetes-list-type: map
					x-kubernetes-list-map-keys:
					- key
					items:
						type: object
						properties:
							key:
								type: string
							field:
								type: string
								x-kubernetes-validations:
								- rule: self == "foo"
						required:
						- key
				map:
					type: object
					additionalProperties:
						type: object
						properties:
							field:
								type: string
								x-kubernetes-validations:
								- rule: self == "foo"
		`)

		framework.ExpectNoError(err, "parsing schema")
		framework.ExpectNoError(updateCRDSchema(testCRDGVR, *sch), "failed to update schema")

		ginkgo.By("changing every field to invalid value")
		modifiedInstance, err := parseUnstructured(`
			field: "notfoo"
			struct:
				field: "notfoo"
			list:
			- key: "foo"
			  field: "notfoo"
			- key: "bar"
			  field: "notfoo"
			map:
				foo:
					field: "notfoo"
				bar:
					field: "notfoo"
		`)
		framework.ExpectNoError(err, "parsing modified resource")
		err = applyPatch(testCRDGVR, "test-resource", modifiedInstance.Object)
		for _, fieldPath := range []string{
			"field",
			"struct.field",
			"list[0].field",
			"list[1].field",
			"map[foo].field",
			"map[bar].field",
		} {
			gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring(fieldPath)))
		}
	})

	ginkgo.It("MUST NOT ratchet errors raised by transition rules", func() {
		ginkgo.By("creating an initial object with many correlatable fields")
		instance, err := parseUnstructured(`
			field: "foo"
			struct:
				field: "foo"
			list:
			- key: "foo"
			  field: "foo"
			- key: "bar"
			  field: "foo"
			map:
				foo:
					field: "foo"
				bar:
					field: "foo"
		`)
		framework.ExpectNoError(err, "parsing test resource")
		framework.ExpectNoError(applyPatch(testCRDGVR, "test-resource", instance.Object), "failed creating test resource")

		ginkgo.By("updating CRD schema with constraints on correlatable fields to make instance invalid")
		sch, err := parseSchema(`
			type: object
			properties:
				field: 
					type: string
					maxLength: 5
					x-kubernetes-validations:
					- rule: self != oldSelf
				struct:
					type: object
					properties:
						field:
							type: string
							maxLength: 5
							x-kubernetes-validations:
							- rule: self != oldSelf
				list:
					type: array
					maxItems: 5
					x-kubernetes-list-type: map
					x-kubernetes-list-map-keys: [key]
					items:
						type: object
						properties:
							key: {type: string}
							field:
								type: string
								maxLength: 5
								x-kubernetes-validations:
								- rule: self != oldSelf
						required:
						- key
				map:
					type: object
					maxProperties: 5
					additionalProperties:
						type: object
						properties:
							field:
								type: string
								maxLength: 5
								x-kubernetes-validations:
								- rule: self != oldSelf
		`)

		framework.ExpectNoError(err, "parsing schema")
		framework.ExpectNoError(updateCRDSchema(testCRDGVR, *sch), "failed to update schema")

		ginkgo.By("updating a label on the test resource")
		instance.SetLabels(map[string]string{
			"foo": "bar",
		})
		err = applyPatch(testCRDGVR, "test-resource", instance.Object)
		for _, fieldPath := range []string{
			"field",
			"struct.field",
			"list[0].field",
			"list[1].field",
			"map[foo].field",
			"map[bar].field",
		} {
			gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring(fieldPath)))
		}
	})

	ginkgo.It("MUST evaluate a CRD Validation Rule with oldSelf = nil for new values when optionalOldSelf is true", func() {
		ginkgo.By("updating CRD schema to use optionalOldSelf")
		sch, err := parseSchema(`
			type: object
			properties:
				field: 
					type: string
					maxLength: 5
					x-kubernetes-validations:
					- rule: "!oldSelf.hasValue() || self != oldSelf.value()"
					  optionalOldSelf: true
				struct:
					type: object
					properties:
						field:
							type: string
							maxLength: 5
							x-kubernetes-validations:
							- rule: "!oldSelf.hasValue() || self != oldSelf.value()"
							  optionalOldSelf: true
				list:
					type: array
					maxItems: 5
					x-kubernetes-list-type: map
					x-kubernetes-list-map-keys: [key]
					items:
						type: object
						properties:
							key: {type: string}
							field:
								type: string
								maxLength: 5
								x-kubernetes-validations:
								- rule: "!oldSelf.hasValue() || self != oldSelf.value()"
								  optionalOldSelf: true
						required:
						- key
				map:
					type: object
					maxProperties: 5
					additionalProperties:
						type: object
						properties:
							field:
								type: string
								maxLength: 5
								x-kubernetes-validations:
								- rule: "!oldSelf.hasValue() || self != oldSelf.value()"
								  optionalOldSelf: true
		`)
		framework.ExpectNoError(err, "parsing schema")
		framework.ExpectNoError(updateCRDSchema(testCRDGVR, *sch), "failed to update schema")

		ginkgo.By("creating an object")
		instance, err := parseUnstructured(`
			field: "foo"
			struct:
				field: "foo"
			list:
			- key: "foo"
			  field: "foo"
			- key: "bar"
			  field: "foo"
			map:
				foo:
					field: "foo"
				bar:
					field: "foo"
		`)
		framework.ExpectNoError(err, "parsing test resource")
		framework.ExpectNoError(applyPatch(testCRDGVR, "test-resource", instance.Object), "failed creating test resource")

		ginkgo.By("updating a label on the test resource")
		instance.SetLabels(map[string]string{
			"foo": "bar",
		})
		err = applyPatch(testCRDGVR, "test-resource", instance.Object)
		for _, fieldPath := range []string{
			"field",
			"struct.field",
			"list[0].field",
			"list[1].field",
			"map[foo].field",
			"map[bar].field",
		} {
			gomega.Expect(err).To(gomega.MatchError(gomega.ContainSubstring(fieldPath)))
		}

		ginkgo.By("updating all fields of the object to show the condition is checked")
		instance, err = parseUnstructured(`
			field: "new"
			struct:
				field: "new"
			list:
			- key: "foo"
			  field: "new"
			- key: "bar"
			  field: "new"
			map:
				foo:
					field: "new"
				bar:
					field: "new"
		`)
		framework.ExpectNoError(err, "parsing test resource")
		framework.ExpectNoError(applyPatch(testCRDGVR, "test-resource", instance.Object), "failed updating test resource")
	})

})

func parseSchema(source string) (*apiextensionsv1.JSONSchemaProps, error) {
	source, err := fixTabs(source)
	if err != nil {
		return nil, err
	}

	d := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(source), 4096)
	props := &apiextensionsv1.JSONSchemaProps{}
	return props, d.Decode(props)
}

func parseUnstructured(source string) (*unstructured.Unstructured, error) {
	source, err := fixTabs(source)
	if err != nil {
		return nil, err
	}

	d := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(source), 4096)
	obj := &unstructured.Unstructured{}
	return obj, d.Decode(&obj.Object)
}

// fixTabs 统计给定 yaml 对象首行前面的
// tab 数量，并从每行移除相同数量的 tab。
// 若某行的 tab 少于首行则返回错误（这是测试函数）。
//
// 这样做的目的是让测试更易读。
func fixTabs(in string) (string, error) {
	lines := bytes.Split([]byte(in), []byte{'\n'})
	if len(lines[0]) == 0 && len(lines) > 1 {
		lines = lines[1:]
	}
	// 构造要移除的 tab 前缀。
	var prefix []byte
	for _, c := range lines[0] {
		if c != '\t' {
			break
		}
		prefix = append(prefix, byte('\t'))
	}
	// 从所有行移除该前缀，否则报错。
	for i := range lines {
		line := lines[i]
		// 最后一行为空是可以的（末尾有 \n）
		if i == len(lines)-1 && len(line) <= len(prefix) && bytes.TrimSpace(line) == nil {
			lines[i] = []byte{}
			break
		}
		if !bytes.HasPrefix(line, prefix) {
			minRange := i - 5
			maxRange := i + 5
			if minRange < 0 {
				minRange = 0
			}
			if maxRange > len(lines) {
				maxRange = len(lines)
			}
			return "", fmt.Errorf("line %d doesn't start with expected number (%d) of tabs (%v-%v):\n%v", i, len(prefix), minRange, maxRange, string(bytes.Join(lines[minRange:maxRange], []byte{'\n'})))
		}
		lines[i] = line[len(prefix):]
	}
	joined := string(bytes.Join(lines, []byte{'\n'}))

	// 剩余的 tab 转成空格，yaml 不接受 tab
	// （假设按 2 空格对齐）
	return strings.ReplaceAll(joined, "\t", "  "), nil
}

type fakeRESTMapper struct {
	m map[schema.GroupVersionResource]schema.GroupVersionKind
}

func (f *fakeRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	gvk, ok := f.m[resource]
	if !ok {
		return schema.GroupVersionKind{}, fmt.Errorf("no mapping for %s", resource)
	}
	return gvk, nil
}

func (f *fakeRESTMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	return nil, nil
}

func (f *fakeRESTMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return schema.GroupVersionResource{}, nil
}

func (f *fakeRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return nil, nil
}

func (f *fakeRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	return nil, nil
}

func (f *fakeRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	return nil, nil
}

func (f *fakeRESTMapper) ResourceSingularizer(resource string) (singular string, err error) {
	return "", nil
}
