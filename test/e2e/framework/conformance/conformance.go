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

package architecture

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	gtypes "github.com/onsi/gomega/types"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/utils/ktesting/format"
	"k8s.io/utils/ptr"
	k8sjson "sigs.k8s.io/json"
)

// ResourceTestcaseInterface 描述如何通过对其执行不同操作来测试某个特定 API endpoint
// 对其执行各种操作。
//
// 创建或打补丁后的对象内容通过确保
// 所有字段都与发送对象中的设置一致来验证。多余字段或 map 条目
// 会被忽略。
//
// 涵盖基本的 create/read/update/delete（CRUD）语义，这是
// GA 特性一致性测试的最低要求。
// 理想情况下还应做实际功能测试，但并非必需。
// required.
//
// 参见 [ResourceTestcase]，它提供了该接口的一个实现，
// 其中测试数据以 Go 对象和 patch 字符串形式提供。
type ResourceTestcaseInterface interface {
	// GetGroupVersionResource 返回 API group、version 和 resource（复数形式、小写）。
	GetGroupVersionResource() schema.GroupVersionResource

	// IsNamespaced 定义对象是否必须在命名空间内创建。
	IsNamespaced() bool

	// HasStatus 定义资源是否有 "status" 子资源。
	//
	// 此通用测试代码不支持其他子资源。
	HasStatus() bool

	// VerifyContent 定义是否将 apiserver 返回的对象内容与
	// 发送的内容进行比较。
	//
	// 如果启用，所有发送的字段值也必须包含在
	// 返回的对象中。可能额外添加字段及 list 或 map 条目
	//（例如由默认值处理或 mutating admission
	// 引入）。
	//
	// 一致性测试中不应启用此选项，因为 admission
	// 可以修改被存储的内容。
	VerifyContent() bool

	// GetInitialObject 返回将用于 Create 调用的数据。
	//
	// 对于集群级资源，可以使用测试命名空间
	// 创建一个不与其他对象冲突的名称，
	// 因为它在测试运行期间是唯一的。
	//
	// 对于命名空间级资源无需设置，因为
	// 调用方会保证这一点。调用方无法对名称这样做，因为
	// 不同资源对有效名称有不同规则
	GetInitialObject(namespace string) *unstructured.Unstructured

	// GetUpdateSpec 修改现有对象。
	// 它会在创建初始对象的结果上调用。
	//
	// 理想情况下它应更改 spec（因此得名）。
	// 如果不可能，添加一些 label 也可以。
	// 目标是添加一些字段，以便 Update 后
	// 可以检查。
	//
	// 可以修改并返回输入对象。
	GetUpdateSpec(object *unstructured.Unstructured) *unstructured.Unstructured

	// GetUpdateStatus 修改现有对象的 status。
	// 它会在创建初始对象
	// 并更新其 spec 之后调用。
	//
	// 可以修改并返回输入对象。
	GetUpdateStatus(object *unstructured.Unstructured) *unstructured.Unstructured

	// GetPatchSpec 描述如何生成 patch。
	//
	// 每个 patch 单独应用到初始对象，不与其他 patch 叠加。
	// 返回空 slice 是合法的，表示禁用 patch 测试。但这可能不足够
	// 完成资源的一致性测试。
	//
	// 如果启用了内容验证，那么它必须产生与 GetUpdateSpec 相同的更改，
	// 因为 patch 结果的验证以 GetUpdateSpec 的结果为参照。
	GetPatchSpec() []Patch

	// GetPatchStatus 对 status 的作用类似于 GetPatchSpec。
	//
	// 带有更新后 spec 的初始对象会被打补丁，
	// 因此结果必须与应用到 GetInitialObject 的 GetUpdateStatus 结果一致，
	// 前提是启用了内容验证。
	// enabled.
	GetPatchStatus(object *unstructured.Unstructured) []Patch
}

// Patch 包含 Patch API 调用的参数。
//
// 数据必须与现有对象匹配。
//
// 由于没有针对冲突的重试循环，patch 不应包含
// 对 ResourceVersion 的检查。鼓励在 patch 中检查 UID，以防止
// 误改一个已被替换的资源。
type Patch struct {
	GetData func(object *unstructured.Unstructured) []byte
	Type    types.PatchType
}

// ResourceTestcase 为测试某个资源的操作提供测试数据。
// 测试数据基于资源的原生 Go 类型。
// 模板参数必须是指向该原生 Go 类型的指针。
//
// 数据使用方式如下：
// - 创建 InitialSpec -> 用 UpdateSpec 更新 -> 用 UpdateStatus 更新 status
// - 创建 InitialSpec -> 应用 StrategicMergePatchSpec 并与 UpdateSpec 比较 -> 应用 StrategicMergePatchStatus 并与 UpdateStatus 比较
type ResourceTestcase[T runtime.Object] struct {
	// GroupResourceVersion 标识要测试的 API 组、版本和资源（复数形式、小写）
	// 以及该 API 中的具体资源。
	GVR schema.GroupVersionResource

	// 如果资源必须在命名空间中创建，Namespaced 必须为 true；
	// 如果是集群级资源则为 false。保持未设置是错误的。
	//
	//
	// Namespaced 资源在测试命名空间中创建。
	//
	// 集群级资源的名称会追加
	// `-<测试命名空间名称>` 以保证唯一。
	Namespaced *bool

	// ContentVerificationEnabled 定义是否比较 apiserver 返回对象的内容
	// 发送的内容进行比较。
	//
	// 如果启用，所有发送的字段值也必须包含在
	// 返回的对象中。可能额外添加字段及 list 或 map 条目
	//（例如由默认值处理或 mutating admission
	// 引入）。
	//
	// 一致性测试中不应启用此选项，因为 admission
	// 可以修改被存储的内容。
	ContentVerificationEnabled bool

	// InitialSpec 必须包含有效资源的初始状态，不含 status。
	InitialSpec T

	// UpdateSpec 会对创建的初始对象调用，
	// 并且必须更改某些内容，理想情况下是 spec（因此得名）。
	// 如果无法更改 spec，添加一些 label 也可以，
	// 只要能测试更新即可。
	//
	// 可以修改并返回输入对象。
	UpdateSpec func(T) T

	// UpdateStatus 会对更新后的对象调用，
	// 并且必须添加 status。
	//
	// 如果不支持 status，可以为 nil。
	//
	// 可以修改并返回输入对象。
	UpdateStatus func(T) T

	// StrategicMergePatchSpec 可以用 strategic merge patch 修改 InitialSpec 中的字段
	//
	//（https://github.com/kubernetes/community/blob/master/contributors/devel/sig-api-machinery/strategic-merge-patch.md）。
	// 必须使用 JSON 编码。
	//
	// 如果启用了内容验证，那么它必须包含与 UpdateSpec 相同的更改，
	// 因为 patch 结果的验证以 UpdateSpec 为参照。
	StrategicMergePatchSpec string

	// StrategicMergePatchStatus 可以添加 status 字段
	//
	//（https://github.com/kubernetes/community/blob/master/contributors/devel/sig-api-machinery/strategic-merge-patch.md）
	// 必须使用 JSON 编码。
	//
	// 带有更新后 spec 的初始对象会被打补丁，
	// 因此结果必须与 GetUpdateStatus 一致
	// 前提是启用了内容验证。
	// enabled.
	//
	// 如果为空，则不测试 status 子资源。
	// 可以包含名称，但不是必需。
	StrategicMergePatchStatus string

	// ApplyPatchSpec 对应 StrategicMergePatchSpec，
	// 使用 server-side apply（SSA）patch 的 JSON 编码
	//（https://kubernetes.io/docs/reference/using-api/server-side-apply）。
	ApplyPatchSpec string

	// ApplyPatchStatus 对应 StrategicMergePatchStatus，
	// 使用 server-side apply（SSA）patch 的 JSON 编码
	//（https://kubernetes.io/docs/reference/using-api/server-side-apply）。
	ApplyPatchStatus string

	// JSONPatchSpec 对应 StrategicMergePatchSpec，
	// 使用 JSON patch（https://tools.ietf.org/html/rfc6902）。
	JSONPatchSpec string

	// JSONPatchStatus 对应 StrategicMergePatchStatus，
	// 使用 JSON patch（https://tools.ietf.org/html/rfc6902）。
	JSONPatchStatus string

	// JSONMergePatchSpec 对应 StrategicMergePatchSpec，
	// 使用 JSON merge patch（https://tools.ietf.org/html/rfc7386）。
	JSONMergePatchSpec string

	// JSONMergePatchStatus 对应 StrategicMergePatchStatus，
	// 使用 JSON merge patch（https://tools.ietf.org/html/rfc7386）。
	JSONMergePatchStatus string
}

var _ ResourceTestcaseInterface = &ResourceTestcase[*v1.Pod]{}

func (tc *ResourceTestcase[T]) GetGroupVersionResource() schema.GroupVersionResource {
	return tc.GVR
}

func (tc *ResourceTestcase[T]) IsNamespaced() bool {
	if tc.Namespaced == nil {
		framework.Fail("Test case error: Namespaced must be set")
	}

	return *tc.Namespaced
}

func (tc *ResourceTestcase[T]) HasStatus() bool {
	return tc.UpdateStatus != nil
}

func (tc *ResourceTestcase[T]) VerifyContent() bool {
	return tc.ContentVerificationEnabled
}

func (tc *ResourceTestcase[T]) GetInitialObject(namespace string) *unstructured.Unstructured {
	object := tc.toUnstructured("InitialSpec", tc.InitialSpec)
	if object.GetName() == "" {
		object.SetName("test")
	}
	if !tc.IsNamespaced() {
		object.SetName(object.GetName() + "-" + namespace)
	}

	return object
}

func (tc *ResourceTestcase[T]) GetUpdateSpec(in *unstructured.Unstructured) *unstructured.Unstructured {
	out := tc.fromUnstructured("existing object", in)
	out = tc.UpdateSpec(out)
	return tc.toUnstructured("updated object", out)
}

func (tc *ResourceTestcase[T]) GetUpdateStatus(in *unstructured.Unstructured) *unstructured.Unstructured {
	out := tc.fromUnstructured("updated object", in)
	out = tc.UpdateStatus(out)
	return tc.toUnstructured("updated object with status", out)
}

func (tc *ResourceTestcase[T]) GetPatchSpec() []Patch {
	var patches []Patch

	if tc.StrategicMergePatchSpec != "" {
		patches = append(patches, Patch{
			Type: types.StrategicMergePatchType,
			GetData: func(existingObject *unstructured.Unstructured) []byte {
				patch := tc.createPatchObject("StrategicMergePatchSpec", tc.StrategicMergePatchSpec, existingObject)

				jsonData, err := patch.MarshalJSON()
				framework.ExpectNoError(err, "re-encode spec patch as JSON")

				return jsonData
			},
		})
	}

	if tc.ApplyPatchSpec != "" {
		patches = append(patches, Patch{
			Type: types.ApplyPatchType,
			GetData: func(existingObject *unstructured.Unstructured) []byte {
				patch := tc.createPatchObject("ApplyPatchSpec", tc.ApplyPatchSpec, existingObject)

				jsonData, err := patch.MarshalJSON()
				framework.ExpectNoError(err, "re-encode spec patch as JSON")

				return jsonData
			},
		})
	}

	if tc.JSONMergePatchSpec != "" {
		patches = append(patches, Patch{
			Type: types.MergePatchType,
			GetData: func(existingObject *unstructured.Unstructured) []byte {
				patch := tc.createPatchObject("JSONMergePatchSpec", tc.JSONMergePatchSpec, existingObject)

				jsonData, err := patch.MarshalJSON()
				framework.ExpectNoError(err, "re-encode spec patch as JSON")

				return jsonData
			},
		})
	}

	if tc.JSONPatchSpec != "" {
		patches = append(patches, Patch{
			Type: types.JSONPatchType,
			GetData: func(existingObject *unstructured.Unstructured) []byte {
				return []byte(tc.JSONPatchSpec)
			},
		})
	}

	return patches
}

func (tc *ResourceTestcase[T]) GetPatchStatus(object *unstructured.Unstructured) []Patch {
	var patches []Patch

	if tc.StrategicMergePatchStatus != "" {
		patches = append(patches, Patch{
			Type: types.StrategicMergePatchType,
			GetData: func(existingObject *unstructured.Unstructured) []byte {
				patch := tc.createPatchObject("StrategicMergePatchStatus", tc.StrategicMergePatchStatus, existingObject)

				jsonData, err := patch.MarshalJSON()
				framework.ExpectNoError(err, "re-encode status patch as JSON")

				return jsonData
			},
		})
	}

	if tc.ApplyPatchStatus != "" {
		patches = append(patches, Patch{
			Type: types.ApplyPatchType,
			GetData: func(existingObject *unstructured.Unstructured) []byte {
				patch := tc.createPatchObject("ApplyPatchStatus", tc.ApplyPatchStatus, existingObject)

				jsonData, err := patch.MarshalJSON()
				framework.ExpectNoError(err, "re-encode status patch as JSON")

				return jsonData
			},
		})
	}

	if tc.JSONMergePatchStatus != "" {
		patches = append(patches, Patch{
			Type: types.MergePatchType,
			GetData: func(existingObject *unstructured.Unstructured) []byte {
				patch := tc.createPatchObject("JSONMergePatchStatus", tc.JSONMergePatchStatus, existingObject)

				jsonData, err := patch.MarshalJSON()
				framework.ExpectNoError(err, "re-encode status patch as JSON")

				return jsonData
			},
		})
	}

	if tc.JSONPatchStatus != "" {
		patches = append(patches, Patch{
			Type: types.JSONPatchType,
			GetData: func(existingObject *unstructured.Unstructured) []byte {
				return []byte(tc.JSONPatchStatus)
			},
		})
	}

	return patches
}

func (tc *ResourceTestcase[T]) toUnstructured(what string, in T) *unstructured.Unstructured {
	data, err := json.Marshal(in)
	framework.ExpectNoError(err, "encode %s as JSON", what)

	out := tc.toUnstructuredFromJSON(what, data)

	return out
}

func (tc *ResourceTestcase[T]) toUnstructuredFromJSON(what string, in []byte) *unstructured.Unstructured {
	// UnmarshalCaseSensitivePreserveInts 不需要 kind（与 unstructured.Unstructured.UnmarshalJSON 不同），
	// 并且保持从 apiserver 接收时的整数保留行为（与普通 json.Unmarshal 不同）。
	var out unstructured.Unstructured
	err := k8sjson.UnmarshalCaseSensitivePreserveInts(in, &out.Object)
	framework.ExpectNoError(err, "decode %s from JSON", what)

	return &out

}

func (tc *ResourceTestcase[T]) fromUnstructured(what string, in *unstructured.Unstructured) T {
	data, err := in.MarshalJSON()
	framework.ExpectNoError(err, "encode %s as JSON", what)

	var out T
	err = k8sjson.UnmarshalCaseSensitivePreserveInts(data, &out)
	framework.ExpectNoError(err, "decode %s from JSON", what)

	return out

}

// createPatchObject 解析 JSON 数据，然后从现有对象复制 namespace/name/uid/kind/apiVersion
// 以使 patch 完整。这适用于 strategic merge patch、apply patch 和 JSON merge patch。
func (tc *ResourceTestcase[T]) createPatchObject(what string, data string, existingObject *unstructured.Unstructured) *unstructured.Unstructured {
	object := tc.toUnstructuredFromJSON(what, []byte(data))
	object.SetNamespace(existingObject.GetNamespace())
	object.SetName(existingObject.GetName())
	object.SetUID(existingObject.GetUID())
	object.SetAPIVersion(existingObject.GetAPIVersion())
	object.SetKind(existingObject.GetKind())
	return object
}

// TestResource 通过 dynamic client 调用覆盖资源的所有典型 endpoint。
// 设置 client。
func TestResource(ctx context.Context, f *framework.Framework, tc ResourceTestcaseInterface) {
	// 例如 `cluster-scoped resource "deviceclasses"`
	gvr := tc.GetGroupVersionResource()
	gv := gvr.GroupVersion()
	resource := gvr.Resource
	resourceClient := f.DynamicClient.Resource(gvr)
	var client dynamic.ResourceInterface
	var resourceType string
	if tc.IsNamespaced() {
		client = resourceClient.Namespace(f.Namespace.Name)
		resourceType = "namespaced resource"
	} else {
		client = resourceClient
		resourceType = "cluster-scoped resource"
	}
	// gvr.String() 太长且包含逗号（"resource.k8s.io/v1, Resource=deviceclasses"）。
	// 测试会给所有对象添加一个 label，用于 List 和 DeleteCollection。
	resourceType = fmt.Sprintf("%s %q", resourceType, gvr.Resource)
	config := dynamic.ConfigFor(f.ClientConfig())
	httpClient, err := rest.HTTPClientFor(config)
	framework.ExpectNoError(err, "construct HTTP client")
	restClient, err := rest.UnversionedRESTClientForConfigAndClient(config, httpClient)
	framework.ExpectNoError(err, "construct REST client")

	// 该 label 必须添加到 ResourceTestcase 返回的所有对象上，
	// 因为该接口的实现不知道这个额外的 label。
	// 为 Create、Get 和 List 做准备。
	labelName := "e2e-test.kubernetes.io"
	labelValue := f.UniqueName
	listOptions := metav1.ListOptions{LabelSelector: labelName + "=" + labelValue}
	addLabel := func(obj *unstructured.Unstructured) *unstructured.Unstructured {
		obj = obj.DeepCopy()
		labels := obj.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[labelName] = labelValue
		obj.SetLabels(labels)
		return obj
	}

	// 获取 API 中的所有资源。结果列表必须包含我们正在测试的资源。
	desiredInitialObject := addLabel(tc.GetInitialObject(f.Namespace.Name))
	if tc.IsNamespaced() {
		desiredInitialObject.SetNamespace(f.Namespace.Name)
	}

	getResource := func(ctx context.Context) (*unstructured.Unstructured, error) {
		return client.Get(ctx, desiredInitialObject.GetName(), metav1.GetOptions{})
	}
	desiredUpdatedObject := tc.GetUpdateSpec(desiredInitialObject.DeepCopy())
	var desiredUpdatedObjectWithStatus *unstructured.Unstructured
	if tc.HasStatus() {
		desiredUpdatedObjectWithStatus = addLabel(tc.GetUpdateStatus(desiredUpdatedObject))
	}

	// 设置 informer，可选地也在命名空间中设置。
	ginkgo.By(fmt.Sprintf("Get %s", gv))
	path := "/apis/" + gv.String()
	var api unstructured.Unstructured
	err = restClient.
		Get().
		AbsPath(path).
		Do(ctx).
		Into(&api)
	framework.ExpectNoError(err, "get resource API")
	resources := api.Object["resources"].([]any)
	index := slices.IndexFunc(resources, func(entry any) bool {
		return entry.(map[string]any)["name"].(string) == resource
	})
	if index < 0 {
		framework.Failf("API for %s does not include entry for %s, got:\n%s", gv, resource, format.Object(api, 1))
	}

	// 每步之后我们检查 informer 是否追上
	// 以及期间它们收到了哪些事件。
	// 它们通过测试 context 取消来停止。
	// matchObject 生成一个 matcher，用于检查 list 操作的结果
	var resourceEvents, namespaceEvents eventRecorder
	resourceInformer := dynamicinformer.NewFilteredDynamicInformer(f.DynamicClient, gvr, "", 0, nil, func(options *metav1.ListOptions) {
		options.LabelSelector = listOptions.LabelSelector
	})
	_, err = resourceInformer.Informer().AddEventHandler(&resourceEvents)
	framework.ExpectNoError(err, "register resource event handler")
	listResource := func(_ context.Context) ([]runtime.Object, error) {
		return resourceInformer.Lister().List(labels.Everything())
	}
	go resourceInformer.Informer().RunWithContext(ctx)
	informersHaveSynced := []cache.InformerSynced{resourceInformer.Informer().HasSynced}
	var namespaceInformer informers.GenericInformer
	var listNamespace func(_ context.Context) ([]runtime.Object, error)
	if tc.IsNamespaced() {
		namespaceInformer = dynamicinformer.NewFilteredDynamicInformer(f.DynamicClient, gvr, f.Namespace.Name, 0, nil, func(options *metav1.ListOptions) {
			options.LabelSelector = listOptions.LabelSelector
		})
		_, err = namespaceInformer.Informer().AddEventHandler(&namespaceEvents)
		framework.ExpectNoError(err, "register namespace event handler")
		listNamespace = func(_ context.Context) ([]runtime.Object, error) {
			return namespaceInformer.Lister().List(labels.Everything())
		}
		go namespaceInformer.Informer().RunWithContext(ctx)
		informersHaveSynced = append(informersHaveSynced, namespaceInformer.Informer().HasSynced)
	}
	if !cache.WaitForNamedCacheSyncWithContext(ctx, informersHaveSynced...) {
		ginkgo.Fail("informers should have synced and didn't")
	}

	// 是否与期望对象一致。内容验证是可选的，没有它时，
	// 只检查 namespace 和 name。
	// matchEvents 生成一个 matcher，用于检查 informer 事件列表。
	matchObject := func(expectedObject *unstructured.Unstructured) gtypes.GomegaMatcher {
		return &matchObjectList{expectedObject: expectedObject, verifyContent: tc.VerifyContent()}
	}
	gomega.Expect(listResource(ctx)).Should(matchObject(nil), "initial list of resources from informer cache")
	gomega.Expect(resourceEvents.list()).To(gomega.HaveField("Events", gomega.BeEmpty()), "no events from resource informer yet")
	if listNamespace != nil {
		gomega.Expect(listNamespace(ctx)).Should(matchObject(nil), "initial list of namespace from informer cache")
		gomega.Expect(resourceEvents.list()).To(gomega.HaveField("Events", gomega.BeEmpty()), "no events from namespace informer yet")
	}

	// 事件应只涉及给定的对象。
	//
	// ResourceVersion 不得降低。
	// 有效事件序列以正则表达式形式给出，
	// 应用于 [EventList.Types] 返回的字符串。
	// 验证 ids 事件的 namespace/name/uid。
	matchEvents := func(obj *unstructured.Unstructured, regexp string) gtypes.GomegaMatcher {
		// 用 regexp 匹配 Types() 的结果。
		ids := gomega.HaveField("Events", gomega.Or(gomega.BeEmpty(), gomega.HaveEach(gomega.HaveField("ID()", gomega.Equal(fmt.Sprintf("%s, %s", klog.KObj(obj), obj.GetUID()))))))

		// 包含完整的对象 dump，HaveField 本身不会。
		order := gomega.HaveField("Types()", gomega.MatchRegexp(regexp))

		// ResourceVersion 不得降低。Delete 事件会重置序列。
		return framework.GomegaObject(gomega.And(ids, order))
	}

	// 尚无内容可比较，初始版本。
	var resourceRV, namespaceRV string
	nextEvents := func(rv *string, events eventList) {
		ginkgo.GinkgoHelper()

		checkNext := func(obj any) {
			ginkgo.GinkgoHelper()
			if obj == nil {
				return
			}
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			metaData, err := meta.Accessor(obj)
			framework.ExpectNoError(err, "access meta data")
			gomega.Expect(metaData).To(apimachineryutils.HaveValidResourceVersion())

			nextRV := metaData.GetResourceVersion()
			if *rv == "" {
				// 每步之后结果的验证（即验证什么）。
				*rv = nextRV
				return
			}

			cmpResult, err := resourceversion.CompareResourceVersion(nextRV, *rv)
			framework.ExpectNoError(err, "compare ResourceVersions")
			if cmpResult < 0 {
				framework.Failf("ResourceVersion %s in %s with UID %s is smaller than previous %s, should be equal or larger", nextRV, klog.KObj(metaData), metaData.GetUID(), *rv)
			}
			*rv = nextRV
		}

		for _, e := range events.Events {
			checkNext(e.oldObj)
			checkNext(e.newObj)
		}
	}

	// 可以检查对象内容，或仅限于 name/namespace。
	//
	// 还会检查 informer。在只读步骤之后这有点冗余，
	//
	// 但速度快，因此可以更频繁地执行。
	// 收集到的 informer 事件会被重置，因此每次 verify 调用必须匹配
	// 自上次调用以来的事件。
	// 这会在把失败交给 Ginkgo 之前捕获多种不同的失败。
	verify := func(what string, expected, actual *unstructured.Unstructured, haveExpectedEvents gtypes.GomegaMatcher) {
		ginkgo.GinkgoHelper()

		// 如果已有失败，立即中止检查，否则只会慢慢超时。
		var failures gomegaFailures

		if expected != nil {
			if tc.VerifyContent() {
				diff := compareObjects(expected, actual)
				failures.Add(fmt.Sprintf("%s: unexpected actual object (- expected, + actual):\n%s", what, diff))
			} else {
				failures.G().Expect(actual.GetName()).Should(gomega.Equal(expected.GetName()), "%s: name in returned object", what)
				failures.G().Expect(actual.GetNamespace()).Should(gomega.Equal(expected.GetNamespace()), "%s: namespace in returned object", what)
			}
			failures.G().Expect(actual).To(apimachineryutils.HaveValidResourceVersion())
		}

		// 即使缓存是最新的，仍需等待事件传递。
		failures.Check()

		failures.G().Eventually(ctx, listResource).Should(matchObject(expected), "list of resources from informer cache after %s", what)
		if haveExpectedEvents != nil {
			// 创建初始资源。
			failures.G().Eventually(resourceEvents.list).
				WithTimeout(5*time.Second).
				Should(haveExpectedEvents, "list of resource informer events after %s", what)
		}
		nextEvents(&resourceRV, resourceEvents.reset())
		if listNamespace != nil {
			failures.G().Eventually(ctx, listNamespace).Should(matchObject(expected), "list of namespace from informer cache after %s", what)
			if haveExpectedEvents != nil {
				failures.G().Eventually(namespaceEvents.list).
					WithTimeout(5*time.Second).
					Should(haveExpectedEvents, "list of namespace informer events after %s", what)
			}
			nextEvents(&namespaceRV, namespaceEvents.reset())
		}
		failures.Check()
	}

	// 始终清理。
	ginkgo.By(fmt.Sprintf("Creating:\n%s", format.Object(desiredInitialObject, 1)))
	existingObject, err := client.Create(ctx, desiredInitialObject, metav1.CreateOptions{FieldValidation: "Strict"})
	framework.ExpectNoError(err, "create initial %s", resourceType)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		// 先初始创建对象，然后由集群组件执行一些可选更新。
		err = client.Delete(ctx, desiredInitialObject.GetName(), metav1.DeleteOptions{})
		if apierrors.IsNotFound(err) {
			return
		}
		framework.ExpectNoError(err, "delete %s", resourceType)
		ensureNotFound(ctx, getResource)
	})
	verify("create", desiredInitialObject, existingObject,
		// Get 以检查存在性。
		matchEvents(existingObject, "^add,(update,)*$"),
	)
	createdResourceVersion := existingObject.GetResourceVersion()

	// 集群组件的可选更新。
	ginkgo.By(fmt.Sprintf("Getting %s", klog.KObj(desiredInitialObject)))
	existingObject, err = client.Get(ctx, desiredInitialObject.GetName(), metav1.GetOptions{})
	framework.ExpectNoError(err, "get updated %s", resourceType)
	verify("get", desiredInitialObject, existingObject,
		// 更新资源。重试，因为现有对象可能同时被更新。
		matchEvents(existingObject, "^(update,)*$"),
	)

	// 立即重试。
	mustGet := false
	gomega.Eventually(ctx, func(ctx context.Context) error {
		if mustGet {
			ginkgo.By(fmt.Sprintf("Getting updated %s", klog.KObj(desiredInitialObject)))
			existingObject, err = getResource(ctx)
			if err != nil {
				return fmt.Errorf("get existing %s: %w", resourceType, err)
			}
		}
		object := tc.GetUpdateSpec(existingObject.DeepCopy())
		ginkgo.By(fmt.Sprintf("Updating:\n%s", format.Object(object, 1)))
		existingObject, err = client.Update(ctx, object, metav1.UpdateOptions{})
		if err == nil {
			return nil
		}
		mustGet = apierrors.IsConflict(err)
		if mustGet {
			// 延迟后重试。
			return fmt.Errorf("update %s: %w", resourceType, err)
		}
		if retry, retryAfter := framework.ShouldRetry(err); retry {
			// 放弃，发生了其他错误。
			return gomega.TryAgainAfter(retryAfter)
		}
		// 至少一次更新。
		return gomega.StopTrying(fmt.Sprintf("update %s", resourceType)).Wrap(err)
	}).Should(gomega.Succeed())
	verify("update", desiredUpdatedObject, existingObject,
		// status 同样处理。此外读取 status（结果相同，但 endpoint+method 不同）。
		matchEvents(existingObject, "^(update,)+$"),
	)
	updatedResourceVersion := existingObject.GetResourceVersion()
	cmpResult, err := resourceversion.CompareResourceVersion(createdResourceVersion, updatedResourceVersion)
	framework.ExpectNoError(err, "compare ResourceVersion after create against ResourceVersion after update")
	if cmpResult >= 0 {
		framework.Failf("ResourceVersion should have increased during update and didn't (before: %s, after: %s)", createdResourceVersion, updatedResourceVersion)
	}

	// patch 资源，可能使用多种不同的 patch 类型。
	if tc.HasStatus() {
		mustGet := false
		gomega.Eventually(ctx, func(ctx context.Context) error {
			if mustGet {
				ginkgo.By(fmt.Sprintf("Getting updated %s", klog.KObj(desiredInitialObject)))
				existingObject, err = client.Get(ctx, desiredInitialObject.GetName(), metav1.GetOptions{}, "status")
				if err != nil {
					return fmt.Errorf("get existing %s: %w", resourceType, err)
				}
			}
			object := tc.GetUpdateStatus(existingObject)
			ginkgo.By(fmt.Sprintf("Updating status:\n%s", format.Object(object, 1)))
			existingObject, err = client.Update(ctx, object, metav1.UpdateOptions{}, "status")
			if err == nil {
				return nil
			}
			mustGet = apierrors.IsConflict(err)
			if mustGet {
				// 延迟后重试。
				return fmt.Errorf("update %s status: %w", resourceType, err)
			}
			if retry, retryAfter := framework.ShouldRetry(err); retry {
				// 放弃，发生了其他错误。
				return gomega.TryAgainAfter(retryAfter)
			}
			// 至少一次更新。
			return gomega.StopTrying(fmt.Sprintf("update %s status", resourceType)).Wrap(err)
		}).Should(gomega.Succeed())
		verify("update status", desiredUpdatedObjectWithStatus, existingObject,
			// status 同样处理。此外读取 status（结果相同，但 endpoint+method 不同）。
			matchEvents(existingObject, "^(update,)+$"),
		)

		ginkgo.By(fmt.Sprintf("Getting %s status", klog.KObj(desiredInitialObject)))
		existingObject, err = client.Get(ctx, desiredInitialObject.GetName(), metav1.GetOptions{}, "status")
		framework.ExpectNoError(err, "get updated %s", resourceType)
		verify("get", desiredUpdatedObjectWithStatus, existingObject,
			// 更新资源。重试，因为现有对象可能同时被更新。
			matchEvents(existingObject, "^(update,)*$"),
		)
	}

	// 如果启用了内容验证，每次结果必须相同。
	// 删除资源以便重新开始。
	for _, patch := range tc.GetPatchSpec() {
		// 可选更新、删除。
		ginkgo.By(fmt.Sprintf("Deleting %s", klog.KObj(desiredInitialObject)))
		err = client.Delete(ctx, desiredInitialObject.GetName(), metav1.DeleteOptions{})
		framework.ExpectNoError(err, "delete updated %s", resourceType)
		ensureNotFound(ctx, getResource)
		verify("delete", nil, nil,
			// 我们必须在这里验证，
			//
			// 否则无法保证能看到 delete 事件。
			// 为 patch 重新创建。
			matchEvents(existingObject, "^(update,)*delete,$"),
		)

		// 重新创建后至少一次更新。
		ginkgo.By(fmt.Sprintf("Creating again:\n%s", format.Object(desiredInitialObject, 1)))
		existingObject, err = client.Create(ctx, desiredInitialObject, metav1.CreateOptions{})
		framework.ExpectNoError(err, "create %s again", resourceType)
		patchData := patch.GetData(existingObject)

		ginkgo.By(fmt.Sprintf("Patching with %s:\n%s", patch.Type, string(patchData)))
		options := metav1.PatchOptions{FieldValidation: "Strict"}
		switch patch.Type {
		case types.ApplyYAMLPatchType, types.ApplyCBORPatchType:
			options.FieldManager = "test-apply"
			options.Force = ptr.To(true)
		}
		existingObject, err = client.Patch(ctx, desiredInitialObject.GetName(), patch.Type, patchData, options)
		framework.ExpectNoError(err, "patch %s %s", patch.Type, resourceType)
		verify(fmt.Sprintf("patch %s", patch.Type), desiredUpdatedObject, existingObject,
			// status 同样处理。patch 在更新后的对象上应用。
			matchEvents(existingObject, "^add,(update,)+$"),
		)
	}

	// 再次更新。
	for _, patch := range tc.GetPatchStatus(existingObject) {
		// 可选更新、删除。
		ginkgo.By(fmt.Sprintf("Deleting %s", klog.KObj(desiredInitialObject)))
		err = client.Delete(ctx, desiredInitialObject.GetName(), metav1.DeleteOptions{})
		framework.ExpectNoError(err, "delete updated %s", resourceType)
		ensureNotFound(ctx, getResource)
		verify("delete", nil, nil,
			// 我们必须在这里验证，
			//
			// 否则无法保证能看到 delete 事件。
			// 为 patch 重新创建。
			matchEvents(existingObject, "^(update,)*delete,$"),
		)

		// 重新创建后至少一次更新。
		ginkgo.By(fmt.Sprintf("Creating again:\n%s", format.Object(desiredInitialObject, 1)))
		existingObject, err = client.Create(ctx, desiredInitialObject, metav1.CreateOptions{})
		framework.ExpectNoError(err, "create %s again", resourceType)

		// 在 List 和 DeleteCollection 调用中使用该 label 作为 selector。
		mustGet := false
		gomega.Eventually(ctx, func(ctx context.Context) error {
			if mustGet {
				ginkgo.By(fmt.Sprintf("Getting updated %s", klog.KObj(desiredInitialObject)))
				existingObject, err = getResource(ctx)
				if err != nil {
					return fmt.Errorf("get existing %s: %w", resourceType, err)
				}
			}
			object := tc.GetUpdateSpec(existingObject.DeepCopy())
			ginkgo.By(fmt.Sprintf("Updating:\n%s", format.Object(object, 1)))
			existingObject, err = client.Update(ctx, object, metav1.UpdateOptions{})
			if err == nil {
				return nil
			}
			mustGet = apierrors.IsConflict(err)
			if mustGet {
				// 延迟后重试。
				return fmt.Errorf("update %s: %w", resourceType, err)
			}
			if retry, retryAfter := framework.ShouldRetry(err); retry {
				// 放弃，发生了其他错误。
				return gomega.TryAgainAfter(retryAfter)
			}
			// 至少一次更新。
			return gomega.StopTrying(fmt.Sprintf("update %s", resourceType)).Wrap(err)
		}).Should(gomega.Succeed())
		patchData := patch.GetData(existingObject)

		ginkgo.By(fmt.Sprintf("Patching status with %s:\n%s", patch.Type, string(patchData)))
		options := metav1.PatchOptions{FieldValidation: "Strict"}
		switch patch.Type {
		case types.ApplyYAMLPatchType, types.ApplyCBORPatchType:
			options.FieldManager = "test-apply"
			options.Force = ptr.To(true)
		}
		existingObject, err = client.Patch(ctx, desiredInitialObject.GetName(), patch.Type, patchData, options, "status")
		framework.ExpectNoError(err, "patch %s %s status", patch.Type, resourceType)
		verify(fmt.Sprintf("patch %s status", patch.Type), desiredUpdatedObject, existingObject,
			// status 同样处理。patch 在更新后的对象上应用。
			matchEvents(existingObject, "^add,(update,)+$"),
		)
	}

	// 集群组件的可选更新，然后删除。
	ginkgo.By(fmt.Sprintf("Listing %s collection with label selector %s", gvr, listOptions.LabelSelector))
	items, err := client.List(ctx, listOptions)
	framework.ExpectNoError(err, "list %s", resourceType)
	gomega.Expect(items.Items).Should(gomega.HaveLen(1), "Should have listed exactly the test resource.")
	verify("list", desiredUpdatedObject, &items.Items[0],
		// 更新资源。重试，因为现有对象可能同时被更新。
		matchEvents(existingObject, "^(update,)*$"),
	)

	if tc.IsNamespaced() {
		ginkgo.By(fmt.Sprintf("Listing %s without namespace and with label selector %s", gvr, listOptions.LabelSelector))
		items, err := resourceClient.List(ctx, listOptions)
		framework.ExpectNoError(err, "list %s in all namespaces", resourceType)
		gomega.Expect(items.Items).Should(gomega.HaveLen(1), "Should have listed exactly the test resource in all namespaces.")
		verify("list all namespaces", desiredUpdatedObject, &items.Items[0],
			// 更新资源。重试，因为现有对象可能同时被更新。
			matchEvents(existingObject, "^(update,)*$"),
		)
	}

	ginkgo.By(fmt.Sprintf("Deleting %s collection with label selector %s", gvr, listOptions.LabelSelector))
	err = client.DeleteCollection(ctx, metav1.DeleteOptions{}, listOptions)
	framework.ExpectNoError(err, "delete collection of %s", resourceType)
	ensureNotFound(ctx, getResource)
	verify("list", nil, nil,
		// ensureNotFound 确保 get 函数返回的错误是 NotFound。
		matchEvents(existingObject, "^(update,)*delete,$"),
	)
}

// 可在删除对象后调用，以确保它确实被移除，
// 而不只是通过 DeletionTimestamp 标记为待删除。删除并不
// 一定是即时的，例如某个集群组件可能添加自己的
// finalizer。
/* 来自以往的一致性测试，例如 https://github.com/kubernetes/kubernetes/blame/be361a18dda0f2fab1f5e25f8067a9ed43fc3b89/test/e2e/storage/storageclass.go#L152 */
func ensureNotFound(ctx context.Context, get func(context.Context) (*unstructured.Unstructured, error)) {
	ginkgo.GinkgoHelper()
	ginkgo.By("Checking for existence")
	gomega.Eventually(ctx, func(ctx context.Context) error {
		obj, err := framework.HandleRetry(get)(ctx)
		switch {
		case apierrors.IsNotFound(err):
			return nil
		case err != nil:
			return fmt.Errorf("unexpected error after GET: %w", err)
		default:
			return fmt.Errorf("resource not removed yet:\n%s", format.Object(obj, 1))
		}
	}).WithTimeout(30 * time.Second /* 来自以往的一致性测试，例如 https://github.com/kubernetes/kubernetes/blame/be361a18dda0f2fab1f5e25f8067a9ed43fc3b89/test/e2e/storage/storageclass.go#L152 */).
		Should(gomega.Succeed())
}
