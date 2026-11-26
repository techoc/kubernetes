/*
Copyright 2018 The Kubernetes Authors.

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

package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/test/e2e/framework"
	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

// LoadFromManifests 加载 .yaml 或 .json manifest 文件，返回
// 其中找到的所有 item。支持在 factories 中注册了 factory 的所有 item，
// 以及用 "---" 分隔多个 item 的 .yaml 文件。文件通过
// "testfiles" 包访问，因此可来自文件系统
// 或被编译进二进制。

//
// LoadFromManifests 有一些限制：
//   - 不支持 alias（即使用 serviceAccountName 而不是已弃用的 serviceAccount，
//     https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1）
//     且会被静默忽略
//   - 无论 manifest 文件指定什么版本，每个 item 都用最新的稳定 API 版本

func LoadFromManifests(files ...string) ([]interface{}, error) {
	var items []interface{}
	err := visitManifests(func(data []byte) error {
		// 暂时忽略其他字段，只确定我们有什么类型。
		var what What
		if err := runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), data, &what); err != nil {
			return fmt.Errorf("decode TypeMeta: %w", err)
		}
		// 忽略空文档。
		if what.Kind == "" {
			return nil
		}

		factory := factories[what]
		if factory == nil {
			return fmt.Errorf("item of type %+v not supported", what)
		}

		object := factory.New()
		if err := runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), data, object); err != nil {
			return fmt.Errorf("decode %+v: %w", what, err)
		}
		items = append(items, object)
		return nil
	}, files...)

	return items, err
}

func visitManifests(cb func([]byte) error, files ...string) error {
	for _, fileName := range files {
		data, err := e2etestfiles.Read(fileName)
		if err != nil {
			framework.Failf("reading manifest file: %v", err)
		}

		// 处理单个 item 前，先在 "---" 分隔符处拆分。
		// 仅对 .yaml 有效。
		//
		// 必须自己拆分，因为 runtime.DecodeInto 需要访问
		// 每个原始数据块。
		// kubectl 有自己的基础设施来做这件事，但那需要大量代码
		// 和许多依赖。
		//
		items := bytes.Split(data, []byte("\n---"))

		for _, item := range items {
			if err := cb(item); err != nil {
				return fmt.Errorf("%s: %w", fileName, err)
			}
		}
	}
	return nil
}

// PatchItems 原地修改给定 item，使每个测试
// 拥有自己的实例，避免不同测试之间以及测试与正常部署之间冲突。
//
//
// 做法如下：
// - 在测试命名空间内创建 namespaced item
// - 修改非 namespaced item（如 ClusterRole）的名称

// PatchItems 有一些限制：
// - 只支持部分常见 item，未知 item 会报错
// - 每个 item 只支持最新的稳定 API 版本
func PatchItems(f *framework.Framework, driverNamespace *v1.Namespace, items ...interface{}) error {

	tCtx := f.TContext(context.Background())
	if driverNamespace != nil {
		tCtx = tCtx.WithNamespace(driverNamespace.Name)
	}
	return PatchItemsTCtx(tCtx, items...)
}

// PatchItemsTCtx 是 PatchItems 的变体，所有参数（包括
// namespace）都通过 TContext 传入。
func PatchItemsTCtx(tCtx ktesting.TContext, items ...interface{}) error {
	for _, item := range items {
		// 调试 item 加载和打补丁时取消注释。
		// Logf("patching original content of %T:\n%s", item, PrettyPrint(item))
		if err := patchItemRecursively(tCtx, item); err != nil {
			return err
		}
	}
	return nil
}

// createItems 创建 item。每个 item 必须是 Factory 中注册类型的 API 对象。
//
// 对象在测试清理时自动删除。
func createItems(tCtx ktesting.TContext, items ...interface{}) error {

	var result error
	for _, item := range items {
		// 每个 factory 知道自己支持哪些 item，逐个尝试即可。
		done := false
		description := describeItem(item)
		// 取消注释这行可获得整个 item 的完整 dump。
		// description = fmt.Sprintf("%s:\n%s", description, PrettyPrint(item))
		tCtx.Logf("creating %s", description)
		for _, factory := range factories {
			destructor, err := factory.Create(tCtx, item)
			if destructor != nil {
				tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
					err := destructor(tCtx)
					if apierrors.IsNotFound(err) {
						return
					}
					tCtx.ExpectNoError(err, fmt.Sprintf("deleting %s", description))
				})
			}
			if err == nil {
				done = true
				break
			} else if !errors.Is(err, errorItemNotSupported) {
				result = err
				break
			}
		}
		if result == nil && !done {
			result = fmt.Errorf("item of type %T not supported", item)
			break
		}
	}

	return result
}

// CreateFromManifests 组合了 LoadFromManifests、
// PatchItems、用可选自定义函数打补丁，
// 以及创建最终的 item。
//
// 对象在测试清理时自动删除。
func CreateFromManifests(ctx context.Context, f *framework.Framework, driverNamespace *v1.Namespace, patch func(item interface{}) error, files ...string) error {
	tCtx := f.TContext(ctx)
	if driverNamespace != nil {
		tCtx = tCtx.WithNamespace(driverNamespace.Name)
	}
	return CreateFromManifestsTCtx(tCtx, patch, files...)
}

// CreateFromManifestsTCtx 是 CreateFromManifests 的变体，所有参数（包括
// driver namespace）都通过 TContext 传入，因此可在 Go 单元测试中使用。
func CreateFromManifestsTCtx(tCtx ktesting.TContext, patch func(item interface{}) error, files ...string) error {
	items, err := LoadFromManifests(files...)
	if err != nil {
		return fmt.Errorf("CreateFromManifests: %w", err)
	}
	if err := PatchItemsTCtx(tCtx, items...); err != nil {
		return err
	}
	if patch != nil {
		for _, item := range items {
			if err := patch(item); err != nil {
				return err
			}
		}
	}
	return createItems(tCtx, items...)
}

// What 是 metav1.TypeMeta 的子集，但与 metav1.TypeMeta 本身不同，
// 它满足 runtime.Object 接口。
type What struct {
	Kind string `json:"kind"`
}

// DeepCopy 是自动生成的 deepcopy 函数，复制接收者并创建新的 What。
func (in *What) DeepCopy() *What {
	return &What{Kind: in.Kind}
}

// DeepCopyInto 是自动生成的 deepcopy 函数，把接收者写入 out。
func (in *What) DeepCopyInto(out *What) {
	out.Kind = in.Kind
}

// DeepCopyObject 是自动生成的 deepcopy 函数，复制接收者并创建新的 runtime.Object。
func (in *What) DeepCopyObject() runtime.Object {
	return &What{Kind: in.Kind}
}

// GetObjectKind 返回 ObjectKind schema
func (in *What) GetObjectKind() schema.ObjectKind {
	return nil
}

// ItemFactory 支持创建某一种特定 item。
// 该类型导出是因为其他包可能想
// 扩展预定义 factory 的集合。
type ItemFactory interface {
	// New 返回一个新的空 item。
	New() runtime.Object

	// Create 负责创建 item。返回
	// 错误或已创建 item 的清理函数。
	// item 类型不受支持时，必须返回
	// 以 errorItemNotSupported 为 cause 的错误。
	Create(tCtx ktesting.TContext, item interface{}) (func(ctx context.Context) error, error)
}

// describeItem 始终返回描述该 item 的字符串，
// 通常调用 cache.MetaNamespaceKeyFunc 拼接
// namespace（如果有）和 name。失败时改为把整个
// item 转成字符串。
func describeItem(item interface{}) string {
	key, err := cache.MetaNamespaceKeyFunc(item)
	if err == nil && key != "" {
		return fmt.Sprintf("%T: %s", item, key)
	}
	return fmt.Sprintf("%T: %s", item, item)
}

// errorItemNotSupported 是 Create 方法不支持给定 item 时
// 必须返回或包装的错误。
var errorItemNotSupported = errors.New("not supported")

var factories = map[What]ItemFactory{
	{"ClusterRole"}:              &clusterRoleFactory{},
	{"ClusterRoleBinding"}:       &clusterRoleBindingFactory{},
	{"CSIDriver"}:                &csiDriverFactory{},
	{"DaemonSet"}:                &daemonSetFactory{},
	{"ReplicaSet"}:               &replicaSetFactory{},
	{"Role"}:                     &roleFactory{},
	{"RoleBinding"}:              &roleBindingFactory{},
	{"Secret"}:                   &secretFactory{},
	{"Service"}:                  &serviceFactory{},
	{"ServiceAccount"}:           &serviceAccountFactory{},
	{"StatefulSet"}:              &statefulSetFactory{},
	{"Deployment"}:               &deploymentFactory{},
	{"StorageClass"}:             &storageClassFactory{},
	{"VolumeAttributesClass"}:    &volumeAttributesClassFactory{},
	{"CustomResourceDefinition"}: &customResourceDefinitionFactory{},
}

// patchName 通过追加生成的唯一名称让某个 item 的名字唯一。
// patchName 通过追加生成的唯一名称让某个 item 的名字唯一。
func patchName(uniqueName string, item *string) {
	if *item != "" {
		*item = *item + "-" + uniqueName
	}
}

// patchNamespace 把 item 移到测试命名空间。并非
// 所有 item 都能 namespaced，对那些 item 还需要
// 修改名称。
func patchNamespace(tCtx ktesting.TContext, item *string) {
	namespace := tCtx.Namespace()
	if namespace != "" {
		*item = namespace
	}
}

func patchItemRecursively(tCtx ktesting.TContext, item interface{}) error {
	uniqueName := tCtx.Namespace()
	switch item := item.(type) {
	case *rbacv1.Subject:
		patchNamespace(tCtx, &item.Namespace)
	case *rbacv1.RoleRef:
		// TODO: 避免硬编码这个特殊名称。也许加一个 Framework.PredefinedRoles
		// 包含测试开始前在集群范围内定义的所有 role 名？
		// 这些名字都免于重命名。该列表可通过查询生成，
		// 也允许测试补充。
		if item.Name != "e2e-test-privileged-psp" {
			patchName(uniqueName, &item.Name)
		}
	case *rbacv1.ClusterRole:
		patchName(uniqueName, &item.Name)
	case *rbacv1.Role:
		patchNamespace(tCtx, &item.Namespace)
		// Role 是 namespaced 的，但上面的 RoleRef 不确定引用的
		// 是 ClusterRole 还是 Role，因此总是重命名；
		// 这里必须做同样处理。
		patchName(uniqueName, &item.Name)
	case *storagev1.StorageClass:
		patchName(uniqueName, &item.Name)
	case *storagev1.VolumeAttributesClass:
		patchName(uniqueName, &item.Name)
	case *storagev1.CSIDriver:
		patchName(uniqueName, &item.Name)
	case *v1.ServiceAccount:
		patchNamespace(tCtx, &item.ObjectMeta.Namespace)
	case *v1.Secret:
		patchNamespace(tCtx, &item.ObjectMeta.Namespace)
	case *rbacv1.ClusterRoleBinding:
		patchName(uniqueName, &item.Name)
		for i := range item.Subjects {
			if err := patchItemRecursively(tCtx, &item.Subjects[i]); err != nil {
				return fmt.Errorf("%T: %w", &item.Subjects[i], err)
			}
		}
		if err := patchItemRecursively(tCtx, &item.RoleRef); err != nil {
			return fmt.Errorf("%T: %w", &item.RoleRef, err)
		}
	case *rbacv1.RoleBinding:
		patchNamespace(tCtx, &item.Namespace)
		for i := range item.Subjects {
			if err := patchItemRecursively(tCtx, &item.Subjects[i]); err != nil {
				return fmt.Errorf("%T: %w", &item.Subjects[i], err)
			}
		}
		if err := patchItemRecursively(tCtx, &item.RoleRef); err != nil {
			return fmt.Errorf("%T: %w", &item.RoleRef, err)
		}
	case *v1.Service:
		patchNamespace(tCtx, &item.ObjectMeta.Namespace)
	case *appsv1.StatefulSet:
		patchNamespace(tCtx, &item.ObjectMeta.Namespace)
		if err := patchContainerImages(item.Spec.Template.Spec.Containers); err != nil {
			return err
		}
		if err := patchContainerImages(item.Spec.Template.Spec.InitContainers); err != nil {
			return err
		}
	case *appsv1.Deployment:
		patchNamespace(tCtx, &item.ObjectMeta.Namespace)
		if err := patchContainerImages(item.Spec.Template.Spec.Containers); err != nil {
			return err
		}
		if err := patchContainerImages(item.Spec.Template.Spec.InitContainers); err != nil {
			return err
		}
	case *appsv1.DaemonSet:
		patchNamespace(tCtx, &item.ObjectMeta.Namespace)
		if err := patchContainerImages(item.Spec.Template.Spec.Containers); err != nil {
			return err
		}
		if err := patchContainerImages(item.Spec.Template.Spec.InitContainers); err != nil {
			return err
		}
	case *appsv1.ReplicaSet:
		patchNamespace(tCtx, &item.ObjectMeta.Namespace)
		if err := patchContainerImages(item.Spec.Template.Spec.Containers); err != nil {
			return err
		}
		if err := patchContainerImages(item.Spec.Template.Spec.InitContainers); err != nil {
			return err
		}
	case *apiextensionsv1.CustomResourceDefinition:
		// 什么都不做。给所有 CRD 打补丁改名并不总是预期行为。
	default:
		return fmt.Errorf("missing support for patching item of type %T", item)
	}
	return nil
}

// 各个 factory 遵循相同模板，但类型和函数差异足够大，
// 复制粘贴看起来是最不脏的做法。也许有一天 Go 会有
// 泛型。
//

type serviceAccountFactory struct{}

func (f *serviceAccountFactory) New() runtime.Object {
	return &v1.ServiceAccount{}
}

func (*serviceAccountFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*v1.ServiceAccount)
	if !ok {
		return nil, errorItemNotSupported
	}
	client := tCtx.Client().CoreV1().ServiceAccounts(tCtx.Namespace())
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create ServiceAccount: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type clusterRoleFactory struct{}

func (f *clusterRoleFactory) New() runtime.Object {
	return &rbacv1.ClusterRole{}
}

func (*clusterRoleFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*rbacv1.ClusterRole)
	if !ok {
		return nil, errorItemNotSupported
	}

	tCtx.Logf("define cluster role %v", item.GetName())
	client := tCtx.Client().RbacV1().ClusterRoles()
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create ClusterRole: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type clusterRoleBindingFactory struct{}

func (f *clusterRoleBindingFactory) New() runtime.Object {
	return &rbacv1.ClusterRoleBinding{}
}

func (*clusterRoleBindingFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*rbacv1.ClusterRoleBinding)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().RbacV1().ClusterRoleBindings()
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create ClusterRoleBinding: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type roleFactory struct{}

func (f *roleFactory) New() runtime.Object {
	return &rbacv1.Role{}
}

func (*roleFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*rbacv1.Role)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().RbacV1().Roles(tCtx.Namespace())
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create Role: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type roleBindingFactory struct{}

func (f *roleBindingFactory) New() runtime.Object {
	return &rbacv1.RoleBinding{}
}

func (*roleBindingFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*rbacv1.RoleBinding)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().RbacV1().RoleBindings(tCtx.Namespace())
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create RoleBinding: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type serviceFactory struct{}

func (f *serviceFactory) New() runtime.Object {
	return &v1.Service{}
}

func (*serviceFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*v1.Service)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().CoreV1().Services(tCtx.Namespace())
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create Service: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type statefulSetFactory struct{}

func (f *statefulSetFactory) New() runtime.Object {
	return &appsv1.StatefulSet{}
}

func (*statefulSetFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*appsv1.StatefulSet)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().AppsV1().StatefulSets(tCtx.Namespace())
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create StatefulSet: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type deploymentFactory struct{}

func (f *deploymentFactory) New() runtime.Object {
	return &appsv1.Deployment{}
}

func (*deploymentFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*appsv1.Deployment)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().AppsV1().Deployments(tCtx.Namespace())
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create Deployment: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type daemonSetFactory struct{}

func (f *daemonSetFactory) New() runtime.Object {
	return &appsv1.DaemonSet{}
}

func (*daemonSetFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*appsv1.DaemonSet)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().AppsV1().DaemonSets(tCtx.Namespace())
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create DaemonSet: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type replicaSetFactory struct{}

func (f *replicaSetFactory) New() runtime.Object {
	return &appsv1.ReplicaSet{}
}

func (*replicaSetFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*appsv1.ReplicaSet)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace())
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create ReplicaSet: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type storageClassFactory struct{}

func (f *storageClassFactory) New() runtime.Object {
	return &storagev1.StorageClass{}
}

func (*storageClassFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*storagev1.StorageClass)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().StorageV1().StorageClasses()
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create StorageClass: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type volumeAttributesClassFactory struct{}

func (f *volumeAttributesClassFactory) New() runtime.Object {
	return &storagev1.VolumeAttributesClass{}
}

func (*volumeAttributesClassFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*storagev1.VolumeAttributesClass)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().StorageV1().VolumeAttributesClasses()
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create VolumeAttributesClass: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type csiDriverFactory struct{}

func (f *csiDriverFactory) New() runtime.Object {
	return &storagev1.CSIDriver{}
}

func (*csiDriverFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*storagev1.CSIDriver)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().StorageV1().CSIDrivers()
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create CSIDriver: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type secretFactory struct{}

func (f *secretFactory) New() runtime.Object {
	return &v1.Secret{}
}

func (*secretFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	item, ok := i.(*v1.Secret)
	if !ok {
		return nil, errorItemNotSupported
	}

	client := tCtx.Client().CoreV1().Secrets(tCtx.Namespace())
	if _, err := client.Create(tCtx, item, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create Secret: %w", err)
	}
	return func(ctx context.Context) error {
		return client.Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

type customResourceDefinitionFactory struct{}

func (f *customResourceDefinitionFactory) New() runtime.Object {
	return &apiextensionsv1.CustomResourceDefinition{}
}

func (*customResourceDefinitionFactory) Create(tCtx ktesting.TContext, i interface{}) (func(ctx context.Context) error, error) {
	var err error
	unstructCRD := &unstructured.Unstructured{}
	gvr := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}

	item, ok := i.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		return nil, errorItemNotSupported
	}

	unstructCRD.Object, err = runtime.DefaultUnstructuredConverter.ToUnstructured(i)
	if err != nil {
		return nil, err
	}

	if _, err = tCtx.Dynamic().Resource(gvr).Create(tCtx, unstructCRD, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create CustomResourceDefinition: %w", err)
	}
	return func(ctx context.Context) error {
		return tCtx.Dynamic().Resource(gvr).Delete(ctx, item.GetName(), metav1.DeleteOptions{})
	}, nil
}

// PrettyPrint 返回 item 的可读表示。
func PrettyPrint(item interface{}) string {
	data, err := json.MarshalIndent(item, "", "  ")
	if err == nil {
		return string(data)
	}
	return fmt.Sprintf("%+v", item)
}

// patchContainerImages 用 KUBE_TEST_REPO_LIST 环境变量提供的自定义 registry
// 替换指定的 Container Registry
func patchContainerImages(containers []v1.Container) error {
	var err error
	for i, c := range containers {
		containers[i].Image, err = imageutils.ReplaceRegistryInImageURL(c.Image)
		if err != nil {
			return err
		}
	}

	return nil
}
