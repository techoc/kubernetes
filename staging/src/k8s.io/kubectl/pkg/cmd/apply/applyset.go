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

package apply

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
)

// ApplySet 规范中的 label 和 annotation 键。
// https://git.k8s.io/enhancements/keps/sig-cli/3659-kubectl-apply-prune#design-details-applyset-specification
const (
	// ApplySetToolingAnnotation 是 label 的键，用于指示由哪个工具管理此 ApplySet。
	// 工具应拒绝修改属于其他工具的 ApplySet。
	// 值必须采用 <toolname>/<semver> 格式。
	// 示例值："kubectl/v1.27"、"helm/v3" 或 "kpt/v1.0.0"
	ApplySetToolingAnnotation = "applyset.kubernetes.io/tooling"

	// ApplySetAdditionalNamespacesAnnotation 注解将 ApplySet 的范围扩展到 parent
	// 对象自身的 namespace（如有）之外，使其包含列出的 namespace。值是以逗号分隔的
	// namespace 名称列表，这些 namespace 不同于 parent 所在 namespace，但其中存在成员对象
	// 示例值："kube-system,ns1,ns2"。
	ApplySetAdditionalNamespacesAnnotation = "applyset.kubernetes.io/additional-namespaces"

	// Deprecated: ApplySetGRsAnnotation is a list of group-resources used to optimize listing of ApplySet member objects.
	// 在 ApplySet 规范中它是可选的，因为工具可以执行发现或使用其他优化方式。
	// 不过，目前在 kubectl 中它是必需的。
	// 如果存在，此注解的值必须是以逗号分隔的 group-resource 列表，
	// 采用完全限定名称格式，即 <resourcename>.<group>。
	// 示例值："certificates.cert-manager.io,configmaps,deployments.apps,secrets,services"
	// 已弃用并由 ApplySetGKsAnnotation 取代，在 applyset 进入 beta 或 GA 时可移除对其的支持。
	DeprecatedApplySetGRsAnnotation = "applyset.kubernetes.io/contains-group-resources"

	// ApplySetGKsAnnotation 是用于优化列出 ApplySet 成员对象的 group-kind 列表。
	// 在 ApplySet 规范中它是可选的，因为工具可以执行发现或使用其他优化方式。
	// 不过，目前在 kubectl 中它是必需的。
	// 如果存在，此注解的值必须是以逗号分隔的 group-kind 列表，
	// 采用完全限定名称格式，即 <kind>.<group>。
	// 示例值："Certificate.cert-manager.io,ConfigMap,deployments.apps,Secret,Service"
	ApplySetGKsAnnotation = "applyset.kubernetes.io/contains-group-kinds"

	// ApplySetParentIDLabel 是使对象成为 ApplySet parent 对象的 label 键。
	// 其值必须使用下面 V1ApplySetIdFormat 中指定的格式
	ApplySetParentIDLabel = "applyset.kubernetes.io/id"

	// V1ApplySetIdFormat 是 ApplySetParentIDLabel（以及 ApplysetPartOfLabel）的值所要求的格式。
	// 其中 %s 段是对象自身的唯一 ID，必须是该对象 GKNN 哈希的 base64 编码
	// （使用 RFC4648 的 URL 安全编码），形式为：
	// base64(sha256(<name>.<namespace>.<kind>.<group>)).
	V1ApplySetIdFormat = "applyset-%s-v1"

	// ApplysetPartOfLabel 是表明对象属于某个 ApplySet 成员的 label 键。
	// 该 label 的值必须与 parent 对象上 ApplySetParentIDLabel 的值一致。
	ApplysetPartOfLabel = "applyset.kubernetes.io/part-of"

	// ApplysetParentCRDLabel 是可设置在 CRD 上的 label 键，用于标识
	// 它所定义的自定义资源类型（而非 CRD 本身）是 ApplySet 允许的 parent。
	ApplysetParentCRDLabel = "applyset.kubernetes.io/is-parent-type"
)

var defaultApplySetParentGVR = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}

// ApplySet 记录一次 applyset apply/prune 的相关信息
type ApplySet struct {
	// parentRef 指向用于跟踪 applyset 的 parent 对象。
	parentRef *ApplySetParentRef

	// toolingID 是要在 applyset.kubernetes.io/tooling 注解中使用并校验的值。
	toolingID ApplySetTooling

	// currentResources 是当前操作开始时属于服务端集合的资源集合。
	currentResources map[schema.GroupKind]*kindInfo

	// currentNamespaces 是当前操作开始时包含此 applyset 中对象的 namespace 集合。
	currentNamespaces sets.Set[string]

	// updatedResources 是当前操作完成后将成为集合中一员的资源集合。
	updatedResources map[schema.GroupKind]*kindInfo

	// updatedNamespaces 是当前操作完成后将包含此 applyset 中对象的 namespace 集合。
	updatedNamespaces sets.Set[string]

	restMapper meta.RESTMapper

	// client 是专用于 ApplySet parent 对象类型的客户端
	client resource.RESTClient
}

var builtinApplySetParentGVRs = sets.New[schema.GroupVersionResource](
	defaultApplySetParentGVR,
	schema.GroupVersionResource{Version: "v1", Resource: "configmaps"},
)

// ApplySetParentRef 存储用于跟踪 applyset 的 parent 对象的对象元信息和类型元信息。
type ApplySetParentRef struct {
	Name      string
	Namespace string
	*meta.RESTMapping
}

func (p ApplySetParentRef) IsNamespaced() bool {
	return p.Scope.Name() == meta.RESTScopeNameNamespace
}

// String 使用与 CLI 上 --applyset flag 所期望的相同格式，
// 返回 parent 对象的字符串表示。
func (p ApplySetParentRef) String() string {
	return fmt.Sprintf("%s.%s/%s", p.Resource.Resource, p.Resource.Group, p.Name)
}

type ApplySetTooling struct {
	Name    string
	Version string
}

func (t ApplySetTooling) String() string {
	return fmt.Sprintf("%s/%s", t.Name, t.Version)
}

// NewApplySet 创建一个新的 ApplySet 对象，由给定的 parent 对象跟踪。
func NewApplySet(parent *ApplySetParentRef, tooling ApplySetTooling, mapper meta.RESTMapper, client resource.RESTClient) *ApplySet {
	return &ApplySet{
		currentResources:  make(map[schema.GroupKind]*kindInfo),
		currentNamespaces: make(sets.Set[string]),
		updatedResources:  make(map[schema.GroupKind]*kindInfo),
		updatedNamespaces: make(sets.Set[string]),
		parentRef:         parent,
		toolingID:         tooling,
		restMapper:        mapper,
		client:            client,
	}
}

const applySetIDPartDelimiter = "."

// ID 是我们用来标识此 applyset 的 label 值。
// 格式：base64(sha256(<name>.<namespace>.<kind>.<group>))，使用 RFC4648 的 URL 安全编码。

func (a ApplySet) ID() string {
	unencoded := strings.Join([]string{a.parentRef.Name, a.parentRef.Namespace, a.parentRef.GroupVersionKind.Kind, a.parentRef.GroupVersionKind.Group}, applySetIDPartDelimiter)
	hashed := sha256.Sum256([]byte(unencoded))
	b64 := base64.RawURLEncoding.EncodeToString(hashed[:])
	// label 值必须以字母数字字符开头和结尾，因此添加已知安全的前缀和后缀。
	return fmt.Sprintf(V1ApplySetIdFormat, b64)
}

// Validate 对用于跟踪 applyset 的 parent 对象施加限制。
func (a ApplySet) Validate(ctx context.Context, client dynamic.Interface) error {
	var errors []error
	if a.parentRef.IsNamespaced() && a.parentRef.Namespace == "" {
		errors = append(errors, fmt.Errorf("namespace is required to use namespace-scoped ApplySet"))
	}
	if !builtinApplySetParentGVRs.Has(a.parentRef.Resource) {
		// 确定哪些自定义资源类型可用作 ApplySet 的 parent。
		// 优化：由于这会产生请求，仅当使用的不是默认类型时才执行。
		permittedCRParents, err := a.getAllowedCustomResourceParents(ctx, client)
		if err != nil {
			errors = append(errors, fmt.Errorf("identifying allowed custom resource parent types: %w", err))
		}
		parentRefResourceIgnoreVersion := a.parentRef.Resource.GroupResource().WithVersion("")
		if !permittedCRParents.Has(parentRefResourceIgnoreVersion) {
			errors = append(errors, fmt.Errorf("resource %q is not permitted as an ApplySet parent", a.parentRef.Resource))
		}
	}
	return utilerrors.NewAggregate(errors)
}

func (a *ApplySet) labelForCustomParentCRDs() *metav1.LabelSelector {
	return &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      ApplysetParentCRDLabel,
			Operator: metav1.LabelSelectorOpExists,
		}},
	}
}

func (a *ApplySet) getAllowedCustomResourceParents(ctx context.Context, client dynamic.Interface) (sets.Set[schema.GroupVersionResource], error) {
	opts := metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(a.labelForCustomParentCRDs()),
	}
	list, err := client.Resource(schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	set := sets.New[schema.GroupVersionResource]()
	for i := range list.Items {
		// 自定义资源必须命名为 `<names.plural>.<group>`
		// 并且在 `/apis/<group>/<version>/.../<plural>` 下提供
		gr := schema.ParseGroupResource(list.Items[i].GetName())
		set.Insert(gr.WithVersion(""))
	}
	return set, nil
}

func (a *ApplySet) LabelsForMember() map[string]string {
	return map[string]string{
		ApplysetPartOfLabel: a.ID(),
	}
}

// addLabels 在每个对象上设置我们的跟踪 label；应在加载对象时调用。
func (a *ApplySet) AddLabels(objects ...*resource.Info) error {
	applysetLabels := a.LabelsForMember()
	for _, obj := range objects {
		accessor, err := meta.Accessor(obj.Object)
		if err != nil {
			return fmt.Errorf("getting accessor: %w", err)
		}
		labels := accessor.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		for k, v := range applysetLabels {
			if _, found := labels[k]; found {
				return fmt.Errorf("ApplySet label %q already set in input data", k)
			}
			labels[k] = v
		}
		accessor.SetLabels(labels)
	}

	return nil
}

func (a *ApplySet) fetchParent() error {
	helper := resource.NewHelper(a.client, a.parentRef.RESTMapping)
	obj, err := helper.Get(a.parentRef.Namespace, a.parentRef.Name)
	if errors.IsNotFound(err) {
		if !builtinApplySetParentGVRs.Has(a.parentRef.Resource) {
			return fmt.Errorf("custom resource ApplySet parents cannot be created automatically")
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to fetch ApplySet parent object %q: %w", a.parentRef, err)
	} else if obj == nil {
		return fmt.Errorf("failed to fetch ApplySet parent object %q", a.parentRef)
	}

	labels, annotations, err := getLabelsAndAnnotations(obj)
	if err != nil {
		return fmt.Errorf("getting metadata from parent object %q: %w", a.parentRef, err)
	}

	toolAnnotation, hasToolAnno := annotations[ApplySetToolingAnnotation]
	if !hasToolAnno {
		return fmt.Errorf("ApplySet parent object %q already exists and is missing required annotation %q", a.parentRef, ApplySetToolingAnnotation)
	}
	if managedBy := toolingBaseName(toolAnnotation); managedBy != a.toolingID.Name {
		return fmt.Errorf("ApplySet parent object %q already exists and is managed by tooling %q instead of %q", a.parentRef, managedBy, a.toolingID.Name)
	}

	idLabel, hasIDLabel := labels[ApplySetParentIDLabel]
	if !hasIDLabel {
		return fmt.Errorf("ApplySet parent object %q exists and does not have required label %s", a.parentRef, ApplySetParentIDLabel)
	}
	if idLabel != a.ID() {
		return fmt.Errorf("ApplySet parent object %q exists and has incorrect value for label %q (got: %s, want: %s)", a.parentRef, ApplySetParentIDLabel, idLabel, a.ID())
	}

	if a.currentResources, err = parseKindAnnotation(annotations, a.restMapper); err != nil {
		// TODO: 处理已删除 CRD 对应的 GVR
		return fmt.Errorf("parsing ApplySet annotation on %q: %w", a.parentRef, err)
	}
	a.currentNamespaces = parseNamespacesAnnotation(annotations)
	if a.parentRef.IsNamespaced() {
		a.currentNamespaces.Insert(a.parentRef.Namespace)
	}
	return nil
}
func (a *ApplySet) LabelSelectorForMembers() string {
	return metav1.FormatLabelSelector(&metav1.LabelSelector{
		MatchLabels: a.LabelsForMember(),
	})
}

// AllPrunableResources 返回所有应被视为可清理的资源列表。
// 这可能包含实际存在资源的类型之外的超集。
func (a *ApplySet) AllPrunableResources() []*kindInfo {
	var ret []*kindInfo
	for _, m := range a.currentResources {
		ret = append(ret, m)
	}
	return ret
}

// AllPrunableNamespaces 返回所有应被视为可清理的 namespace 列表。
// 这可能包含实际存在资源的 namespace 之外的超集。
func (a *ApplySet) AllPrunableNamespaces() []string {
	var ret []string
	for ns := range a.currentNamespaces {
		ret = append(ret, ns)
	}
	return ret
}

func getLabelsAndAnnotations(obj runtime.Object) (map[string]string, map[string]string, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, nil, err
	}
	return accessor.GetLabels(), accessor.GetAnnotations(), nil
}

func toolingBaseName(toolAnnotation string) string {
	parts := strings.Split(toolAnnotation, "/")
	if len(parts) >= 2 {
		return strings.Join(parts[:len(parts)-1], "/")
	}
	return toolAnnotation
}

// kindInfo 保存特定资源类型的类型信息。
type kindInfo struct {
	restMapping *meta.RESTMapping
}

func parseKindAnnotation(annotations map[string]string, mapper meta.RESTMapper) (map[schema.GroupKind]*kindInfo, error) {
	annotation, ok := annotations[ApplySetGKsAnnotation]
	if !ok {
		if annotations[DeprecatedApplySetGRsAnnotation] != "" {
			return parseDeprecatedResourceAnnotation(annotations[DeprecatedApplySetGRsAnnotation], mapper)
		}

		// 规范并不要求此注解。但“缺失”意味着“执行发现”。
		// 由于当前 kubectl apply 不支持动态发现，我们返回错误。
		return nil, fmt.Errorf("kubectl requires the %q annotation to be set on all ApplySet parent objects", ApplySetGKsAnnotation)
	}
	mappings := make(map[schema.GroupKind]*kindInfo)
	// 注解存在但为空表示当前集合为空。
	if annotation == "" {
		return mappings, nil
	}
	for _, gkString := range strings.Split(annotation, ",") {
		gk := schema.ParseGroupKind(gkString)
		restMapping, err := mapper.RESTMapping(gk)
		if err != nil {
			return nil, fmt.Errorf("could not find mapping for kind in %q annotation: %w", ApplySetGKsAnnotation, err)
		}
		mappings[gk] = &kindInfo{
			restMapping: restMapping,
		}
	}

	return mappings, nil
}

func parseDeprecatedResourceAnnotation(annotation string, mapper meta.RESTMapper) (map[schema.GroupKind]*kindInfo, error) {
	mappings := make(map[schema.GroupKind]*kindInfo)
	// 注解存在但为空表示当前集合为空。
	if annotation == "" {
		return mappings, nil
	}
	for _, grString := range strings.Split(annotation, ",") {
		gr := schema.ParseGroupResource(grString)
		gvk, err := mapper.KindFor(gr.WithVersion(""))
		if err != nil {
			return nil, fmt.Errorf("invalid group resource in %q annotation: %w", DeprecatedApplySetGRsAnnotation, err)
		}
		restMapping, err := mapper.RESTMapping(gvk.GroupKind())
		if err != nil {
			return nil, fmt.Errorf("could not find kind for resource in %q annotation: %w", DeprecatedApplySetGRsAnnotation, err)
		}
		mappings[gvk.GroupKind()] = &kindInfo{
			restMapping: restMapping,
		}
	}
	return mappings, nil
}

func parseNamespacesAnnotation(annotations map[string]string) sets.Set[string] {
	annotation, ok := annotations[ApplySetAdditionalNamespacesAnnotation]
	if !ok { // 此注解完全是可选的
		return sets.Set[string]{}
	}
	// 不要包含空的 namespace
	if annotation == "" {
		return sets.Set[string]{}
	}
	return sets.New(strings.Split(annotation, ",")...)
}

// addResource 将给定的资源和 namespace 注册为当前操作所应用的更新集合
// 中的一部分。
func (a *ApplySet) addResource(restMapping *meta.RESTMapping, namespace string) {
	gk := restMapping.GroupVersionKind.GroupKind()
	if _, found := a.updatedResources[gk]; !found {
		a.updatedResources[gk] = &kindInfo{
			restMapping: restMapping,
		}
	}
	if restMapping.Scope == meta.RESTScopeNamespace && namespace != "" {
		a.updatedNamespaces.Insert(namespace)
	}
}

type ApplySetUpdateMode string

var updateToLatestSet ApplySetUpdateMode = "latest"
var updateToSuperset ApplySetUpdateMode = "superset"

func (a *ApplySet) updateParent(mode ApplySetUpdateMode, dryRun cmdutil.DryRunStrategy, validation string) error {
	data, err := json.Marshal(a.buildParentPatch(mode))
	if err != nil {
		return fmt.Errorf("failed to encode patch for ApplySet parent: %w", err)
	}
	// 注意，由于使用 SSA，我们会移除所有未指定的注解，
	// 这也是移除已弃用的 contains-group-resources 注解的方式。
	err = serverSideApplyRequest(a, data, dryRun, validation, false)
	if err != nil && errors.IsConflict(err) {
		// 强制解决冲突后重试
		klog.Warningf("WARNING: failed to update ApplySet: %s\nApplySet field manager %s should own these fields. Retrying with conflicts forced.", err.Error(), a.FieldManager())
		err = serverSideApplyRequest(a, data, dryRun, validation, true)
	}
	if err != nil {
		return fmt.Errorf("failed to update ApplySet: %w", err)
	}
	return nil
}

func serverSideApplyRequest(a *ApplySet, data []byte, dryRun cmdutil.DryRunStrategy, validation string, forceConficts bool) error {
	if dryRun == cmdutil.DryRunClient {
		return nil
	}
	helper := resource.NewHelper(a.client, a.parentRef.RESTMapping).
		DryRun(dryRun == cmdutil.DryRunServer).
		WithFieldManager(a.FieldManager()).
		WithFieldValidation(validation)

	options := metav1.PatchOptions{
		Force: &forceConficts,
	}
	_, err := helper.Patch(
		a.parentRef.Namespace,
		a.parentRef.Name,
		types.ApplyPatchType,
		data,
		&options,
	)
	return err
}

func (a *ApplySet) buildParentPatch(mode ApplySetUpdateMode) *metav1.PartialObjectMetadata {
	var newGKsAnnotation, newNsAnnotation string
	switch mode {
	case updateToSuperset:
		// 如果 apply 成功但清理失败，那么 ApplySet 应跟踪的
		// group resource 集合应为之前和当前资源的并集。
		// 这可以确保清理失败的资源不会从集合中变成孤立资源。
		grSuperset := sets.KeySet(a.currentResources).Union(sets.KeySet(a.updatedResources))
		newGKsAnnotation = generateKindsAnnotation(grSuperset)
		newNsAnnotation = generateNamespacesAnnotation(a.currentNamespaces.Union(a.updatedNamespaces), a.parentRef.Namespace)
	case updateToLatestSet:
		newGKsAnnotation = generateKindsAnnotation(sets.KeySet(a.updatedResources))
		newNsAnnotation = generateNamespacesAnnotation(a.updatedNamespaces, a.parentRef.Namespace)
	}

	return &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{
			Kind:       a.parentRef.GroupVersionKind.Kind,
			APIVersion: a.parentRef.GroupVersionKind.GroupVersion().String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      a.parentRef.Name,
			Namespace: a.parentRef.Namespace,
			Annotations: map[string]string{
				ApplySetToolingAnnotation:              a.toolingID.String(),
				ApplySetGKsAnnotation:                  newGKsAnnotation,
				ApplySetAdditionalNamespacesAnnotation: newNsAnnotation,
			},
			Labels: map[string]string{
				ApplySetParentIDLabel: a.ID(),
			},
		},
	}
}

func generateNamespacesAnnotation(namespaces sets.Set[string], skip string) string {
	nsList := namespaces.Clone().Delete(skip).UnsortedList()
	sort.Strings(nsList)
	return strings.Join(nsList, ",")
}

func generateKindsAnnotation(resources sets.Set[schema.GroupKind]) string {
	var gks []string
	for gk := range resources {
		gks = append(gks, gk.String())
	}
	sort.Strings(gks)
	return strings.Join(gks, ",")
}

func (a ApplySet) FieldManager() string {
	return fmt.Sprintf("%s-applyset", a.toolingID.Name)
}

// ParseApplySetParentRef 根据 [RESOURCE][.GROUP]/NAME 格式的 parent 引用创建新的 ApplySetParentRef
func ParseApplySetParentRef(parentRefStr string, mapper meta.RESTMapper) (*ApplySetParentRef, error) {
	var gvr schema.GroupVersionResource
	var name string

	if groupRes, nameSuffix, hasTypeInfo := strings.Cut(parentRefStr, "/"); hasTypeInfo {
		name = nameSuffix
		gvr = schema.ParseGroupResource(groupRes).WithVersion("")
	} else {
		name = parentRefStr
		gvr = defaultApplySetParentGVR
	}

	if name == "" {
		return nil, fmt.Errorf("name cannot be blank")
	}

	gvk, err := mapper.KindFor(gvr)
	if err != nil {
		return nil, err
	}
	mapping, err := mapper.RESTMapping(gvk.GroupKind())
	if err != nil {
		return nil, err
	}
	return &ApplySetParentRef{Name: name, RESTMapping: mapping}, nil
}

// Prune 从 apiserver 删除所有不再属于该 applyset 的对象。
func (a *ApplySet) Prune(ctx context.Context, o *ApplyOptions) error {
	printer, err := o.ToPrinter("pruned")
	if err != nil {
		return err
	}
	opt := &ApplySetDeleteOptions{
		CascadingStrategy: o.DeleteOptions.CascadingStrategy,
		DryRunStrategy:    o.DryRunStrategy,
		GracePeriod:       o.DeleteOptions.GracePeriod,

		Printer: printer,

		IOStreams: o.IOStreams,
	}

	if err := a.pruneAll(ctx, o.DynamicClient, o.VisitedUids, opt); err != nil {
		return err
	}

	if err := a.updateParent(updateToLatestSet, o.DryRunStrategy, o.ValidationDirective); err != nil {
		return fmt.Errorf("apply and prune succeeded, but ApplySet update failed: %w", err)
	}

	return nil
}

// BeforeApply 应在应用对象之前调用。
// 它会预先更新 parent 对象，使其覆盖即将被应用的资源。
// 这样即使操作被中断，也不会泄漏对象。
func (a *ApplySet) BeforeApply(objects []*resource.Info, dryRunStrategy cmdutil.DryRunStrategy, validationDirective string) error {
	if err := a.fetchParent(); err != nil {
		return err
	}
	// 将 live parent 对象更新为当前资源和之前资源的并集。
	// 在实际执行 apply 和 prune 之前这样做可以改善行为：确保
	// 失败时 live 对象仍包含并集。这可能导致下一次清理
	// 操作发出比严格必要数量更多的 GET 请求，但可以防止
	// 对象从集合中泄漏。下一次成功操作会自动将并集缩减为正确的
	// 集合。
	for _, info := range objects {
		a.addResource(info.ResourceMapping(), info.Namespace)
	}
	if err := a.updateParent(updateToSuperset, dryRunStrategy, validationDirective); err != nil {
		return err
	}
	return nil
}
