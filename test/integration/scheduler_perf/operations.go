/*
Copyright 2019 The Kubernetes Authors.

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

package benchmark

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"text/template"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

// createAny 定义一个操作：从 YAML 文件创建某个对象。
// 可以指定 Namespace。
// 支持 Go 模板，并提供了一些额外的方法（参见 getTemplateFuncs）
// 以及参数：
// .Index：计数的起始索引
// .Count：要创建的对象数量
// .<template param>：该操作的 templateParams 中的一个成员
type createAny struct {
	// 必须与 createAnyOpcode 一致。
	Opcode operationCode
	// 对象所在的 Namespace。对于集群作用域的对象，必须为空。
	Namespace string
	// 描述待创建对象的 spec 文件路径。
	// 该文件将由 text/template 处理。
	// 创建多个对象时，.Index 的取值范围为 [0, Count-1]，
	// .Count 为对象总数。
	TemplatePath string
	// Count 决定创建多少个对象。未设置时默认为 1。
	Count      *int
	CountParam string
	// 传递给模板的参数。
	// 以 `$` 开头的值将解析为工作负载参数。
	TemplateParams map[string]any
}

var _ runnableOp = &createAny{}

func (c *createAny) isValid(allowParameterization bool) error {
	if c.TemplatePath == "" {
		return fmt.Errorf("TemplatePath must be set")
	}
	// Namespace 只能稍后检查，因为目前还不知道
	// 该对象是命名空间作用域还是集群作用域。
	return nil
}

func (c *createAny) collectsMetrics() bool {
	return false
}

func (c createAny) patchParams(w *Workload) (realOp, error) {
	if c.CountParam != "" {
		count, err := w.Params.get(c.CountParam[1:])
		if err != nil {
			return nil, err
		}
		c.Count = ptr.To(count)
	}
	if len(c.TemplateParams) > 0 {
		var err error
		c.TemplateParams, err = resolveTemplateParams(c.TemplateParams, w)
		if err != nil {
			return nil, err
		}
	}
	return &c, c.isValid(false)
}

func (c *createAny) requiredNamespaces() []string {
	if c.Namespace == "" {
		return nil
	}
	return []string{c.Namespace}
}

func (c *createAny) run(tCtx ktesting.TContext) {
	count := 1
	if c.Count != nil {
		count = *c.Count
	}
	var items []string
	for index := 0; index < count; index++ {
		env := make(map[string]any)
		maps.Copy(env, c.TemplateParams)
		env["Index"] = index
		env["Count"] = count
		item := c.create(tCtx, env)
		// 仅保留第一个和最后一个对象用于报告。
		if index == 0 {
			items = []string{strings.TrimSpace(item)}
			continue
		}
		if index == count-1 {
			if index > 1 {
				// 我们跳过了一些中间对象。
				items = append(items, "\n\n...\n\n")
			}
			items = append(items, strings.TrimSpace(item))
		}
	}

	params := ""
	if len(c.TemplateParams) > 0 {
		params = fmt.Sprintf(" with params %v", c.TemplateParams)
	}
	namespace := " in cluster scope"
	if c.Namespace != "" {
		namespace = fmt.Sprintf(" in namespace %q", c.Namespace)
	}
	tCtx.Logf("Created %d item(s) from %s%s%s:\n%s", count, c.TemplatePath, params, namespace, strings.Join(items, ""))
}

func (c *createAny) create(tCtx ktesting.TContext, env map[string]any) string {
	var obj *unstructured.Unstructured
	objData, err := getSpecFromTextTemplateFile(c.TemplatePath, env, &obj)
	if err != nil {
		tCtx.Fatalf("%s: parsing failed: %v\n%s", c.TemplatePath, err, objData)
	}

	// 不缓存 discovery 结果效率不高，但在 createAny 不频繁执行时
	// 已经足够用了。
	mapping, err := restMappingFromUnstructuredObj(tCtx, obj)
	if err != nil {
		tCtx.Fatalf("%s: %v", c.TemplatePath, err)
	}
	resourceClient := tCtx.Dynamic().Resource(mapping.Resource)

	create := func() error {
		options := metav1.CreateOptions{
			// 如果输入的 YAML 无效，我们希望
			// apiserver 通过报错告知我们。之所以会这样，
			// 是因为解码为 unstructured 对象时
			// 不会做校验。
			FieldValidation: "Strict",
		}
		if c.Namespace != "" {
			if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
				return fmt.Errorf("namespace %q set for %q, but %q has scope %q", c.Namespace, c.TemplatePath, mapping.GroupVersionKind, mapping.Scope.Name())
			}
			_, err = resourceClient.Namespace(c.Namespace).Create(tCtx, obj, options)
		} else {
			if mapping.Scope.Name() != meta.RESTScopeNameRoot {
				return fmt.Errorf("namespace not set for %q, but %q has scope %q", c.TemplatePath, mapping.GroupVersionKind, mapping.Scope.Name())
			}
			_, err = resourceClient.Create(tCtx, obj, options)
		}
		return err
	}
	// 需要重试：有些错误（例如刚创建 CRD、类型尚未就绪）是暂时的。
	ctx, cancel := context.WithTimeout(tCtx, 20*time.Second)
	defer cancel()
	for {
		err := create()
		if err == nil {
			return objData
		}
		select {
		case <-ctx.Done():
			tCtx.Fatalf("%s: timed out (%q) while creating %q, last error was: %v\n\n%s", c.TemplatePath, context.Cause(ctx), klog.KObj(obj), err, objData)
		case <-time.After(time.Second):
		}
	}
}

func getSpecFromTextTemplateFile(path string, env map[string]any, spec interface{}) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	tmpl, err := template.New("object").Funcs(getTemplateFuncs()).Parse(string(content))
	if err != nil {
		return "", err
	}
	var buffer bytes.Buffer
	if err := tmpl.Execute(&buffer, env); err != nil {
		return "", err
	}

	return buffer.String(), yaml.UnmarshalStrict(buffer.Bytes(), spec)
}

func restMappingFromUnstructuredObj(tCtx ktesting.TContext, obj *unstructured.Unstructured) (*meta.RESTMapping, error) {
	discoveryCache := memory.NewMemCacheClient(tCtx.Client().Discovery())
	restMapper := restmapper.NewDeferredDiscoveryRESTMapper(discoveryCache)
	gv, err := schema.ParseGroupVersion(obj.GetAPIVersion())
	if err != nil {
		return nil, fmt.Errorf("extract group+version from object %q: %w", klog.KObj(obj), err)
	}
	gk := schema.GroupKind{Group: gv.Group, Kind: obj.GetKind()}

	mapping, err := restMapper.RESTMapping(gk, gv.Version)
	if err != nil {
		// 缓存的映射可能已过期，下次尝试时刷新。
		restMapper.Reset()
		return nil, fmt.Errorf("failed mapping %q to resource: %w", gk, err)
	}
	return mapping, nil
}

// createNodesOp 定义一个操作：作为工作负载的一部分创建 Node。
type createNodesOp struct {
	// 必须是 "createNodes"。
	Opcode operationCode
	// 要创建的 Node 数量。可通过 CountParam 参数化。
	Count int
	// Count 的模板参数。
	CountParam string
	// 描述待创建 Node 的 spec 文件路径。
	// 可选
	NodeTemplatePath *string
	// 以下策略最多只能定义一种。未指定时
	// 默认为 TrivialNodePrepareStrategy。
	// 可选
	NodeAllocatableStrategy  *NodeAllocatableStrategy
	LabelNodePrepareStrategy *LabelNodePrepareStrategy
	UniqueNodeLabelStrategy  *UniqueNodeLabelStrategy
	// 传递给模板的参数。
	// 以 `$` 开头的值将解析为工作负载参数。
	TemplateParams map[string]any
}

func (cno *createNodesOp) isValid(allowParameterization bool) error {
	if !isValidCount(allowParameterization, cno.Count, cno.CountParam) {
		return fmt.Errorf("invalid Count=%d / CountParam=%q", cno.Count, cno.CountParam)
	}
	return nil
}

func (*createNodesOp) collectsMetrics() bool {
	return false
}

func (cno createNodesOp) patchParams(w *Workload) (realOp, error) {
	var err error
	if cno.CountParam != "" {
		cno.Count, err = w.Params.get(cno.CountParam[1:])
		if err != nil {
			return nil, err
		}
	}
	if len(cno.TemplateParams) > 0 {
		cno.TemplateParams, err = resolveTemplateParams(cno.TemplateParams, w)
		if err != nil {
			return nil, err
		}
	}
	return &cno, (&cno).isValid(false)
}

// createNamespacesOp 定义用于创建 Namespace 的操作
type createNamespacesOp struct {
	// 必须是 "createNamespaces"。
	Opcode operationCode
	// Namespace 的名称前缀。格式为 "<prefix>-<number>"，其中 number
	// 的取值范围为 0 到 count-1。
	Prefix string
	// 要创建的 Namespace 数量。可通过 CountParam 参数化。
	Count int
	// Count 的模板参数。若与 Count 同时设置，以本项为准。
	CountParam string
	// 描述待创建 Namespace 的 spec 文件路径。
	// 可选
	NamespaceTemplatePath *string
}

func (cmo *createNamespacesOp) isValid(allowParameterization bool) error {
	if !isValidCount(allowParameterization, cmo.Count, cmo.CountParam) {
		return fmt.Errorf("invalid Count=%d / CountParam=%q", cmo.Count, cmo.CountParam)
	}
	return nil
}

func (*createNamespacesOp) collectsMetrics() bool {
	return false
}

func (cmo createNamespacesOp) patchParams(w *Workload) (realOp, error) {
	if cmo.CountParam != "" {
		var err error
		cmo.Count, err = w.Params.get(cmo.CountParam[1:])
		if err != nil {
			return nil, err
		}
	}
	return &cmo, (&cmo).isValid(false)
}

// createPodsOp 定义一个操作：作为工作负载的一部分调度 Pod。
// 测试可以等待该操作完成后再继续，
// 也可以异步继续执行。
type createPodsOp struct {
	// 必须是 "createPods"。
	Opcode operationCode
	// 要调度的 Pod 数量。可通过 CountParam 参数化。
	Count int
	// Count 的模板参数。
	CountParam string
	// 用于乘以 CountParam 的模板参数。当 Pod 总数
	// 由多个 podgroup 中每个 podgroup 的 Pod 数量决定时使用。
	// 可选。
	CountMultiplierParam string
	// 为 false 时，会快速创建 Count 个 Pod。这可用于
	// 测量调度器填满集群的速度。
	//
	// 为 true 时，会创建 Count 个 Pod，操作会等待
	// 一个 Pod 完成调度，将其删除，然后再创建下一个。
	// 如此持续，直到配置的 Duration 结束。
	// 如果启用了指标收集，它将并行运行。
	//
	// 该模式可用于测量调度器在稳态下的表现，
	// 此时集群的利用率大致保持在
	// 同一水平。可以在更早的独立操作中创建 Pod，
	// 以模拟非空集群。
	//
	// 注意，该操作会删除 Namespace 中任何已调度的 Pod，
	// 因此对于需要保持运行的 Pod，请使用不同的
	// Namespace。
	SteadyState bool
	// SteadyState 的模板参数。
	SteadyStateParam string
	// 保持集群处于稳态的时长。
	Duration metav1.Duration
	// Duration 的模板参数。
	DurationParam string
	// 是否为该 createPodsOp 启用指标收集。
	// 可选。对于同一个 createPodsOp，CollectMetrics 和 SkipWaitToCompletion
	// 不能同时为 true。
	CollectMetrics bool
	// Pod 所在的 Namespace。默认为形如
	// "namespace-<number>" 的唯一 Namespace。
	// 可选
	Namespace *string
	// 描述待调度 Pod 的 spec 文件路径。
	// 若为 nil，则使用 DefaultPodTemplatePath。
	// 可选
	PodTemplatePath *string
	// 是否等待该操作中的所有 Pod 完成调度。
	// 未指定时默认为 false。
	// 可选
	SkipWaitToCompletion bool
	// 待调度 Pod 的持久化卷设置。
	// 可选
	PersistentVolumeTemplatePath      *string
	PersistentVolumeClaimTemplatePath *string
	// 传递给模板的参数。
	// 以 `$` 开头的值将解析为工作负载参数。
	TemplateParams map[string]any
	// SignatureBatchSize 定义连续多少个 Pod 使用相同的 "signature" 标签。
	// 若为正数，则每 SignatureBatchSize 个 Pod 会带有一个值为 "signature-label-<index/batchSize>" 的 "signature" 标签。
	// 未指定时默认为 1（每个 Pod 都有唯一的签名）。
	// 可选
	SignatureBatchSize int
	// SignatureBatchSize 的模板参数。
	SignatureBatchSizeParam string
}

func (cpo *createPodsOp) isValid(allowParameterization bool) error {
	if !isValidCount(allowParameterization, cpo.SignatureBatchSize, cpo.SignatureBatchSizeParam) {
		return fmt.Errorf("invalid SignatureBatchSize=%d / SignatureBatchSizeParam=%q", cpo.SignatureBatchSize, cpo.SignatureBatchSizeParam)
	}
	if !isValidCount(allowParameterization, cpo.Count, cpo.CountParam) {
		return fmt.Errorf("invalid Count=%d / CountParam=%q", cpo.Count, cpo.CountParam)
	}
	if cpo.CountMultiplierParam != "" && !isValidParameterizable(cpo.CountMultiplierParam) {
		return fmt.Errorf("invalid CountMultiplierParam=%q", cpo.CountMultiplierParam)
	}
	if cpo.CollectMetrics && cpo.SkipWaitToCompletion {
		// 虽然技术上可以做到这一点，但额外的
		// 复杂性并不值得，尤其是考虑到目前我们没有任何
		// 相关用例。
		return fmt.Errorf("collectMetrics and skipWaitToCompletion cannot be true at the same time")
	}
	if cpo.SkipWaitToCompletion && cpo.SteadyState {
		return errors.New("skipWaitToCompletion and steadyState cannot be true at the same time")
	}
	if cpo.SteadyState && !allowParameterization && cpo.Duration.Duration <= 0 {
		return errors.New("when creating pods in a steady state, the test duration must be > 0")
	}
	return nil
}

func (cpo *createPodsOp) collectsMetrics() bool {
	return cpo.CollectMetrics
}

func (cpo createPodsOp) patchParams(w *Workload) (realOp, error) {
	var err error
	if cpo.CountParam != "" {
		cpo.Count, err = w.Params.get(cpo.CountParam[1:])
		if err != nil {
			return nil, err
		}
	}
	if cpo.CountMultiplierParam != "" {
		multiplier, err := w.Params.get(cpo.CountMultiplierParam[1:])
		if err != nil {
			return nil, err
		}
		cpo.Count *= multiplier
	}
	if cpo.SignatureBatchSizeParam != "" {
		paramKey := cpo.SignatureBatchSizeParam[1:]
		signatureBatchSize, err := w.Params.get(paramKey)
		if err != nil {
			return nil, err
		}
		cpo.SignatureBatchSize = signatureBatchSize
	}
	if cpo.SignatureBatchSize == 0 {
		cpo.SignatureBatchSize = 1
	}
	if cpo.DurationParam != "" {
		durationStr, err := getParam[string](w.Params, cpo.DurationParam[1:])
		if err != nil {
			return nil, err
		}
		if cpo.Duration.Duration, err = time.ParseDuration(durationStr); err != nil {
			return nil, fmt.Errorf("parsing duration parameter %s: %w", cpo.DurationParam, err)
		}
	}
	if cpo.SteadyStateParam != "" {
		cpo.SteadyState, err = getParam[bool](w.Params, cpo.SteadyStateParam[1:])
		if err != nil {
			return nil, err
		}
	}
	if len(cpo.TemplateParams) > 0 {
		cpo.TemplateParams, err = resolveTemplateParams(cpo.TemplateParams, w)
		if err != nil {
			return nil, err
		}
	}
	return &cpo, (&cpo).isValid(false)
}

// createPodSetsOp 定义一个操作：在每个不同的 Namespace 中创建一组 createPodsOp。
type createPodSetsOp struct {
	// 必须是 "createPodSets"。
	Opcode operationCode
	// 要创建的集合数量。
	Count int
	// Count 的模板参数。
	CountParam string
	// 每组 Pod 将创建在形如 namespacePrefix-<number> 的 Namespace 中，
	// 其中 number 的取值范围为 0 到 count-1
	NamespacePrefix string
	// createPodsOp 的模板。
	CreatePodsOp createPodsOp
}

func (cpso *createPodSetsOp) isValid(allowParameterization bool) error {
	if !isValidCount(allowParameterization, cpso.Count, cpso.CountParam) {
		return fmt.Errorf("invalid Count=%d / CountParam=%q", cpso.Count, cpso.CountParam)
	}
	return cpso.CreatePodsOp.isValid(allowParameterization)
}

func (cpso *createPodSetsOp) collectsMetrics() bool {
	return cpso.CreatePodsOp.CollectMetrics
}

func (cpso createPodSetsOp) patchParams(w *Workload) (realOp, error) {
	if cpso.CountParam != "" {
		var err error
		cpso.Count, err = w.Params.get(cpso.CountParam[1:])
		if err != nil {
			return nil, err
		}
	}
	return &cpso, (&cpso).isValid(true)
}

// deletePodsOp 定义一个操作：删除之前创建的 Pod。
// 测试可以等待该操作完成后再继续，
// 也可以异步继续执行。
type deletePodsOp struct {
	// 必须是 "deletePods"。
	Opcode operationCode
	// 要从中删除 Pod 的 Namespace。
	Namespace string
	// 用于筛选待删除 Pod 的标签。
	// 若为空，将删除该 Namespace 中的所有 Pod。
	// 可选。
	LabelSelector map[string]string
	// 是否等待该操作中的所有 Pod 被删除。
	// 未指定时默认为 false。
	// 可选
	SkipWaitToCompletion bool
	// 每秒删除的 Pod 数量。
	// 若为 0，则一次性删除所有 Pod。
	// 可选
	DeletePodsPerSecond int
}

func (dpo *deletePodsOp) isValid(allowParameterization bool) error {
	if dpo.Opcode != deletePodsOpcode {
		return fmt.Errorf("invalid opcode %q; expected %q", dpo.Opcode, deletePodsOpcode)
	}
	if dpo.DeletePodsPerSecond < 0 {
		return fmt.Errorf("invalid DeletePodsPerSecond=%d; should be non-negative", dpo.DeletePodsPerSecond)
	}
	return nil
}

func (dpo *deletePodsOp) collectsMetrics() bool {
	return false
}

func (dpo deletePodsOp) patchParams(w *Workload) (realOp, error) {
	return &dpo, nil
}

// churnOp 定义一个操作：作为工作负载的一部分创建服务。
type churnOp struct {
	// 必须是 "churnOp"。
	Opcode operationCode
	// 取值必须是以下之一：
	// - recreate。在此模式下，API 对象会创建 N 个周期，然后
	//   在接下来的 N 个周期中删除。N 由 "Number" 字段指定。
	// - create。在此模式下，API 对象会持续创建（不删除），直到
	//   达到由 "Number" 字段指定的阈值。
	Mode string
	// 要创建的 API 对象数量上限。
	// 默认为 0，表示不限制。
	Number int
	// churn 的时间间隔。默认为 500 毫秒。
	IntervalMilliseconds int64
	// churn 对象所在的 Namespace。默认为形如
	// "namespace-<number>" 的唯一 Namespace。
	// 可选
	Namespace *string
	// API spec 文件的路径。
	TemplatePaths []string
}

func (co *churnOp) isValid(_ bool) error {
	if co.Mode != Recreate && co.Mode != Create {
		return fmt.Errorf("invalid mode: %v. must be one of %v", co.Mode, []string{Recreate, Create})
	}
	if co.Number < 0 {
		return fmt.Errorf("number (%v) cannot be negative", co.Number)
	}
	if co.Mode == Recreate && co.Number == 0 {
		return fmt.Errorf("number cannot be 0 when mode is %v", Recreate)
	}
	if len(co.TemplatePaths) == 0 {
		return fmt.Errorf("at least one template spec file needs to be specified")
	}
	return nil
}

func (*churnOp) collectsMetrics() bool {
	return false
}

func (co churnOp) patchParams(w *Workload) (realOp, error) {
	return &co, nil
}

type SchedulingStage string

const (
	Scheduled SchedulingStage = "Scheduled"
	Attempted SchedulingStage = "Attempted"
)

// barrierOp 定义一个操作，可用于等待一个或多个 Namespace 中所有已调度的 Pod
// 都绑定到 Node。当 Pod 是在
// SkipWaitToCompletion 设为 true 的情况下调度时，这个操作很有用。
type barrierOp struct {
	// 必须是 "barrier"。
	Opcode operationCode
	// 要等待的 Namespace。空数组或未指定该字段表示
	// barrier 应等待所有 Namespace。
	Namespaces []string
	// 用于筛选要等待的 Pod 的标签。
	// 若为空，则不按标签筛选。
	// 可选。
	LabelSelector map[string]string
	// 决定 barrier 应等待 Pod 调度的哪个阶段。
	// 若为空，则视为 "Scheduled"。
	// 可选
	StageRequirement SchedulingStage
}

func (bo *barrierOp) isValid(allowParameterization bool) error {
	if bo.StageRequirement != "" && bo.StageRequirement != Scheduled && bo.StageRequirement != Attempted {
		return fmt.Errorf("invalid StageRequirement %s", bo.StageRequirement)
	}
	return nil
}

func (*barrierOp) collectsMetrics() bool {
	return false
}

func (bo barrierOp) patchParams(w *Workload) (realOp, error) {
	if bo.StageRequirement == "" {
		bo.StageRequirement = Scheduled
	}
	return &bo, nil
}

// sleepOp 定义一个操作，可用于睡眠指定的时长。
// 这在模拟需要某种基于时间同步的工作负载时很有用。
type sleepOp struct {
	// 必须是 "sleep"。
	Opcode operationCode
	// 睡眠时长。
	Duration metav1.Duration
	// Duration 的模板参数。
	DurationParam string
}

func (so *sleepOp) isValid(_ bool) error {
	return nil
}

func (so *sleepOp) collectsMetrics() bool {
	return false
}

func (so sleepOp) patchParams(w *Workload) (realOp, error) {
	if so.DurationParam != "" {
		durationStr, err := getParam[string](w.Params, so.DurationParam[1:])
		if err != nil {
			return nil, err
		}
		if so.Duration.Duration, err = time.ParseDuration(durationStr); err != nil {
			return nil, fmt.Errorf("invalid duration parameter %s: %w", so.DurationParam, err)
		}
	}
	return &so, nil
}

// createPodGroups 定义一个操作：从 YAML 模板创建 PodGroup，
// 然后等待它们在调度器的 informer 缓存中可见。
type createPodGroups struct {
	// 必须与 createPodGroupsOpcode 一致。
	Opcode operationCode
	// 对象所在的 Namespace。
	Namespace string
	// 描述待创建 PodGroup 的 spec 文件路径。
	TemplatePath string
	// 传递给模板的参数。
	TemplateParams map[string]any
	// Count 决定创建多少个 PodGroup。
	Count int
	// CountParam 是决定该数量的参数名。
	CountParam string
}

func (cpg *createPodGroups) isValid(allowParameterization bool) error {
	if cpg.TemplatePath == "" {
		return fmt.Errorf("templatePath must be set")
	}
	if cpg.Namespace == "" {
		return fmt.Errorf("namespace must be set")
	}
	if !isValidCount(allowParameterization, cpg.Count, cpg.CountParam) {
		return fmt.Errorf("invalid Count=%d / CountParam=%q", cpg.Count, cpg.CountParam)
	}
	if !allowParameterization && cpg.Count < 1 {
		return fmt.Errorf("count must be greater than 0, got %d", cpg.Count)
	}
	return nil
}

func (cpg *createPodGroups) collectsMetrics() bool {
	return false
}

func (cpg createPodGroups) patchParams(w *Workload) (realOp, error) {
	if cpg.CountParam != "" {
		count, err := w.Params.get(cpg.CountParam[1:])
		if err != nil {
			return nil, err
		}
		cpg.Count = count
	}
	if len(cpg.TemplateParams) > 0 {
		var err error
		cpg.TemplateParams, err = resolveTemplateParams(cpg.TemplateParams, w)
		if err != nil {
			return nil, err
		}
	}
	return &cpg, cpg.isValid(false)
}

// startCollectingMetricsOp 定义一个启动指标收集器的操作。
// 必须在该操作之后使用 stopCollectingMetricsOp 来完成收集。
type startCollectingMetricsOp struct {
	// 必须是 "startCollectingMetrics"。
	Opcode operationCode
	// 在结果中追加到工作负载名称后面的名称。
	Name string
	// 计算调度吞吐量指标所针对的 Namespace。
	Namespaces []string
	// 用于筛选要收集调度吞吐量指标的 Pod 的标签。
	// 若为空，则收集所选 Namespace 中所有 Pod 的指标。
	// 可选。
	LabelSelector map[string]string
}

func (scm *startCollectingMetricsOp) isValid(_ bool) error {
	if len(scm.Namespaces) == 0 {
		return fmt.Errorf("namespaces cannot be empty")
	}
	return nil
}

func (*startCollectingMetricsOp) collectsMetrics() bool {
	return false
}

func (scm startCollectingMetricsOp) patchParams(_ *Workload) (realOp, error) {
	return &scm, nil
}

// stopCollectingMetricsOp 定义一个操作：停止收集指标
// 并将其写入结果切片。
// 必须在该操作之前使用 startCollectingMetricsOp 开始收集。
type stopCollectingMetricsOp struct {
	// 必须是 "stopCollectingMetrics"。
	Opcode operationCode
}

func (scm *stopCollectingMetricsOp) isValid(_ bool) error {
	return nil
}

func (*stopCollectingMetricsOp) collectsMetrics() bool {
	return true
}

func (scm stopCollectingMetricsOp) patchParams(_ *Workload) (realOp, error) {
	return &scm, nil
}

// startCollectingProfileOp 定义一个开始收集性能剖析数据的操作。
// 必须在该操作之后使用 stopCollectingProfileOp 来完成收集。
type startCollectingProfileOp struct {
	// 必须是 "startCollectingProfile"。
	Opcode operationCode
	// Type 是要收集的 profile 类型（目前仅支持 "CPU"）。
	Type string
	// FilePath 是输出 profile 文件的路径。如果设置了 dataItemsDir，
	// 该文件将相对于 dataItemsDir 创建。
	FilePath string
}

func (scp *startCollectingProfileOp) isValid(_ bool) error {
	if scp.FilePath == "" {
		return fmt.Errorf("filePath cannot be empty")
	}
	if strings.ToUpper(scp.Type) != "CPU" {
		return fmt.Errorf("only CPU profile type is supported, got %q", scp.Type)
	}
	return nil
}

func (*startCollectingProfileOp) collectsMetrics() bool {
	return false
}

func (scp startCollectingProfileOp) patchParams(_ *Workload) (realOp, error) {
	return &scp, nil
}

// stopCollectingProfileOp 定义一个停止收集性能剖析数据的操作。
type stopCollectingProfileOp struct {
	// 必须是 "stopCollectingProfile"。
	Opcode operationCode
	// Type 是要停止的 profile 类型（目前仅支持 "CPU"）。
	Type string
}

func (scp *stopCollectingProfileOp) isValid(_ bool) error {
	if strings.ToUpper(scp.Type) != "CPU" {
		return fmt.Errorf("only CPU profile type is supported, got %q", scp.Type)
	}
	return nil
}

func (*stopCollectingProfileOp) collectsMetrics() bool {
	return false
}

func (scp stopCollectingProfileOp) patchParams(_ *Workload) (realOp, error) {
	return &scp, nil
}

// resolveTemplateParams 使用工作负载参数解析模板参数。
// 以 $ 开头的值是对工作负载参数的引用。
// 可选的 | 后面的字符串是默认值，采用 YAML 编码
// （特别是，不带引号的纯字符串也是有效的）。
func resolveTemplateParams(templateParams map[string]any, w *Workload) (map[string]any, error) {
	if len(templateParams) == 0 {
		return templateParams, nil
	}
	resolved := maps.Clone(templateParams)
	for k, v := range resolved {
		if s, ok := v.(string); ok && strings.HasPrefix(s, "$") {
			paramKey := s[1:]
			var def *string
			sep := strings.Index(paramKey, "|")
			if sep > 0 {
				def = new(paramKey[sep+1:])
				paramKey = paramKey[:sep]
			}
			if val, found := w.Params.params[paramKey]; found {
				w.Params.isUsed[paramKey] = true
				resolved[k] = val
				continue
			}
			if def != nil {
				// 必须先将其转换为实际的值，
				// 否则只有字符串类型的值才能有默认值。
				var val any
				if err := yaml.Unmarshal([]byte(*def), &val); err != nil {
					return nil, fmt.Errorf("decoding parameter %q default %q as YAML: %w", paramKey, *def, err)
				}
				resolved[k] = val
				continue
			}
			return nil, fmt.Errorf("parameter %q not found", paramKey)
		}
	}
	return resolved, nil
}

func getTemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"AddFloat":      addFloat,
		"AddInt":        addInt,
		"DivideFloat":   divideFloat,
		"DivideInt":     divideInt,
		"Int":           toInt,
		"JSON":          toJSON,
		"Mod":           mod,
		"MultiplyFloat": multiplyFloat,
		"MultiplyInt":   multiplyInt,
		"SubtractFloat": subtractFloat,
		"SubtractInt":   subtractInt,
	}
}

func toFloat64(val any) float64 {
	switch i := val.(type) {
	case float64:
		return i
	case float32:
		return float64(i)
	case int64:
		return float64(i)
	case int32:
		return float64(i)
	case int:
		return float64(i)
	case uint64:
		return float64(i)
	case uint32:
		return float64(i)
	case uint:
		return float64(i)
	case string:
		f, err := strconv.ParseFloat(i, 64)
		if err == nil {
			return f
		}
	}
	panic(fmt.Sprintf("cannot cast %v to float64", val))
}

func toInt(val any) int {
	switch i := val.(type) {
	case float64:
		return int(i)
	case float32:
		return int(i)
	case int64:
		return int(i)
	case int32:
		return int(i)
	case int:
		return i
	case uint64:
		return int(i)
	case uint32:
		return int(i)
	case uint:
		return int(i)
	case string:
		v, err := strconv.Atoi(i)
		if err == nil {
			return v
		}
	}
	panic(fmt.Sprintf("cannot cast %v to int", val))
}

func toJSON(val any) string {
	var buffer strings.Builder
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(val); err != nil {
		panic(fmt.Sprintf("cannot JSON-encode %v: %v", val, err))
	}
	return strings.TrimSpace(buffer.String())
}

func addInt(numbers ...any) int {
	return int(addFloat(numbers...))
}

func subtractInt(i, j any) int {
	return int(subtractFloat(i, j))
}

func multiplyInt(numbers ...any) int {
	return int(multiplyFloat(numbers...))
}

func divideInt(i, j any) int {
	return int(divideFloat(i, j))
}

func addFloat(numbers ...any) float64 {
	sum := 0.0
	for _, number := range numbers {
		sum += toFloat64(number)
	}
	return sum
}

func subtractFloat(i, j any) float64 {
	typedI := toFloat64(i)
	typedJ := toFloat64(j)
	return typedI - typedJ
}

func multiplyFloat(numbers ...any) float64 {
	product := 1.0
	for _, number := range numbers {
		product *= toFloat64(number)
	}
	return product
}

func divideFloat(i, j any) float64 {
	typedI := toFloat64(i)
	typedJ := toFloat64(j)
	if typedJ == 0 {
		panic("division by zero")
	}
	return typedI / typedJ
}

func mod(a, b any) int {
	return int(toFloat64(a)) % int(toFloat64(b))
}
