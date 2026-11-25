/*
Copyright The Kubernetes Authors.

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

// 本包提供从清单文件加载策略配置的 Source 实现。
//
// 该包把磁盘上的清单文件视为唯一事实来源，通过轮询目录 mtime/内容哈希实现热加载；
// 加载与编译失败时保留上一份可用的 hooks，避免把配置错误放大成请求失败。
package source

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission/plugin/manifest/metrics"
	"k8s.io/apiserver/pkg/admission/plugin/policy/generic"
	"k8s.io/apiserver/pkg/util/filesystem"
	"k8s.io/klog/v2"
)

// defaultReloadInterval 是检查清单目录是否发生变化的默认轮询间隔。
// 1 分钟在“配置生效延迟”和“目录扫描开销”之间取折中；测试可临时覆盖。
var defaultReloadInterval = 1 * time.Minute

// SetReloadIntervalForTests 设置测试用的重载间隔，并返回一个恢复原值的函数。
// 返回恢复函数而不是依赖 defer 内的全局状态，避免测试间相互污染。
func SetReloadIntervalForTests(interval time.Duration) func() {
	original := defaultReloadInterval
	defaultReloadInterval = interval
	return func() {
		defaultReloadInterval = original
	}
}

// PolicyLoadFunc 从目录中加载策略与绑定清单。
// 返回加载到的策略、绑定以及内容哈希；哈希用于在内容未变时跳过重新编译。
type PolicyLoadFunc[P, B runtime.Object] func(dir string) ([]P, []B, string, error)

// PolicyCompiler 把策略编译为 Evaluator。
// 编译可能失败（例如 CEL 表达式非法），此时整个清单集视为无效。
type PolicyCompiler[P runtime.Object, E generic.Evaluator] func(P) (E, error)

// BindingPolicyName 提取绑定所引用的策略名。
// 绑定通过策略名与策略关联，而非通过对象引用，以保持清单文件可独立编辑。
type BindingPolicyName[B runtime.Object] func(B) string

// StaticPolicySource 提供从清单文件加载的策略配置。
//
// 所有可变状态都通过 atomic 发布：读请求只 Load 不可变快照，
// 后台 reload 构造新切片后整体替换指针，因此 Hooks 无需加锁即可并发调用。
type StaticPolicySource[P, B runtime.Object, E generic.Evaluator] struct {
	manifestsDir         string
	apiServerID          string
	reloadInterval       time.Duration
	compiler             PolicyCompiler[P, E]
	loadFunc             PolicyLoadFunc[P, B]
	getBindingPolicyName BindingPolicyName[B]
	manifestType         metrics.ManifestType

	// current 指向当前生效的 hooks 切片；先完成编译再一次性发布，读者不会看到半成品。
	current      atomic.Pointer[[]generic.PolicyHook[P, B, E]]
	lastReadHash atomic.Pointer[string] // 上次读取的文件内容哈希（用于短路跳过）
	hasSynced    atomic.Bool
}

// NewStaticPolicySource 创建从指定目录加载配置的静态策略源。
// 构造函数不做 IO：初次加载由 LoadInitial 完成，便于调用方在启动失败时统一处理错误。
func NewStaticPolicySource[P, B runtime.Object, E generic.Evaluator](
	manifestsDir, apiServerID string,
	compiler PolicyCompiler[P, E],
	loadFunc PolicyLoadFunc[P, B],
	getBindingPolicyName BindingPolicyName[B],
	manifestType metrics.ManifestType,
) *StaticPolicySource[P, B, E] {
	metrics.RegisterMetrics()
	return &StaticPolicySource[P, B, E]{
		manifestsDir:         manifestsDir,
		apiServerID:          apiServerID,
		reloadInterval:       defaultReloadInterval,
		compiler:             compiler,
		loadFunc:             loadFunc,
		getBindingPolicyName: getBindingPolicyName,
		manifestType:         manifestType,
	}
}

// LoadInitial 执行清单的首次加载。应在 API server 启动期间调用；
// 清单加载失败时返回错误。
func (s *StaticPolicySource[P, B, E]) LoadInitial() error {
	policies, bindings, hash, err := s.loadFunc(s.manifestsDir)
	if err != nil {
		return err
	}

	// 编译失败同样视为启动错误：带着无效策略启动会让准入行为与配置不符。
	hooks, err := s.compile(policies, bindings)
	if err != nil {
		return err
	}
	// 先存内容哈希再置 hasSynced，保证观察者看到 synced 时快照已就绪。
	s.current.Store(&hooks)
	s.lastReadHash.Store(&hash)
	s.hasSynced.Store(true)

	klog.InfoS("Loaded manifest-based admission policy configurations", "plugin", string(s.manifestType), "count", len(hooks))
	metrics.RecordAutomaticReloadSuccess(s.manifestType, s.apiServerID, hash)
	return nil
}

// RunReloadLoop 监听配置变化并在检测到时重载。
// 该方法会阻塞直到 ctx 被取消。
func (s *StaticPolicySource[P, B, E]) RunReloadLoop(ctx context.Context) {
	// WatchUntil 负责按 reloadInterval 周期性触发回调，并在 ctx 取消时退出。
	filesystem.WatchUntil(
		ctx,
		s.reloadInterval,
		s.manifestsDir,
		func() {
			s.checkAndReload()
		},
		func(err error) {
			// 目录暂时不可读（如挂载卷重建）不应导致进程退出，仅记录并等待下一轮。
			klog.ErrorS(err, "watching manifest directory", "plugin", string(s.manifestType), "dir", s.manifestsDir)
		},
	)
}

// compile 将策略与绑定按策略名归组并逐一编译，任一策略编译失败则整体失败，
// 以避免部分生效造成的策略集不一致。
func (s *StaticPolicySource[P, B, E]) compile(policies []P, bindings []B) ([]generic.PolicyHook[P, B, E], error) {
	// 一次遍历建立 policy -> bindings 索引，避免 O(policies*bindings) 的嵌套扫描。
	bindingsByPolicy := make(map[string][]B)
	for _, binding := range bindings {
		policyName := s.getBindingPolicyName(binding)
		bindingsByPolicy[policyName] = append(bindingsByPolicy[policyName], binding)
	}

	var hooks []generic.PolicyHook[P, B, E]
	for _, policy := range policies {
		// 策略类型由调用方泛型决定，这里用结构断言取名字，避免为每种类型增加约束。
		nameGetter, ok := any(policy).(interface{ GetName() string })
		if !ok {
			return nil, fmt.Errorf("policy type %T does not implement GetName()", policy)
		}
		evaluator, err := s.compiler(policy)
		if err != nil {
			return nil, fmt.Errorf("failed to compile policy %q: %w", nameGetter.GetName(), err)
		}
		// 没有匹配绑定的策略也会保留，但其 hooks 不会被触发。
		hook := generic.PolicyHook[P, B, E]{
			Policy:    policy,
			Bindings:  bindingsByPolicy[nameGetter.GetName()],
			Evaluator: evaluator,
		}
		hooks = append(hooks, hook)
	}

	return hooks, nil
}

// checkAndReload 执行一次轮询：加载失败或编译失败时保留旧快照，仅记录指标与日志。
func (s *StaticPolicySource[P, B, E]) checkAndReload() {
	policies, bindings, hash, err := s.loadFunc(s.manifestsDir)
	if err != nil {
		klog.ErrorS(err, "reloading manifest config", "plugin", string(s.manifestType), "dir", s.manifestsDir)
		metrics.RecordAutomaticReloadFailure(s.manifestType, s.apiServerID)
		return
	}

	// 短路：文件内容与上次读取相比没有变化。
	// 目录扫描本身有成本，内容哈希相同则跳过解析与编译。
	if last := s.lastReadHash.Load(); last != nil && hash == *last {
		return
	}
	s.lastReadHash.Store(&hash)

	hooks, err := s.compile(policies, bindings)
	if err != nil {
		// 编译失败时哈希已更新，下轮不会再因相同内容重复报错，直到文件再次变化。
		klog.ErrorS(err, "compiling manifest config", "plugin", string(s.manifestType), "dir", s.manifestsDir)
		metrics.RecordAutomaticReloadFailure(s.manifestType, s.apiServerID)
		return
	}
	// 原子替换快照；已在进行中的请求继续使用旧快照，保证单请求内策略一致。
	s.current.Store(&hooks)

	klog.InfoS("reloaded manifest config", "plugin", string(s.manifestType), "dir", s.manifestsDir)
	metrics.RecordAutomaticReloadSuccess(s.manifestType, s.apiServerID, hash)
}

// HasSynced 在初次加载完成后返回 true。
func (s *StaticPolicySource[P, B, E]) HasSynced() bool {
	return s.hasSynced.Load()
}

// Hooks 返回策略 hook 列表。
// 返回的快照只读，调用方不得修改其元素。
func (s *StaticPolicySource[P, B, E]) Hooks() []generic.PolicyHook[P, B, E] {
	current := s.current.Load()
	if current == nil {
		return nil
	}
	return *current
}
