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

// Package source 提供一个 Source 实现，从静态清单文件加载 webhook 配置。
package source

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission/plugin/manifest/metrics"
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
	stagingloader "k8s.io/apiserver/pkg/admission/plugin/webhook/manifest/loader"
	"k8s.io/apiserver/pkg/util/filesystem"
	"k8s.io/klog/v2"
)

// defaultReloadInterval 是检查清单目录变更的默认间隔。
var defaultReloadInterval = 1 * time.Minute

// SetReloadIntervalForTests 为测试设置重载间隔，并返回一个恢复原值的函数。
//
// 这是包级全局变量，测试必须用返回的函数恢复原值，否则会污染同包其他用例。
func SetReloadIntervalForTests(interval time.Duration) func() {
	original := defaultReloadInterval
	defaultReloadInterval = interval
	return func() {
		defaultReloadInterval = original
	}
}

// ValidatingWebhookLoadFunc 从目录加载 validating webhook 配置，
// 返回配置列表、用于变更检测的哈希字符串以及错误。
//
// hash 由文件内容计算，用于在内容未变时跳过重建 accessor 的开销。
type ValidatingWebhookLoadFunc func(dir string) ([]*admissionregistrationv1.ValidatingWebhookConfiguration, string, error)

// MutatingWebhookLoadFunc 从目录加载 mutating webhook 配置，
// 返回配置列表、用于变更检测的哈希字符串以及错误。
type MutatingWebhookLoadFunc func(dir string) ([]*admissionregistrationv1.MutatingWebhookConfiguration, string, error)

// accessorBuilder 从配置构建 webhook 访问器。
type accessorBuilder[T runtime.Object] func(configs []T) []webhook.WebhookAccessor

// webhookData 保存当前已加载的 webhook 配置与预构建的访问器。
//
// 配置与 accessor 放在同一个不可变快照里整体原子替换，读者不会看到半更新的状态。
type webhookData[T runtime.Object] struct {
	configurations []T
	accessors      []webhook.WebhookAccessor
}

// staticWebhookSource 提供从清单文件加载的 webhook 配置。
//
// 所有可变状态都用 atomic 保存：读路径（每个准入请求）无锁，写路径（重载循环）单线程。
type staticWebhookSource[T runtime.Object] struct {
	manifestsDir   string
	apiServerID    string
	reloadInterval time.Duration
	webhookType    metrics.ManifestType
	loadFunc       func(dir string) ([]T, string, error)
	buildAccessors accessorBuilder[T]

	current      atomic.Pointer[webhookData[T]]
	lastReadHash atomic.Pointer[string] // 上次读取的文件内容哈希（用于短路跳过重载）
	hasSynced    atomic.Bool
}

// ValidatingSource 提供从清单文件加载的 validating webhook 配置。
type ValidatingSource struct {
	*staticWebhookSource[*admissionregistrationv1.ValidatingWebhookConfiguration]
}

// MutatingSource 提供从清单文件加载的 mutating webhook 配置。
type MutatingSource struct {
	*staticWebhookSource[*admissionregistrationv1.MutatingWebhookConfiguration]
}

// NewValidatingSource 创建一个新的 validating webhook 数据源，从指定目录加载配置。
func NewValidatingSource(manifestsDir, apiServerID string, loadFunc ValidatingWebhookLoadFunc) *ValidatingSource {
	metrics.RegisterMetrics()
	return &ValidatingSource{
		staticWebhookSource: &staticWebhookSource[*admissionregistrationv1.ValidatingWebhookConfiguration]{
			manifestsDir:   manifestsDir,
			apiServerID:    apiServerID,
			reloadInterval: defaultReloadInterval,
			webhookType:    metrics.ValidatingWebhookManifestType,
			loadFunc:       loadFunc,
			buildAccessors: stagingloader.BuildValidatingAccessors,
		},
	}
}

// NewMutatingSource 创建一个新的 mutating webhook 数据源，从指定目录加载配置。
func NewMutatingSource(manifestsDir, apiServerID string, loadFunc MutatingWebhookLoadFunc) *MutatingSource {
	metrics.RegisterMetrics()
	return &MutatingSource{
		staticWebhookSource: &staticWebhookSource[*admissionregistrationv1.MutatingWebhookConfiguration]{
			manifestsDir:   manifestsDir,
			apiServerID:    apiServerID,
			reloadInterval: defaultReloadInterval,
			webhookType:    metrics.MutatingWebhookManifestType,
			loadFunc:       loadFunc,
			buildAccessors: stagingloader.BuildMutatingAccessors,
		},
	}
}

// LoadInitial 执行 webhook 清单的初始加载。
//
// 失败直接返回错误，让 apiserver 启动失败：静态清单是启动前提，静默降级会让本应被拦截的
// 请求放行。
func (s *staticWebhookSource[T]) LoadInitial() error {
	configs, hash, err := s.loadFunc(s.manifestsDir)
	if err != nil {
		return err
	}

	accessors := s.buildAccessors(configs)

	// 提前校验 accessor，以便在启动时就发现选择器解析错误。
	if err := validateAccessors(accessors); err != nil {
		return err
	}

	s.current.Store(&webhookData[T]{configurations: configs, accessors: accessors})
	s.lastReadHash.Store(&hash)
	s.hasSynced.Store(true)

	klog.InfoS("Loaded manifest-based webhook configurations", "plugin", string(s.webhookType),
		"configurations", len(configs))
	metrics.RecordAutomaticReloadSuccess(s.webhookType, s.apiServerID, hash)
	return nil
}

// RunReloadLoop 监听配置变更并在检测到变化时重载。
// 该方法会阻塞直到 ctx 被取消。
func (s *staticWebhookSource[T]) RunReloadLoop(ctx context.Context) {
	filesystem.WatchUntil(
		ctx,
		s.reloadInterval,
		s.manifestsDir,
		func() {
			s.checkAndReload()
		},
		func(err error) {
			klog.ErrorS(err, "watching manifest directory", "plugin", string(s.webhookType), "dir", s.manifestsDir)
		},
	)
}

// checkAndReload 尝试重载清单；任何失败都保留上一次成功的配置，避免短暂的文件错误
// 导致准入策略全部失效。
func (s *staticWebhookSource[T]) checkAndReload() {
	configs, hash, err := s.loadFunc(s.manifestsDir)
	if err != nil {
		klog.ErrorS(err, "reloading admission manifest config", "plugin", string(s.webhookType), "dir", s.manifestsDir)
		metrics.RecordAutomaticReloadFailure(s.webhookType, s.apiServerID)
		return
	}

	// 若文件内容自上次读取以来未变化，则短路返回。
	if last := s.lastReadHash.Load(); last != nil && hash == *last {
		return
	}
	s.lastReadHash.Store(&hash)

	// 构建并校验 accessor（文件被删除时 configs 可能为空）
	accessors := s.buildAccessors(configs)
	if err := validateAccessors(accessors); err != nil {
		klog.ErrorS(err, "manifest webhook accessor validation failed after reload", "plugin", string(s.webhookType), "dir", s.manifestsDir)
		metrics.RecordAutomaticReloadFailure(s.webhookType, s.apiServerID)
		return
	}

	s.current.Store(&webhookData[T]{configurations: configs, accessors: accessors})
	klog.InfoS("reloaded admission manifest config", "plugin", string(s.webhookType), "dir", s.manifestsDir)
	metrics.RecordAutomaticReloadSuccess(s.webhookType, s.apiServerID, hash)
}

// HasSynced 在初始加载完成后返回 true。
func (s *staticWebhookSource[T]) HasSynced() bool {
	return s.hasSynced.Load()
}

// Webhooks 返回 webhook 访问器列表。
//
// 返回的切片属于不可变快照，调用方不得修改；初始加载完成前返回 nil。
func (s *staticWebhookSource[T]) Webhooks() []webhook.WebhookAccessor {
	current := s.current.Load()
	if current == nil {
		return nil
	}
	return current.accessors
}

// validateAccessors 对每个 accessor 触发惰性 getter，以便尽早暴露潜在错误
// （如无效的标签选择器）。
// 注意：这里有意跳过 GetRESTClient 校验，因为它需要 ClientManager。
// API 校验（在默认化阶段执行）已确保 CA bundle 可解析、URL 存在且合法，
// 且清单加载器会拒绝 service 引用，这已覆盖准入时构造 REST 客户端所需的输入。
func validateAccessors(accessors []webhook.WebhookAccessor) error {
	var errs []error
	for _, a := range accessors {
		// 选择器解析失败时 informer 的过滤逻辑会 panic 或静默放行，必须在加载期暴露。
		if _, err := a.GetParsedNamespaceSelector(); err != nil {
			errs = append(errs, fmt.Errorf("webhook %q: invalid namespaceSelector: %w", a.GetName(), err))
		}
		if _, err := a.GetParsedObjectSelector(); err != nil {
			errs = append(errs, fmt.Errorf("webhook %q: invalid objectSelector: %w", a.GetName(), err))
		}
	}
	// 汇总所有错误而非首个错误，便于一次性修复多个清单。
	return errors.Join(errs...)
}
