/*
Copyright 2020 The Kubernetes Authors.

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

package v1

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/go-logr/logr"

	"k8s.io/component-base/featuregate"
)

// logRegistry 是进程级单例：日志格式必须在任何 LoggingConfiguration 使用前注册。
var logRegistry = newLogFormatRegistry()

// logFormatRegistry stores factories for all supported logging formats.
//
// frozen 之后禁止再注册，防止组件启动后出现“前半段日志一种格式、后半段另一种”的情况。
type logFormatRegistry struct {
	mutex    sync.Mutex
	registry map[string]logFormat
	frozen   bool
}

// logFormat 把格式名绑定到工厂及其所需特性门控。
// 门控为 LoggingStableOptions 表示无需门控即可使用。
type logFormat struct {
	factory LogFormatFactory
	feature featuregate.Feature
}

// +k8s:deepcopy-gen=false

// RuntimeControl provides operations that aren't available through the normal
// Logger or LogSink API.
type RuntimeControl struct {
	// Flush ensures that all in-memory data is written.
	// May be nil.
	//
	// 进程退出前调用，确保缓冲日志落盘。
	Flush func()

	// SetVerbosityLevel changes the level for all Logger instances
	// derived from the initial one. May be nil.
	//
	// The parameter is intentionally a plain uint32 instead of
	// VerbosityLevel to enable implementations that don't need to import
	// the API (helps avoid circular dependencies).
	SetVerbosityLevel func(v uint32) error
}

// LogFormatFactory provides support for a certain additional,
// non-default log format.
type LogFormatFactory interface {
	// Create returns a logger with the requested configuration.
	Create(c LoggingConfiguration, o LoggingOptions) (logr.Logger, RuntimeControl)
}

// RegisterLogFormat registers support for a new logging format. This must be called
// before using any of the methods in LoggingConfiguration. The feature must
// be one of those defined in this package (typically LoggingAlphaOptions,
// LoggingBetaOptions or LoggingStableOptions).
//
// 允许在 init 阶段由格式插件自行注册，但必须在注册表冻结前完成。
func RegisterLogFormat(name string, factory LogFormatFactory, feature featuregate.Feature) error {
	return logRegistry.register(name, logFormat{factory, feature})
}

// newLogFormatRegistry 预先注册默认文本格式，使注册表启动即至少有一个可用格式。
func newLogFormatRegistry() *logFormatRegistry {
	registry := &logFormatRegistry{
		registry: make(map[string]logFormat),
		frozen:   false,
	}
	_ = registry.register(DefaultLogFormat, logFormat{factory: textFactory{}, feature: LoggingStableOptions})
	return registry
}

// register adds a new log format. It's an error to modify an existing one.
func (lfr *logFormatRegistry) register(name string, format logFormat) error {
	lfr.mutex.Lock()
	defer lfr.mutex.Unlock()
	if lfr.frozen {
		return fmt.Errorf("log format registry is frozen, unable to register log format %s", name)
	}
	// 已存在的名称不允许覆盖，避免插件之间相互干扰。
	if _, ok := lfr.registry[name]; ok {
		return fmt.Errorf("log format: %s already exists", name)
	}
	// 除稳定格式外，其余格式必须对应本包已定义的特性门控，防止插件使用未声明的门控。
	if _, ok := featureGates()[format.feature]; !ok && format.feature != LoggingStableOptions {
		return fmt.Errorf("log format %s: unsupported feature gate %s", name, format.feature)
	}
	lfr.registry[name] = format
	return nil
}

// get specified log format factory
func (lfr *logFormatRegistry) get(name string) (*logFormat, error) {
	lfr.mutex.Lock()
	defer lfr.mutex.Unlock()
	format, ok := lfr.registry[name]
	if !ok {
		return nil, fmt.Errorf("log format: %s does not exists", name)
	}
	return &format, nil
}

// list names of registered log formats, including feature gates (sorted)
//
// 排序后输出，使帮助文本与错误信息稳定可测。
func (lfr *logFormatRegistry) list() string {
	lfr.mutex.Lock()
	defer lfr.mutex.Unlock()
	formats := make([]string, 0, len(lfr.registry))
	for name, format := range lfr.registry {
		item := fmt.Sprintf(`"%s"`, name)
		if format.feature != LoggingStableOptions {
			item += fmt.Sprintf(" (gated by %s)", format.feature)
		}
		formats = append(formats, item)
	}
	sort.Strings(formats)
	return strings.Join(formats, ", ")
}

// freeze prevents further modifications of the registered log formats.
//
// 通常在配置初始化时调用，之后所有注册尝试都会失败。
func (lfr *logFormatRegistry) freeze() {
	lfr.mutex.Lock()
	defer lfr.mutex.Unlock()
	lfr.frozen = true
}
