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

package metrics

import (
	"fmt"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"

	"k8s.io/klog/v2"
)

// Desc 是 prometheus.Desc 的扩展，可通过 NewDesc 创建。
// Desc 扩展了 prometheus.Desc，额外携带稳定性级别与弃用信息。
// 创建被延迟到注册时，以便注册时才知道当前版本并判断是否隐藏。
type Desc struct {
	// fqName 由 Namespace、Subsystem 和 Name 拼接而成。
	fqName string
	// help 提供该指标的说明信息。
	help string
	// constLabels 保存标签名，其标签值固定不变。
	constLabels Labels
	// variableLabels 保存指标所维护可变值的标签名。
	variableLabels []string

	// promDesc 是每个 Prometheus Metric 使用的描述符。
	promDesc      *prometheus.Desc
	annotatedHelp string

	// stabilityLevel 表示已定义指标的 API 保证级别。
	stabilityLevel StabilityLevel
	// deprecatedVersion 表示该指标被弃用的版本。
	deprecatedVersion string

	isDeprecated bool
	isHidden     bool
	isCreated    bool
	// createLock 保护 isCreated 等状态的读写；其余 Once 保证各阶段只执行一次。
	createLock          sync.RWMutex
	markDeprecationOnce sync.Once
	createOnce          sync.Once
	deprecateOnce       sync.Once
	hideOnce            sync.Once
	annotateOnce        sync.Once
}

// NewDesc 扩展 prometheus.NewDesc，增加稳定性支持。
//
// stabilityLevel 应为有效的稳定性标签，如 "metrics.ALPHA" 和 "metrics.STABLE"（未来可能支持
// "metrics.BETA"）；为空或非法时默认使用 "metrics.ALPHA"。
//
// deprecatedVersion 表示该指标的弃用版本，弃用策略遵循控制面指标稳定性 KEP。
func NewDesc(fqName string, help string, variableLabels []string, constLabels Labels,
	stabilityLevel StabilityLevel, deprecatedVersion string) *Desc {
	d := &Desc{
		fqName:            fqName,
		help:              help,
		annotatedHelp:     help,
		variableLabels:    variableLabels,
		constLabels:       constLabels,
		stabilityLevel:    stabilityLevel,
		deprecatedVersion: deprecatedVersion,
	}
	d.stabilityLevel.setDefaults()

	return d
}

// String 将 Desc 格式化为字符串；若在 registry 处理后调用，稳定性元数据会标注在 HELP 段，否则不会。
// 例如："Desc{fqName: "normal_stable_descriptor", help: "[STABLE] this is a stable descriptor", constLabels: {}, variableLabels: []}"
func (d *Desc) String() string {
	if d.isCreated {
		return d.promDesc.String()
	}

	// 未创建时临时构造 desc 仅用于展示，不会产生注册副作用。
	return prometheus.NewDesc(d.fqName, d.help, d.variableLabels, prometheus.Labels(d.constLabels)).String()
}

// toPrometheusDesc 将自身转换为 prometheus.Desc。
func (d *Desc) toPrometheusDesc() *prometheus.Desc {
	return d.promDesc
}

// DeprecatedVersion 返回弃用 Version 的指针；未设置时返回 nil。
func (d *Desc) DeprecatedVersion() *semver.Version {
	return parseSemver(d.deprecatedVersion)

}

// determineDeprecationStatus 依据当前版本计算弃用/隐藏状态。
// 隐藏判定仅执行一次；若用户显式要求展示隐藏指标，则保持可见并告警。
func (d *Desc) determineDeprecationStatus(currentVersion semver.Version) {
	deprecatedVersion := d.DeprecatedVersion()
	if deprecatedVersion == nil {
		return
	}
	d.markDeprecationOnce.Do(func() {
		d.isDeprecated = isDeprecated(currentVersion, *deprecatedVersion)
		if shouldHide(d.stabilityLevel, &currentVersion, deprecatedVersion) {
			if ShouldShowHidden() {
				klog.Warningf("Hidden metrics(%s) have been manually overridden, showing this very deprecated metric.", d.fqName)
				return
			}
			// TODO(RainbowMango): 暂时移除此日志。https://github.com/kubernetes/kubernetes/issues/85369
			// klog.Warningf("This metric(%s) has been deprecated for more than one release, hiding.", d.fqName)
			d.isHidden = true
		}
	})
}

// IsHidden 报告指标是否将被隐藏。
func (d *Desc) IsHidden() bool {
	return d.isHidden
}

// IsDeprecated 报告指标是否已弃用。
func (d *Desc) IsDeprecated() bool {
	return d.isDeprecated
}

// IsCreated 报告指标是否已经创建。
func (d *Desc) IsCreated() bool {
	d.createLock.RLock()
	defer d.createLock.RUnlock()

	return d.isCreated
}

// create 强制完成此前延迟的 Desc 初始化，并判定其是否已弃用或隐藏；应隐藏时不执行创建。
// 若 Desc 已经创建，则不再操作并返回 true。
func (d *Desc) create(version *semver.Version) bool {
	if version != nil {
		d.determineDeprecationStatus(*version)
	}

	// 已决定隐藏的指标不创建，避免出现在输出中。
	if d.IsHidden() {
		return false
	}
	d.createOnce.Do(func() {
		d.createLock.Lock()
		defer d.createLock.Unlock()

		d.isCreated = true
		// 弃用指标在 Help 中额外标注，方便用户发现并迁移。
		if d.IsDeprecated() {
			d.initializeDeprecatedDesc()
		} else {
			d.initialize()
		}
	})
	return d.IsCreated()
}

// 重置所有状态与 Once，使同一个 Desc 可以被重新注册（例如测试中换 registry）。
func (d *Desc) ClearState() {
	d.isDeprecated = false
	d.isHidden = false
	d.isCreated = false

	d.markDeprecationOnce = *new(sync.Once)
	d.createOnce = *new(sync.Once)
	d.deprecateOnce = *new(sync.Once)
	d.hideOnce = *new(sync.Once)
	d.annotateOnce = *new(sync.Once)

	d.annotatedHelp = d.help
	d.promDesc = nil
}

// markDeprecated 在 Help 前加上 "(Deprecated since <version>)"，只添加一次。
func (d *Desc) markDeprecated() {
	d.deprecateOnce.Do(func() {
		d.annotatedHelp = fmt.Sprintf("(Deprecated since %s) %s", d.deprecatedVersion, d.annotatedHelp)
	})
}

// annotateStabilityLevel 在 Help 前加上如 "[ALPHA]" 的稳定性标记，只添加一次。
func (d *Desc) annotateStabilityLevel() {
	d.annotateOnce.Do(func() {
		d.annotatedHelp = fmt.Sprintf("[%v] %v", d.stabilityLevel, d.annotatedHelp)
	})
}

// initialize 创建真正的 prometheus.Desc；Help 已包含稳定性/弃用标注。
func (d *Desc) initialize() {
	d.annotateStabilityLevel()

	// 这里才真正创建底层 prometheus desc。
	d.promDesc = prometheus.NewDesc(d.fqName, d.annotatedHelp, d.variableLabels, prometheus.Labels(d.constLabels))
}

// initializeDeprecatedDesc 为弃用指标创建 Desc，先标注弃用版本再初始化。
func (d *Desc) initializeDeprecatedDesc() {
	d.markDeprecated()
	d.initialize()
}

// GetRawDesc 返回使用 NewDesc() 原始参数构造的新 *Desc。
//
// 在测试中将同一个 Desc 注册到不同 registry 时很有用：
//  1. Desc `D` 在 TestA 中注册到 registry 'A'（注意：`D` 可能已经创建）。
//  2. Desc `D` 在 TestB 中注册到 registry 'B'（注意：`D` 已创建过一次，因此会被 registry 'B' 忽略）。
func (d *Desc) GetRawDesc() *Desc {
	return NewDesc(d.fqName, d.help, d.variableLabels, d.constLabels, d.stabilityLevel, d.deprecatedVersion)
}
