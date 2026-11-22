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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/component-base/version"
	"k8s.io/klog/v2"

	"go.yaml.in/yaml/v2"
	v1 "k8s.io/component-base/metrics/api/v1"
)

var (
	// disabledMetrics 保存被 --disabled-metrics 关闭的指标全名；
	// 用读写锁保护，因为配置可在运行期被应用。
	disabledMetricsLock sync.RWMutex
	disabledMetrics     = map[string]struct{}{}
	// showHidden 只允许被开启一次：一旦开启，后续调用应无副作用。
	showHiddenOnce sync.Once
	showHidden     atomic.Bool
)

var (
	// disabledMetricsTotal 记录被关闭的指标数量，便于确认配置是否意外拦截了指标。
	disabledMetricsTotal = NewCounter(
		&CounterOpts{
			Name:           "disabled_metrics_total",
			Help:           "The count of disabled metrics.",
			StabilityLevel: BETA,
		},
	)

	// hiddenMetricsTotal 记录被隐藏的弃用指标数量。
	hiddenMetricsTotal = NewCounter(
		&CounterOpts{
			Name:           "hidden_metrics_total",
			Help:           "The count of hidden metrics.",
			StabilityLevel: BETA,
		},
	)

	// 当标签值不在 allow list 中而被替换为 "unexpected" 时计数，
	// 用于发现基数失控或配置不匹配。
	cardinalityEnforcementUnexpectedCategorizationsTotal = NewCounter(
		&CounterOpts{
			Name:           "cardinality_enforcement_unexpected_categorizations_total",
			Help:           "The count of unexpected categorizations during cardinality enforcement.",
			StabilityLevel: ALPHA,
		},
	)
)

// Options has all parameters needed for exposing metrics from components
// Options 汇集暴露组件指标所需的参数，可直接嵌入组件的配置结构。
type Options struct {
	// 这里省略配置的序列化：父配置不会嵌入该类型，避免字段重复暴露。
	v1.MetricsConfiguration `json:"-"`
}

// NewOptions returns default metrics options
func NewOptions() *Options {
	return &Options{}
}

// AddFlags adds flags for exposing component metrics.
// This won't be called in embedded instances within component configurations.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}
	fs.StringVar(&o.ShowHiddenMetricsForVersion, "show-hidden-metrics-for-version", o.ShowHiddenMetricsForVersion,
		"The previous version for which you want to show hidden metrics. "+
			"Only the previous minor version is meaningful, other values will not be allowed. "+
			"The format is <major>.<minor>, e.g.: '1.16'. "+
			"The purpose of this format is make sure you have the opportunity to notice if the next release hides additional metrics, "+
			"rather than being surprised when they are permanently removed in the release after that.")
	fs.StringSliceVar(&o.DisabledMetrics,
		"disabled-metrics",
		o.DisabledMetrics,
		"This flag provides an escape hatch for misbehaving metrics. "+
			"You must provide the fully qualified metric name in order to disable it. "+
			"Disclaimer: disabling metrics is higher in precedence than showing hidden metrics.")
	fs.StringToStringVar(&o.AllowListMapping, "allow-metric-labels", o.AllowListMapping,
		"The map from metric-label to value allow-list of this label. The key's format is <MetricName>,<LabelName>. "+
			"The value's format is <allowed_value>,<allowed_value>..."+
			"e.g. metric1,label1='v1,v2,v3', metric1,label2='v1,v2,v3' metric2,label1='v1,v2,v3'.")
	fs.StringVar(&o.AllowListMappingManifest, "allow-metric-labels-manifest", o.AllowListMappingManifest,
		"The path to the manifest file that contains the allow-list mapping. "+
			"The format of the file is the same as the flag --allow-metric-labels, i.e., \n"+
			"allowListMapping:\n  \"metric1,label1\": \"value11,value12\"\n  \"metric2,label2\": \"\"\n"+
			"Note that the flag --allow-metric-labels will override the manifest file.")
}

// SetShowHidden will enable showing hidden metrics. This will no-opt
// after the initial call
func SetShowHidden() {
	showHiddenOnce.Do(func() {
		showHidden.Store(true)

		// 上一轮注册时被隐藏的 collector 需要重新启用，否则已创建的指标仍不可见。
		for _, r := range registries {
			r.enableHiddenCollectors()
			r.enableHiddenStableCollectors()
		}
	})
}

// ShouldShowHidden returns whether showing hidden deprecated metrics is enabled.
// While the primary use case for this is internal (to determine registration behavior) this can also be used to introspect.
func ShouldShowHidden() bool {
	return showHidden.Load()
}

// SetDisabledMetric will disable a metric by name.
// This will also increment the disabled metrics counter.
// Note that this is a no-op if the metric is already disabled.
func SetDisabledMetrics(names []string) {
	for _, name := range names {
		func(name string) {
			// 空名称不是合法的 Prometheus 指标名，忽略以免污染禁用集合。
			if name == "" {
				klog.Warningf("Attempted to disable an empty metric name, ignoring.")
				return
			}
			disabledMetricsLock.Lock()
			defer disabledMetricsLock.Unlock()
			// 只在首次禁用时计数，重复配置不重复累加。
			if _, ok := disabledMetrics[name]; !ok {
				disabledMetrics[name] = struct{}{}
				disabledMetricsTotal.Inc()
			}
		}(name)
	}
}

// MetricLabelAllowList 保存某个指标的 标签名 -> 允许值集合 映射。
type MetricLabelAllowList struct {
	labelToAllowList map[string]sets.Set[string]
}

// ConstrainToAllowedList 按位置约束标签值：越界值替换为 "unexpected" 并计数。
// 位置约束依赖 labelNameList 与 labelValueList 顺序一致（即 Desc 的字段顺序）。
func (allowList *MetricLabelAllowList) ConstrainToAllowedList(labelNameList, labelValueList []string) {
	for index, value := range labelValueList {
		name := labelNameList[index]
		if allowValues, ok := allowList.labelToAllowList[name]; ok {
			if !allowValues.Has(value) {
				labelValueList[index] = "unexpected"
				cardinalityEnforcementUnexpectedCategorizationsTotal.Inc()
			}
		}
	}
}

// ConstrainLabelMap 对 map 形式的标签执行同样的允许值约束。
func (allowList *MetricLabelAllowList) ConstrainLabelMap(labels map[string]string) {
	for name, value := range labels {
		if allowValues, ok := allowList.labelToAllowList[name]; ok {
			if !allowValues.Has(value) {
				labels[name] = "unexpected"
				cardinalityEnforcementUnexpectedCategorizationsTotal.Inc()
			}
		}
	}
}

// SetLabelAllowList 注册内联的 allow list 配置。key 的格式为 "<metricName>,<labelName>"。
func SetLabelAllowList(allowListMapping map[string]string) {
	if len(allowListMapping) == 0 {
		klog.Errorf("empty allow-list mapping supplied, ignoring.")
		return
	}

	allowListLock.Lock()
	defer allowListLock.Unlock()
	for metricLabelName, labelValues := range allowListMapping {
		metricName := strings.Split(metricLabelName, ",")[0]
		labelName := strings.Split(metricLabelName, ",")[1]
		valueSet := sets.New[string](strings.Split(labelValues, ",")...)

		allowList, ok := labelValueAllowLists[metricName]
		if ok {
			allowList.labelToAllowList[labelName] = valueSet
		} else {
			labelToAllowList := make(map[string]sets.Set[string])
			labelToAllowList[labelName] = valueSet
			labelValueAllowLists[metricName] = &MetricLabelAllowList{
				labelToAllowList,
			}
		}
	}
}

// SetLabelAllowListFromManifest 从 YAML 清单读取 allow list 配置并注册。
func SetLabelAllowListFromManifest(manifest string) {
	if manifest == "" {
		klog.Errorf("The manifest file is empty, ignoring.")
		return
	}

	data, err := os.ReadFile(filepath.Clean(manifest))
	if err != nil {
		klog.Errorf("Failed to read allow list manifest: %v", err)
		return
	}
	allowListMapping := make(map[string]string)
	err = yaml.Unmarshal(data, &allowListMapping)
	if err != nil {
		klog.Errorf("Failed to parse allow list manifest: %v", err)
		return
	}
	SetLabelAllowList(allowListMapping)
}

// Apply applies a MetricsConfiguration into global configuration of metrics.
func Apply(c *v1.MetricsConfiguration) {
	if c == nil {
		return
	}

	// 只要配置了展示隐藏指标的版本，就开启展示开关（版本合法性在 Validate 中检查）。
	if len(c.ShowHiddenMetricsForVersion) > 0 {
		SetShowHidden()
	}
	SetDisabledMetrics(c.DisabledMetrics)
	// 内联映射优先于清单文件；两者互斥已由 Validate 保证。
	if c.AllowListMapping != nil {
		SetLabelAllowList(c.AllowListMapping)
	} else {
		SetLabelAllowListFromManifest(c.AllowListMappingManifest)
	}
}

// Apply applies parameters into global configuration of metrics.
func (o *Options) Apply() {
	if o == nil {
		return
	}

	Apply(&o.MetricsConfiguration)
}

// ValidateShowHiddenMetricsVersion checks invalid version for which show hidden metrics.
// TODO: This is kept here for backward compatibility in Kubelet (as metrics configuration fields were exposed on an individual basis earlier).
// TODO: Revisit this after Kubelet supports the new metrics configuration API.
func ValidateShowHiddenMetricsVersion(v string) []error {
	err := v1.ValidateShowHiddenMetricsVersionForKubeletBackwardCompatOnly(parseVersion(version.Get()), v)
	if err != nil {
		return []error{err}
	}

	return nil
}

// Validate validates metrics flags options.
func (o *Options) Validate() []error {
	if o == nil {
		return nil
	}
	currentVersion := parseVersion(version.Get())
	fldPath := field.NewPath("metrics")
	fldErrs := v1.Validate(&o.MetricsConfiguration, currentVersion, fldPath)

	if len(fldErrs) == 0 {
		return nil
	}

	return fldErrs.ToAggregate().Errors()
}
