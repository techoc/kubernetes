/*
Copyright 2024 The Kubernetes Authors.

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

package features

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/util/naming"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
)

// internalPackages are packages that ignored when creating a name for featureGates. These packages are in the common
// call chains, so they'd be unhelpful as names.
//
// 这些包处于公共调用链上，若用于推断调用点名称只能得到无意义的 "features"，
// 因此从栈回溯中排除。
var internalPackages = []string{"k8s.io/client-go/features/envvar.go"}

var _ Gates = &envVarFeatureGates{}

// newEnvVarFeatureGates creates a feature gate that allows for registration
// of features and checking if the features are enabled.
//
// On the first call to Enabled, the effective state of all known features is loaded from
// environment variables. The environment variable read for a given feature is formed by
// concatenating the prefix "KUBE_FEATURE_" with the feature's name.
//
// For example, if you have a feature named "MyFeature"
// setting an environmental variable "KUBE_FEATURE_MyFeature"
// will allow you to configure the state of that feature.
//
// Please note that environmental variables can only be set to the boolean value.
// Incorrect values will be ignored and logged.
//
// Features can also be set directly via the Set method.
// In that case, these features take precedence over
// features set via environmental variables.
//
// newEnvVarFeatureGates 基于环境变量实现 Gates。延迟到首次 Enabled/Set 时才读取环境变量，
// 以避免包初始化阶段读取环境造成不可预期的行为。
func newEnvVarFeatureGates(features map[Feature]FeatureSpec) *envVarFeatureGates {
	// 复制一份已知开关，防止调用方之后修改传入 map 影响本实例。
	known := map[Feature]FeatureSpec{}
	for name, spec := range features {
		known[name] = spec
	}

	fg := &envVarFeatureGates{
		callSiteName: naming.GetNameFromCallsite(internalPackages...),
		known:        known,
	}
	fg.enabledViaEnvVar.Store(map[Feature]bool{})
	fg.enabledViaSetMethod = map[Feature]bool{}

	return fg
}

// envVarFeatureGates implements Gates and allows for feature registration.
type envVarFeatureGates struct {
	// callSiteName holds the name of the file
	// that created this instance
	callSiteName string

	// readEnvVarsOnce guards reading environmental variables
	readEnvVarsOnce sync.Once

	// known holds known feature gates
	known map[Feature]FeatureSpec

	// enabledViaEnvVar holds a map[Feature]bool
	// with values explicitly set via env var
	//
	// 使用 atomic.Value 存放只写一次的 map，读取路径无锁。
	enabledViaEnvVar atomic.Value

	// lockEnabledViaSetMethod protects enabledViaSetMethod
	lockEnabledViaSetMethod sync.RWMutex

	// enabledViaSetMethod holds values explicitly set
	// via Set method, features stored in this map take
	// precedence over features stored in enabledViaEnvVar
	enabledViaSetMethod map[Feature]bool

	// readEnvVars holds the boolean value which
	// indicates whether readEnvVarsOnce has been called.
	readEnvVars atomic.Bool
}

// Enabled returns true if the key is enabled. If the key is not known, this call will panic.
//
// 查找顺序：Set/SetForTesting 显式设置 > 环境变量 > 默认值 > panic（未知开关）。
// 未知开关直接 panic，是为了尽早暴露拼写错误，而不是静默返回 false。
func (f *envVarFeatureGates) Enabled(key Feature) bool {
	if v, ok := f.wasFeatureEnabledViaSetMethod(key); ok {
		// 即使命中 Set 值，也要触发一次环境变量加载，确保首次调用 Enabled 时
		// 已把所有已知 feature 的状态从环境变量读入，保证日志中打印的是完整状态。
		if !f.hasAlreadyReadEnvVar() {
			_ = f.getEnabledMapFromEnvVar()
		}
		return v
	}
	if v, ok := f.getEnabledMapFromEnvVar()[key]; ok {
		return v
	}
	if v, ok := f.known[key]; ok {
		return v.Default
	}
	panic(fmt.Errorf("feature %q is not registered in FeatureGates %q", key, f.callSiteName))
}

// Set sets the given feature to the given value.
//
// Features set via this method take precedence over
// the features set via environment variables.
func (f *envVarFeatureGates) Set(featureName Feature, featureValue bool) error {
	return f.set(featureName, featureValue, false)
}

// SetForTesting sets the given feature to the given value. This method
// bypasses the check for locked features and should only be used for
// testing purposes.
//
// Features set via this method take precedence over
// the features set via environment variables.
//
// SetForTesting 绕过 LockToDefault 检查，仅限测试使用。
func (f *envVarFeatureGates) SetForTesting(featureName Feature, featureValue bool) error {
	return f.set(featureName, featureValue, true)
}

// set 是 Set 与 SetForTesting 的共同实现；allowChangingLockedFeatures 决定是否允许
// 违反 LockToDefault 的锁定值。锁定是为了让已发布开关在移除前保持稳定。
func (f *envVarFeatureGates) set(featureName Feature, featureValue bool, allowChangingLockedFeatures bool) error {
	feature, ok := f.known[featureName]
	if !ok {
		return fmt.Errorf("feature %q is not registered in FeatureGates %q", featureName, f.callSiteName)
	}
	if !allowChangingLockedFeatures {
		if feature.LockToDefault && feature.Default != featureValue {
			return fmt.Errorf("cannot set feature gate %q to %v, feature is locked to %v", featureName, featureValue, feature.Default)
		}
	}

	f.lockEnabledViaSetMethod.Lock()
	defer f.lockEnabledViaSetMethod.Unlock()
	f.enabledViaSetMethod[featureName] = featureValue

	return nil
}

// getEnabledMapFromEnvVar will fill the enabled map on the first call.
// This is the only time a known feature can be set to a value
// read from the corresponding environmental variable.
//
// 只读取一次环境变量：后续修改环境不会再生效，这与“进程启动时配置固定”的语义一致。
func (f *envVarFeatureGates) getEnabledMapFromEnvVar() map[Feature]bool {
	f.readEnvVarsOnce.Do(func() {
		// This code does not really support contextual logging. Making it do so has huge
		// implications for several call chains because the Enabled call then needs
		// a `*WithLogger` variant. This does not matter in Kubernetes itself because
		// all Kubernetes components replace the feature gate implementation used
		// by client-go, but it might matter elsewhere.
		logger := klog.Background()

		featureGatesState := map[Feature]bool{}
		for feature, featureSpec := range f.known {
			featureState, featureStateSet := os.LookupEnv(fmt.Sprintf("KUBE_FEATURE_%s", feature))
			if !featureStateSet {
				continue
			}
			boolVal, boolErr := strconv.ParseBool(featureState)
			switch {
			case boolErr != nil:
				// 非法布尔值被忽略并记录，而不是让进程启动失败。
				utilruntime.HandleErrorWithLogger(logger, boolErr, "Could not set feature gate", "feature", feature, "desiredState", featureState)
			case featureSpec.LockToDefault:
				// 锁定开关只接受与默认值相同的环境变量值。
				if boolVal != featureSpec.Default {
					utilruntime.HandleErrorWithLogger(logger, nil, "Could not set feature gate, feature is locked", "feature", feature, "desiredState", featureState, "lockedState", featureSpec.Default)
					break
				}
				featureGatesState[feature] = featureSpec.Default
			default:
				featureGatesState[feature] = boolVal
			}
		}
		f.enabledViaEnvVar.Store(featureGatesState)
		f.readEnvVars.Store(true)

		// 在 V(1) 下打印每个开关的最终状态，便于排查为什么某个开关未生效。
		for feature, featureSpec := range f.known {
			if featureState, ok := featureGatesState[feature]; ok {
				logger.V(1).Info("Feature gate updated state", "feature", feature, "enabled", featureState)
				continue
			}
			logger.V(1).Info("Feature gate default state", "feature", feature, "enabled", featureSpec.Default)
		}
	})
	return f.enabledViaEnvVar.Load().(map[Feature]bool)
}

// wasFeatureEnabledViaSetMethod 加读锁查询 Set 方法写入的值。
func (f *envVarFeatureGates) wasFeatureEnabledViaSetMethod(key Feature) (bool, bool) {
	f.lockEnabledViaSetMethod.RLock()
	defer f.lockEnabledViaSetMethod.RUnlock()

	value, found := f.enabledViaSetMethod[key]
	return value, found
}

// hasAlreadyReadEnvVar 报告环境变量是否已加载；Enabled 用它避免重复触发。
func (f *envVarFeatureGates) hasAlreadyReadEnvVar() bool {
	return f.readEnvVars.Load()
}
