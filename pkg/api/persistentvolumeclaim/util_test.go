/*
Copyright 2017 The Kubernetes Authors.

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

package persistentvolumeclaim

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"

	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/features"
)

// TestDropDisabledSnapshotDataSource 验证 EnforceDataSourceBackwardsCompatibility
// 在传入 nil 旧对象（创建请求）时不会修改新旧 PVC：旧对象本就不该被改动，
// 而这里没有可提供“已在使用”证据的旧 spec，新对象也应保持原样。
func TestDropDisabledSnapshotDataSource(t *testing.T) {
	pvcWithoutDataSource := func() *core.PersistentVolumeClaim {
		return &core.PersistentVolumeClaim{
			Spec: core.PersistentVolumeClaimSpec{
				DataSource: nil,
			},
		}
	}
	apiGroup := "snapshot.storage.k8s.io"
	pvcWithDataSource := func() *core.PersistentVolumeClaim {
		return &core.PersistentVolumeClaim{
			Spec: core.PersistentVolumeClaimSpec{
				DataSource: &core.TypedLocalObjectReference{
					APIGroup: &apiGroup,
					Kind:     "VolumeSnapshot",
					Name:     "test_snapshot",
				},
			},
		}
	}

	pvcInfo := []struct {
		description string
		pvc         func() *core.PersistentVolumeClaim
	}{
		{
			description: "pvc without DataSource",
			pvc:         pvcWithoutDataSource,
		},
		{
			description: "pvc with DataSource",
			pvc:         pvcWithDataSource,
		},
		{
			description: "is nil",
			pvc:         func() *core.PersistentVolumeClaim { return nil },
		},
	}

	for _, oldpvcInfo := range pvcInfo {
		for _, newpvcInfo := range pvcInfo {
			oldpvc := oldpvcInfo.pvc()
			newpvc := newpvcInfo.pvc()
			if newpvc == nil {
				// 新对象为 nil 时没有 spec 可处理，跳过该组合。
				continue
			}

			t.Run(fmt.Sprintf("old pvc %v, new pvc %v", oldpvcInfo.description, newpvcInfo.description), func(t *testing.T) {
				EnforceDataSourceBackwardsCompatibility(&newpvc.Spec, nil)

				// 旧对象是只读输入，函数在任何情况下都不应改动它。
				if !reflect.DeepEqual(oldpvc, oldpvcInfo.pvc()) {
					t.Errorf("old pvc changed: %v", cmp.Diff(oldpvc, oldpvcInfo.pvc()))
				}

				// 旧对象为 nil 时不提供任何“已在用”的证据，因此新对象也不应被改动。
				if !reflect.DeepEqual(newpvc, newpvcInfo.pvc()) {
					t.Errorf("new pvc changed: %v", cmp.Diff(newpvc, newpvcInfo.pvc()))
				}
			})
		}
	}
}

// TestPVCDataSourceSpecFilter 验证 PVCDataSource 特性门控下 DataSource 的裁剪：
// 空 spec 与 APIGroup 非法的 DataSource 应被丢弃；APIGroup 为空字符串
// （核心组）或显式为空指针的 DataSource 都是合法 Clone 来源，必须保留。
// 注意本用例面向旧对象为 nil 的创建路径。
func TestPVCDataSourceSpecFilter(t *testing.T) {
	apiGroup := ""
	validSpec := core.PersistentVolumeClaimSpec{
		DataSource: &core.TypedLocalObjectReference{
			APIGroup: &apiGroup,
			Kind:     "PersistentVolumeClaim",
			Name:     "test_clone",
		},
	}
	validSpecNilAPIGroup := core.PersistentVolumeClaimSpec{
		DataSource: &core.TypedLocalObjectReference{
			Kind: "PersistentVolumeClaim",
			Name: "test_clone",
		},
	}

	// 非法 APIGroup 不是有效的卷克隆来源，应被丢弃。
	invalidAPIGroup := "invalid.pvc.api.group"
	invalidSpec := core.PersistentVolumeClaimSpec{
		DataSource: &core.TypedLocalObjectReference{
			APIGroup: &invalidAPIGroup,
			Kind:     "PersistentVolumeClaim",
			Name:     "test_clone_invalid",
		},
	}

	var tests = map[string]struct {
		spec core.PersistentVolumeClaimSpec
		want *core.TypedLocalObjectReference
	}{
		"enabled with empty ds": {
			spec: core.PersistentVolumeClaimSpec{},
			want: nil,
		},
		"enabled with invalid spec": {
			spec: invalidSpec,
			want: nil,
		},
		"enabled with valid spec": {
			spec: validSpec,
			want: validSpec.DataSource,
		},
		"enabled with valid spec but nil APIGroup": {
			spec: validSpecNilAPIGroup,
			want: validSpecNilAPIGroup.DataSource,
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			EnforceDataSourceBackwardsCompatibility(&test.spec, nil)
			if test.spec.DataSource != test.want {
				t.Errorf("expected drop datasource condition was not met, test: %s, spec: %v, expected: %v", testName, test.spec, test.want)
			}

		})
	}
}

var (
	coreGroup    = ""
	snapGroup    = "snapshot.storage.k8s.io"
	genericGroup = "generic.storage.k8s.io"
	pvcKind      = "PersistentVolumeClaim"
	snapKind     = "VolumeSnapshot"
	genericKind  = "Generic"
	podKind      = "Pod"
)

func makeDataSource(apiGroup, kind, name string) *core.TypedLocalObjectReference {
	return &core.TypedLocalObjectReference{
		APIGroup: &apiGroup,
		Kind:     kind,
		Name:     name,
	}
}

func makeDataSourceRef(apiGroup, kind, name string, namespace *string) *core.TypedObjectReference {
	return &core.TypedObjectReference{
		APIGroup:  &apiGroup,
		Kind:      kind,
		Name:      name,
		Namespace: namespace,
	}
}

// TestDataSourceFilter 验证 DropDisabledFields 对跨命名空间数据源的裁剪：
// 门控关闭时新出现的跨命名空间 DataSourceRef 应被丢弃；但旧 spec 中已存在
// 该字段时（升级/回滚场景）必须保留，以免破坏既有对象；门控开启时一律保留。
func TestDataSourceFilter(t *testing.T) {
	ns := "ns1"
	volumeDataSource := makeDataSource(coreGroup, pvcKind, "my-vol")
	volumeDataSourceRef := makeDataSourceRef(coreGroup, pvcKind, "my-vol", nil)
	xnsVolumeDataSourceRef := makeDataSourceRef(coreGroup, pvcKind, "my-vol", &ns)

	var tests = map[string]struct {
		spec       core.PersistentVolumeClaimSpec
		oldSpec    core.PersistentVolumeClaimSpec
		anyEnabled bool
		xnsEnabled bool
		want       *core.TypedLocalObjectReference
		wantRef    *core.TypedObjectReference
	}{
		"empty ds": {
			spec: core.PersistentVolumeClaimSpec{},
		},
		"volume ds": {
			spec: core.PersistentVolumeClaimSpec{DataSource: volumeDataSource},
			want: volumeDataSource,
		},
		"volume ds ref": {
			spec:    core.PersistentVolumeClaimSpec{DataSourceRef: volumeDataSourceRef},
			wantRef: volumeDataSourceRef,
		},
		"both data sources": {
			spec:    core.PersistentVolumeClaimSpec{DataSource: volumeDataSource, DataSourceRef: volumeDataSourceRef},
			want:    volumeDataSource,
			wantRef: volumeDataSourceRef,
		},
		// 门控开启时跨命名空间引用应保留。
		"xns enabled with xns volume ds": {
			spec:       core.PersistentVolumeClaimSpec{DataSourceRef: xnsVolumeDataSourceRef},
			xnsEnabled: true,
			wantRef:    xnsVolumeDataSourceRef,
		},
		"xns enabled with xns volume ds when xns volume exists in oldSpec": {
			spec:       core.PersistentVolumeClaimSpec{DataSourceRef: xnsVolumeDataSourceRef},
			oldSpec:    core.PersistentVolumeClaimSpec{DataSourceRef: xnsVolumeDataSourceRef},
			xnsEnabled: true,
			wantRef:    xnsVolumeDataSourceRef,
		},
		// 门控关闭且旧 spec 未使用该字段：这是新建的跨命名空间引用，必须丢弃。
		"xns volume ds": {
			spec: core.PersistentVolumeClaimSpec{DataSourceRef: xnsVolumeDataSourceRef},
		},
		"xns volume ds when xns volume exists in oldSpec": {
			spec:    core.PersistentVolumeClaimSpec{DataSourceRef: xnsVolumeDataSourceRef},
			oldSpec: core.PersistentVolumeClaimSpec{DataSourceRef: xnsVolumeDataSourceRef},
			wantRef: xnsVolumeDataSourceRef, // 旧 spec 已包含该字段，不能因门控关闭而丢弃。
		},
		"xns volume ds when volume exists in oldSpec": {
			spec:    core.PersistentVolumeClaimSpec{DataSourceRef: xnsVolumeDataSourceRef},
			oldSpec: core.PersistentVolumeClaimSpec{DataSourceRef: volumeDataSourceRef},
			wantRef: xnsVolumeDataSourceRef, // 旧 spec 已有 DataSourceRef，整个字段都应保留。
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.CrossNamespaceVolumeDataSource: test.xnsEnabled,
			})
			DropDisabledFields(&test.spec, &test.oldSpec)
			if test.spec.DataSource != test.want {
				t.Errorf("expected condition was not met, test: %s, anyEnabled: %v, xnsEnabled: %v, spec: %+v, expected DataSource: %+v",
					testName, test.anyEnabled, test.xnsEnabled, test.spec, test.want)
			}
			if test.spec.DataSourceRef != test.wantRef {
				t.Errorf("expected condition was not met, test: %s, anyEnabled: %v, xnsEnabled: %v, spec: %+v, expected DataSourceRef: %+v",
					testName, test.anyEnabled, test.xnsEnabled, test.spec, test.wantRef)
			}
		})
	}
}

// TestDataSourceRef 验证 NormalizeDataSources 在 DataSource 与 DataSourceRef
// 之间的双向补齐：同命名空间引用会在两个字段间互相填充，跨命名空间引用只保留
// 在 DataSourceRef 中（DataSource 类型无法表达命名空间），避免数据被错误截断。
func TestDataSourceRef(t *testing.T) {
	ns := "ns1"
	volumeDataSource := makeDataSource(coreGroup, pvcKind, "my-vol")
	volumeDataSourceRef := makeDataSourceRef(coreGroup, pvcKind, "my-vol", nil)
	xnsVolumeDataSourceRef := makeDataSourceRef(coreGroup, pvcKind, "my-vol", &ns)
	snapshotDataSource := makeDataSource(snapGroup, snapKind, "my-snap")
	snapshotDataSourceRef := makeDataSourceRef(snapGroup, snapKind, "my-snap", nil)
	xnsSnapshotDataSourceRef := makeDataSourceRef(snapGroup, snapKind, "my-snap", &ns)
	genericDataSource := makeDataSource(genericGroup, genericKind, "my-foo")
	genericDataSourceRef := makeDataSourceRef(genericGroup, genericKind, "my-foo", nil)
	xnsGenericDataSourceRef := makeDataSourceRef(genericGroup, genericKind, "my-foo", &ns)
	coreDataSource := makeDataSource(coreGroup, podKind, "my-pod")
	coreDataSourceRef := makeDataSourceRef(coreGroup, podKind, "my-pod", nil)
	xnsCoreDataSourceRef := makeDataSourceRef(coreGroup, podKind, "my-pod", &ns)

	var tests = map[string]struct {
		spec    core.PersistentVolumeClaimSpec
		want    *core.TypedLocalObjectReference
		wantRef *core.TypedObjectReference
	}{
		"empty ds": {
			spec: core.PersistentVolumeClaimSpec{},
		},
		"volume ds": {
			spec:    core.PersistentVolumeClaimSpec{DataSource: volumeDataSource},
			want:    volumeDataSource,
			wantRef: volumeDataSourceRef,
		},
		"snapshot ds": {
			spec:    core.PersistentVolumeClaimSpec{DataSource: snapshotDataSource},
			want:    snapshotDataSource,
			wantRef: snapshotDataSourceRef,
		},
		"generic ds": {
			spec:    core.PersistentVolumeClaimSpec{DataSource: genericDataSource},
			want:    genericDataSource,
			wantRef: genericDataSourceRef,
		},
		"core ds": {
			spec:    core.PersistentVolumeClaimSpec{DataSource: coreDataSource},
			want:    coreDataSource,
			wantRef: coreDataSourceRef,
		},
		"volume ds ref": {
			spec:    core.PersistentVolumeClaimSpec{DataSourceRef: volumeDataSourceRef},
			want:    volumeDataSource,
			wantRef: volumeDataSourceRef,
		},
		"snapshot ds ref": {
			spec:    core.PersistentVolumeClaimSpec{DataSourceRef: snapshotDataSourceRef},
			want:    snapshotDataSource,
			wantRef: snapshotDataSourceRef,
		},
		"generic ds ref": {
			spec:    core.PersistentVolumeClaimSpec{DataSourceRef: genericDataSourceRef},
			want:    genericDataSource,
			wantRef: genericDataSourceRef,
		},
		"core ds ref": {
			spec:    core.PersistentVolumeClaimSpec{DataSourceRef: coreDataSourceRef},
			want:    coreDataSource,
			wantRef: coreDataSourceRef,
		},
		"xns volume ds ref": {
			spec:    core.PersistentVolumeClaimSpec{DataSourceRef: xnsVolumeDataSourceRef},
			wantRef: xnsVolumeDataSourceRef,
		},
		"xns snapshot ds ref": {
			spec:    core.PersistentVolumeClaimSpec{DataSourceRef: xnsSnapshotDataSourceRef},
			wantRef: xnsSnapshotDataSourceRef,
		},
		"xns generic ds ref": {
			spec:    core.PersistentVolumeClaimSpec{DataSourceRef: xnsGenericDataSourceRef},
			wantRef: xnsGenericDataSourceRef,
		},
		"xns core ds ref": {
			spec:    core.PersistentVolumeClaimSpec{DataSourceRef: xnsCoreDataSourceRef},
			wantRef: xnsCoreDataSourceRef,
		},
	}

	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.CrossNamespaceVolumeDataSource: true,
	})

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			NormalizeDataSources(&test.spec)
			if !reflect.DeepEqual(test.spec.DataSource, test.want) {
				t.Errorf("expected condition was not met, test: %s, spec.datasource: %+v, want: %+v",
					testName, test.spec.DataSource, test.want)
			}
			if !reflect.DeepEqual(test.spec.DataSourceRef, test.wantRef) {
				t.Errorf("expected condition was not met, test: %s, spec.datasourceRef: %+v, wantRef: %+v",
					testName, test.spec.DataSourceRef, test.wantRef)
			}
		})
	}
}

// TestDropDisabledVolumeAttributesClass 验证 VolumeAttributesClass 门控对 VAC 字段的裁剪：
// 门控关闭时只有旧 spec 已带 VAC 的更新请求才保留该字段；门控开启时一律保留；
// 这里通过切换到 1.35 之前的仿真版本，确保“特性未在旧版本使用”的判断生效。
func TestDropDisabledVolumeAttributesClass(t *testing.T) {
	vacName := ptr.To("foo")

	var tests = map[string]struct {
		spec       core.PersistentVolumeClaimSpec
		oldSpec    core.PersistentVolumeClaimSpec
		vacEnabled bool
		wantVAC    *string
	}{
		"vac disabled with empty vac": {
			spec: core.PersistentVolumeClaimSpec{},
		},
		"vac disabled with vac": {
			spec: core.PersistentVolumeClaimSpec{VolumeAttributesClassName: vacName},
		},
		"vac enabled with empty vac": {
			spec:       core.PersistentVolumeClaimSpec{},
			vacEnabled: true,
		},
		"vac enabled with vac": {
			spec:       core.PersistentVolumeClaimSpec{VolumeAttributesClassName: vacName},
			vacEnabled: true,
			wantVAC:    vacName,
		},
		"vac disabled with vac when vac doesn't exists in oldSpec": {
			spec:    core.PersistentVolumeClaimSpec{VolumeAttributesClassName: vacName},
			oldSpec: core.PersistentVolumeClaimSpec{},
		},
		"vac disabled with vac when vac exists in oldSpec": {
			spec:       core.PersistentVolumeClaimSpec{VolumeAttributesClassName: vacName},
			oldSpec:    core.PersistentVolumeClaimSpec{VolumeAttributesClassName: vacName},
			vacEnabled: false,
			wantVAC:    vacName,
		},
		"vac enabled with vac when vac doesn't exists in oldSpec": {
			spec:       core.PersistentVolumeClaimSpec{VolumeAttributesClassName: vacName},
			oldSpec:    core.PersistentVolumeClaimSpec{},
			vacEnabled: true,
			wantVAC:    vacName,
		},
		"vac enable with vac when vac exists in oldSpec": {
			spec:       core.PersistentVolumeClaimSpec{VolumeAttributesClassName: vacName},
			oldSpec:    core.PersistentVolumeClaimSpec{VolumeAttributesClassName: vacName},
			vacEnabled: true,
			wantVAC:    vacName,
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			if !test.vacEnabled {
				// 仿真版本低于 VAC 引入版本，门控关闭时才会真正裁剪该字段。
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeAttributesClass, test.vacEnabled)
			DropDisabledFields(&test.spec, &test.oldSpec)
			if test.spec.VolumeAttributesClassName != test.wantVAC {
				t.Errorf("expected vac was not met, test: %s, vacEnabled: %v, spec: %+v, expected VAC: %+v",
					testName, test.vacEnabled, test.spec, test.wantVAC)
			}
		})
	}
}

// TestDropDisabledFieldsFromStatus 验证 DropDisabledFieldsFromStatus 按特性门控
// 和旧对象状态裁剪 PVC status 中的新字段（AllocatedResources、ResizeStatus、
// VolumeAttributesClassName、ModifyVolumeStatus、HealthStatus）：
// 创建请求在门控关闭时丢弃，更新请求中旧对象已使用该字段则保留，避免破坏既有对象。
func TestDropDisabledFieldsFromStatus(t *testing.T) {
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.33"))
	tests := []struct {
		name                                string
		enableRecoverVolumeExpansionFailure bool
		enableVolumeAttributesClass         bool
		enableCSIVolumeHealth               bool
		pvc                                 *core.PersistentVolumeClaim
		oldPVC                              *core.PersistentVolumeClaim
		expected                            *core.PersistentVolumeClaim
	}{
		// 门控关闭且旧对象未使用该字段：创建路径必须丢弃 AllocatedResources。
		{
			name:                                "for:newPVC=hasAllocatedResource,oldPVC=doesnot,featuregate=false; should drop field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         false,
			pvc:                                 withAllocatedResource("5G"),
			oldPVC:                              getPVC(),
			expected:                            getPVC(),
		},
		{
			name:                                "for:newPVC=hasAllocatedResource,oldPVC=doesnot,featuregate=RecoverVolumeExpansionFailure=true; should keep field",
			enableRecoverVolumeExpansionFailure: true,
			enableVolumeAttributesClass:         false,
			pvc:                                 withAllocatedResource("5G"),
			oldPVC:                              getPVC(),
			expected:                            withAllocatedResource("5G"),
		},
		{
			name:                                "for:newPVC=hasAllocatedResource,oldPVC=hasAllocatedResource,featuregate=RecoverVolumeExpansionFailure=true; should keep field",
			enableRecoverVolumeExpansionFailure: true,
			enableVolumeAttributesClass:         false,
			pvc:                                 withAllocatedResource("5G"),
			oldPVC:                              withAllocatedResource("5G"),
			expected:                            withAllocatedResource("5G"),
		},
		// 旧对象已有该字段，即使门控关闭也要保留，避免升级后字段突然消失。
		{
			name:                                "for:newPVC=hasAllocatedResource,oldPVC=hasAllocatedResource,featuregate=false; should keep field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         false,
			pvc:                                 withAllocatedResource("10G"),
			oldPVC:                              withAllocatedResource("5G"),
			expected:                            withAllocatedResource("10G"),
		},
		{
			name:                                "for:newPVC=hasAllocatedResource,oldPVC=nil,featuregate=false; should drop field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         false,
			pvc:                                 withAllocatedResource("5G"),
			oldPVC:                              nil,
			expected:                            getPVC(),
		},
		{
			name:                                "for:newPVC=hasResizeStatus,oldPVC=nil, featuregate=false should drop field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         false,
			pvc:                                 withResizeStatus(core.PersistentVolumeClaimNodeResizeInfeasible),
			oldPVC:                              nil,
			expected:                            getPVC(),
		},
		{
			name:                                "for:newPVC=hasResizeStatus,oldPVC=doesnot,featuregate=RecoverVolumeExpansionFailure=true; should keep field",
			enableRecoverVolumeExpansionFailure: true,
			enableVolumeAttributesClass:         false,
			pvc:                                 withResizeStatus(core.PersistentVolumeClaimNodeResizeInfeasible),
			oldPVC:                              getPVC(),
			expected:                            withResizeStatus(core.PersistentVolumeClaimNodeResizeInfeasible),
		},
		{
			name:                                "for:newPVC=hasResizeStatus,oldPVC=hasResizeStatus,featuregate=RecoverVolumeExpansionFailure=true; should keep field",
			enableRecoverVolumeExpansionFailure: true,
			enableVolumeAttributesClass:         false,
			pvc:                                 withResizeStatus(core.PersistentVolumeClaimNodeResizeInfeasible),
			oldPVC:                              withResizeStatus(core.PersistentVolumeClaimNodeResizeInfeasible),
			expected:                            withResizeStatus(core.PersistentVolumeClaimNodeResizeInfeasible),
		},
		{
			name:                                "for:newPVC=hasResizeStatus,oldPVC=hasResizeStatus,featuregate=false; should keep field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         false,
			pvc:                                 withResizeStatus(core.PersistentVolumeClaimNodeResizeInfeasible),
			oldPVC:                              withResizeStatus(core.PersistentVolumeClaimNodeResizeInfeasible),
			expected:                            withResizeStatus(core.PersistentVolumeClaimNodeResizeInfeasible),
		},
		{
			name:                                "for:newPVC=hasVolumeAttributeClass,oldPVC=nil, featuregate=false should drop field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         false,
			pvc:                                 withVolumeAttributesClassName("foo"),
			oldPVC:                              nil,
			expected:                            getPVC(),
		},
		{
			name:                                "for:newPVC=hasVolumeAttributeClass,oldPVC=doesnot,featuregate=VolumeAttributesClass=true; should keep field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         true,
			pvc:                                 withVolumeAttributesClassName("foo"),
			oldPVC:                              getPVC(),
			expected:                            withVolumeAttributesClassName("foo"),
		},
		{
			name:                                "for:newPVC=hasVolumeAttributeClass,oldPVC=hasVolumeAttributeClass,featuregate=VolumeAttributesClass=true; should keep field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         true,
			pvc:                                 withVolumeAttributesClassName("foo"),
			oldPVC:                              withVolumeAttributesClassName("foo"),
			expected:                            withVolumeAttributesClassName("foo"),
		},
		{
			name:                                "for:newPVC=hasVolumeAttributeClass,oldPVC=hasVolumeAttributeClass,featuregate=false; should keep field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         false,
			pvc:                                 withVolumeAttributesClassName("foo"),
			oldPVC:                              withVolumeAttributesClassName("foo"),
			expected:                            withVolumeAttributesClassName("foo"),
		},
		{
			name:                                "for:newPVC=hasVolumeAttributesModifyStatus,oldPVC=nil, featuregate=false should drop field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         false,
			pvc:                                 withVolumeAttributesModifyStatus("bar", core.PersistentVolumeClaimModifyVolumePending),
			oldPVC:                              nil,
			expected:                            getPVC(),
		},
		{
			name:                                "for:newPVC=hasVolumeAttributesModifyStatus,oldPVC=doesnot,featuregate=VolumeAttributesClass=true; should keep field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         true,
			pvc:                                 withVolumeAttributesModifyStatus("bar", core.PersistentVolumeClaimModifyVolumePending),
			oldPVC:                              getPVC(),
			expected:                            withVolumeAttributesModifyStatus("bar", core.PersistentVolumeClaimModifyVolumePending),
		},
		{
			name:                                "for:newPVC=hasVolumeAttributesModifyStatus,oldPVC=hasVolumeAttributesModifyStatus,featuregate=VolumeAttributesClass=true; should keep field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         true,
			pvc:                                 withVolumeAttributesModifyStatus("bar", core.PersistentVolumeClaimModifyVolumePending),
			oldPVC:                              withVolumeAttributesModifyStatus("bar", core.PersistentVolumeClaimModifyVolumePending),
			expected:                            withVolumeAttributesModifyStatus("bar", core.PersistentVolumeClaimModifyVolumePending),
		},
		{
			name:                                "for:newPVC=hasVolumeAttributesModifyStatus,oldPVC=hasVolumeAttributesModifyStatus,featuregate=false; should keep field",
			enableRecoverVolumeExpansionFailure: false,
			enableVolumeAttributesClass:         false,
			pvc:                                 withVolumeAttributesModifyStatus("bar", core.PersistentVolumeClaimModifyVolumePending),
			oldPVC:                              withVolumeAttributesModifyStatus("bar", core.PersistentVolumeClaimModifyVolumePending),
			expected:                            withVolumeAttributesModifyStatus("bar", core.PersistentVolumeClaimModifyVolumePending),
		},
		// HealthStatus 由独立门控控制：门控关闭且旧对象不存在时必须丢弃。
		{
			name:                  "for:newPVC=hasHealthStatus,oldPVC=nil,featuregate=CSIVolumeHealth=false; should drop field",
			enableCSIVolumeHealth: false,
			pvc:                   withHealthStatus(),
			oldPVC:                nil,
			expected:              getPVC(),
		},
		{
			name:                  "for:newPVC=hasHealthStatus,oldPVC=doesnot,featuregate=CSIVolumeHealth=false; should drop field",
			enableCSIVolumeHealth: false,
			pvc:                   withHealthStatus(),
			oldPVC:                getPVC(),
			expected:              getPVC(),
		},
		{
			name:                  "for:newPVC=hasHealthStatus,oldPVC=hasHealthStatus,featuregate=CSIVolumeHealth=false; should keep field",
			enableCSIVolumeHealth: false,
			pvc:                   withHealthStatus(),
			oldPVC:                withHealthStatus(),
			expected:              withHealthStatus(),
		},
		{
			name:                  "for:newPVC=hasHealthStatus,oldPVC=nil,featuregate=CSIVolumeHealth=true; should keep field",
			enableCSIVolumeHealth: true,
			pvc:                   withHealthStatus(),
			oldPVC:                nil,
			expected:              withHealthStatus(),
		},
		{
			name:                  "for:newPVC=hasHealthStatus,oldPVC=doesnot,featuregate=CSIVolumeHealth=true; should keep field",
			enableCSIVolumeHealth: true,
			pvc:                   withHealthStatus(),
			oldPVC:                getPVC(),
			expected:              withHealthStatus(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.RecoverVolumeExpansionFailure: test.enableRecoverVolumeExpansionFailure,
				features.VolumeAttributesClass:         test.enableVolumeAttributesClass,
				features.CSIVolumeHealth:               test.enableCSIVolumeHealth,
			})

			DropDisabledFieldsFromStatus(test.pvc, test.oldPVC)

			if !reflect.DeepEqual(*test.expected, *test.pvc) {
				t.Errorf("Unexpected change: %+v", cmp.Diff(test.expected, test.pvc))
			}
		})
	}
}

// getPVC 返回不含任何 status 字段的空 PVC，作为裁剪结果的基准。
func getPVC() *core.PersistentVolumeClaim {
	return &core.PersistentVolumeClaim{}
}

// withAllocatedResource 构造带 AllocatedResources 的 PVC，用于检查扩容恢复相关字段的裁剪。
func withAllocatedResource(q string) *core.PersistentVolumeClaim {
	return &core.PersistentVolumeClaim{
		Status: core.PersistentVolumeClaimStatus{
			AllocatedResources: core.ResourceList{
				core.ResourceStorage: resource.MustParse(q),
			},
		},
	}
}

// withResizeStatus 构造带 AllocatedResourceStatuses 的 PVC，用于检查节点扩容状态字段的裁剪。
func withResizeStatus(status core.ClaimResourceStatus) *core.PersistentVolumeClaim {
	return &core.PersistentVolumeClaim{
		Status: core.PersistentVolumeClaimStatus{
			AllocatedResourceStatuses: map[core.ResourceName]core.ClaimResourceStatus{
				core.ResourceStorage: status,
			},
		},
	}
}

// withVolumeAttributesClassName 构造带 CurrentVolumeAttributesClassName 的 PVC。
func withVolumeAttributesClassName(vacName string) *core.PersistentVolumeClaim {
	return &core.PersistentVolumeClaim{
		Status: core.PersistentVolumeClaimStatus{
			CurrentVolumeAttributesClassName: &vacName,
		},
	}
}

// withHealthStatus 构造带 HealthStatus 的 PVC，用于检查 CSI 卷健康字段的裁剪。
func withHealthStatus() *core.PersistentVolumeClaim {
	return &core.PersistentVolumeClaim{
		Status: core.PersistentVolumeClaimStatus{
			HealthStatus: &core.VolumeHealthStatus{
				HealthConditions: []core.VolumeHealthCondition{
					{
						Status: core.VolumeHealthDegraded,
						Reason: "DiskSlow",
					},
				},
			},
		},
	}
}

// withVolumeAttributesModifyStatus 构造带 ModifyVolumeStatus 的 PVC，
// 用于检查 VAC 修改状态字段的裁剪。
func withVolumeAttributesModifyStatus(target string, status core.PersistentVolumeClaimModifyVolumeStatus) *core.PersistentVolumeClaim {
	return &core.PersistentVolumeClaim{
		Status: core.PersistentVolumeClaimStatus{
			ModifyVolumeStatus: &core.ModifyVolumeStatus{
				TargetVolumeAttributesClassName: target,
				Status:                          status,
			},
		},
	}
}

// TestWarnings 验证 GetWarningsForPersistentVolumeClaim 的告警内容：
// nil PVC 与整数大小的资源请求不告警；storage 请求/限制中的小数字节值会分别告警；
// 使用已弃用的 beta storage class 注解时给出迁移提示。
func TestWarnings(t *testing.T) {
	testcases := []struct {
		name     string
		template *core.PersistentVolumeClaim
		expected []string
	}{
		{
			name:     "null",
			template: nil,
			expected: nil,
		},
		{
			name: "200Mi requests no warning",
			template: &core.PersistentVolumeClaim{
				Spec: core.PersistentVolumeClaimSpec{
					Resources: core.VolumeResourceRequirements{
						Requests: core.ResourceList{
							core.ResourceStorage: resource.MustParse("200Mi"),
						},
						Limits: core.ResourceList{
							core.ResourceStorage: resource.MustParse("200Mi"),
						},
					},
				},
			},
			expected: nil,
		},
		{
			name: "200m warning",
			template: &core.PersistentVolumeClaim{
				Spec: core.PersistentVolumeClaimSpec{
					Resources: core.VolumeResourceRequirements{
						Requests: core.ResourceList{
							core.ResourceStorage: resource.MustParse("200m"),
						},
						Limits: core.ResourceList{
							core.ResourceStorage: resource.MustParse("100m"),
						},
					},
				},
			},
			expected: []string{
				`spec.resources.requests[storage]: fractional byte value "200m" is invalid, must be an integer`,
				`spec.resources.limits[storage]: fractional byte value "100m" is invalid, must be an integer`,
			},
		},
		{
			name: "integer no warning",
			template: &core.PersistentVolumeClaim{
				Spec: core.PersistentVolumeClaimSpec{
					Resources: core.VolumeResourceRequirements{
						Requests: core.ResourceList{
							core.ResourceStorage: resource.MustParse("200"),
						},
					},
				},
			},
			expected: nil,
		},
		{
			name: "storageclass annotations warning",
			template: &core.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
					Annotations: map[string]string{
						core.BetaStorageClassAnnotation: "",
					},
				},
			},
			expected: []string{
				`metadata.annotations[volume.beta.kubernetes.io/storage-class]: deprecated since v1.8; use "storageClassName" attribute instead`,
			},
		},
	}

	for _, tc := range testcases {
		t.Run("pvcspec_"+tc.name, func(t *testing.T) {
			actual := sets.New[string](GetWarningsForPersistentVolumeClaim(tc.template)...)
			expected := sets.New[string](tc.expected...)
			for _, missing := range sets.List[string](expected.Difference(actual)) {
				t.Errorf("missing: %s", missing)
			}
			for _, extra := range sets.List[string](actual.Difference(expected)) {
				t.Errorf("extra: %s", extra)
			}
		})

	}
}
