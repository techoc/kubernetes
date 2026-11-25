/*
Copyright 2015 The Kubernetes Authors.

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
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	apitesting "k8s.io/kubernetes/pkg/api/testing"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/features"

	// 确保类型已注册
	_ "k8s.io/kubernetes/pkg/apis/core/install"
)

func TestSelectableFieldLabelConversions(t *testing.T) {
	apitesting.TestSelectableFieldLabelConversionsOfKind(t,
		"v1",
		"PersistentVolumeClaim",
		PersistentVolumeClaimToSelectableFields(&api.PersistentVolumeClaim{}),
		map[string]string{"name": "metadata.name"},
	)
}

// TestDropConditions 验证 PVC 更新时旧条件的保留/清理逻辑，避免客户端误删服务端维护的状态条件。
func TestDropConditions(t *testing.T) {
	ctx := genericapirequest.NewDefaultContext()
	pvcWithConditions := func() *api.PersistentVolumeClaim {
		return &api.PersistentVolumeClaim{
			Status: api.PersistentVolumeClaimStatus{
				Conditions: []api.PersistentVolumeClaimCondition{
					{Type: api.PersistentVolumeClaimResizing, Status: api.ConditionTrue},
				},
			},
		}
	}
	pvcWithoutConditions := func() *api.PersistentVolumeClaim {
		return &api.PersistentVolumeClaim{
			Status: api.PersistentVolumeClaimStatus{},
		}
	}

	pvcInfo := []struct {
		description   string
		hasConditions bool
		pvc           func() *api.PersistentVolumeClaim
	}{
		{
			description:   "has Conditions",
			hasConditions: true,
			pvc:           pvcWithConditions,
		},
		{
			description:   "does not have Conditions",
			hasConditions: false,
			pvc:           pvcWithoutConditions,
		},
	}

	for _, oldPvcInfo := range pvcInfo {
		for _, newPvcInfo := range pvcInfo {
			oldPvcHasConditins, oldPvc := oldPvcInfo.hasConditions, oldPvcInfo.pvc()
			newPvcHasConditions, newPvc := newPvcInfo.hasConditions, newPvcInfo.pvc()

			t.Run(fmt.Sprintf("old pvc %s, new pvc %s", oldPvcInfo.description, newPvcInfo.description), func(t *testing.T) {
				StatusStrategy.PrepareForUpdate(ctx, newPvc, oldPvc)

				// 旧 pvc 不应该被修改
				if !reflect.DeepEqual(oldPvc, oldPvcInfo.pvc()) {
					t.Errorf("old pvc changed: %v", cmp.Diff(oldPvc, oldPvcInfo.pvc()))
				}

				switch {
				case oldPvcHasConditins || newPvcHasConditions:
					// 如果特性门控已开启，或旧 pvc 已带有 Conditions，则新 pvc 保持不变
					if !reflect.DeepEqual(newPvc, newPvcInfo.pvc()) {
						t.Errorf("new pvc changed: %v", cmp.Diff(newPvc, newPvcInfo.pvc()))
					}
				default:
					// 新 pvc 无需改动
					if !reflect.DeepEqual(newPvc, newPvcInfo.pvc()) {
						t.Errorf("new pvc changed: %v", cmp.Diff(newPvc, newPvcInfo.pvc()))
					}
				}
			})
		}
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

func makeDataSource(apiGroup, kind, name string) *api.TypedLocalObjectReference {
	return &api.TypedLocalObjectReference{
		APIGroup: &apiGroup,
		Kind:     kind,
		Name:     name,
	}

}

func makeDataSourceRef(apiGroup, kind, name string, namespace *string) *api.TypedObjectReference {
	return &api.TypedObjectReference{
		APIGroup:  &apiGroup,
		Kind:      kind,
		Name:      name,
		Namespace: namespace,
	}
}

// TestPrepareForCreate 覆盖 dataSource/dataSourceRef 单边填写、历史无效来源值清理及规范化结果。
func TestPrepareForCreate(t *testing.T) {
	ctx := genericapirequest.NewDefaultContext()

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
		xnsEnabled    bool
		dataSource    *api.TypedLocalObjectReference
		dataSourceRef *api.TypedObjectReference
		want          *api.TypedLocalObjectReference
		wantRef       *api.TypedObjectReference
	}{
		"empty ds": {
			want: nil,
		},
		"volume ds": {
			dataSource: volumeDataSource,
			want:       volumeDataSource,
			wantRef:    volumeDataSourceRef,
		},
		"snapshot ds": {
			dataSource: snapshotDataSource,
			want:       snapshotDataSource,
			wantRef:    snapshotDataSourceRef,
		},
		"generic ds": {
			dataSource: genericDataSource,
		},
		"invalid ds": {
			dataSource: coreDataSource,
		},
		"volume ds ref": {
			dataSourceRef: volumeDataSourceRef,
			want:          volumeDataSource,
			wantRef:       volumeDataSourceRef,
		},
		"snapshot ds ref": {
			dataSourceRef: snapshotDataSourceRef,
			want:          snapshotDataSource,
			wantRef:       snapshotDataSourceRef,
		},
		"generic ds ref": {
			dataSourceRef: genericDataSourceRef,
			want:          genericDataSource,
			wantRef:       genericDataSourceRef,
		},
		"invalid ds ref": {
			dataSourceRef: coreDataSourceRef,
			want:          coreDataSource,
			wantRef:       coreDataSourceRef,
		},
		"mismatched data sources": {
			dataSource:    volumeDataSource,
			dataSourceRef: snapshotDataSourceRef,
			want:          volumeDataSource,
			wantRef:       snapshotDataSourceRef,
		},
		"xns enabled with empty ds": {
			xnsEnabled: true,
			want:       nil,
		},
		"xns enabled with volume ds": {
			dataSource: volumeDataSource,
			xnsEnabled: true,
			want:       volumeDataSource,
			wantRef:    volumeDataSourceRef,
		},
		"xns enabled with snapshot ds": {
			dataSource: snapshotDataSource,
			xnsEnabled: true,
			want:       snapshotDataSource,
			wantRef:    snapshotDataSourceRef,
		},
		"xns enabled with generic ds": {
			dataSource: genericDataSource,
			xnsEnabled: true,
		},
		"xns enabled with invalid ds": {
			dataSource: coreDataSource,
			xnsEnabled: true,
		},
		"xns enabled with volume ds ref": {
			dataSourceRef: volumeDataSourceRef,
			xnsEnabled:    true,
			want:          volumeDataSource,
			wantRef:       volumeDataSourceRef,
		},
		"xns enabled with snapshot ds ref": {
			dataSourceRef: snapshotDataSourceRef,
			xnsEnabled:    true,
			want:          snapshotDataSource,
			wantRef:       snapshotDataSourceRef,
		},
		"xns enabled with generic ds ref": {
			dataSourceRef: genericDataSourceRef,
			xnsEnabled:    true,
			want:          genericDataSource,
			wantRef:       genericDataSourceRef,
		},
		"xns enabled with invalid ds ref": {
			dataSourceRef: coreDataSourceRef,
			xnsEnabled:    true,
			want:          coreDataSource,
			wantRef:       coreDataSourceRef,
		},
		"xns enabled with mismatched data sources": {
			dataSource:    volumeDataSource,
			dataSourceRef: snapshotDataSourceRef,
			xnsEnabled:    true,
			want:          volumeDataSource,
			wantRef:       snapshotDataSourceRef,
		},
		"xns enabled with volume xns ds ref": {
			dataSourceRef: xnsVolumeDataSourceRef,
			xnsEnabled:    true,
			wantRef:       xnsVolumeDataSourceRef,
		},
		"xns enabled with snapshot xns ds ref": {
			dataSourceRef: xnsSnapshotDataSourceRef,
			xnsEnabled:    true,
			wantRef:       xnsSnapshotDataSourceRef,
		},
		"xns enabled with generic xns ds ref": {
			dataSourceRef: xnsGenericDataSourceRef,
			xnsEnabled:    true,
			wantRef:       xnsGenericDataSourceRef,
		},
		"xns enabled with invalid xns ds ref": {
			dataSourceRef: xnsCoreDataSourceRef,
			xnsEnabled:    true,
			wantRef:       xnsCoreDataSourceRef,
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.CrossNamespaceVolumeDataSource: test.xnsEnabled,
			})
			pvc := api.PersistentVolumeClaim{
				Spec: api.PersistentVolumeClaimSpec{
					DataSource:    test.dataSource,
					DataSourceRef: test.dataSourceRef,
				},
			}

			// 被测方法
			Strategy.PrepareForCreate(ctx, &pvc)

			if !reflect.DeepEqual(pvc.Spec.DataSource, test.want) {
				t.Errorf("data source does not match, test: %s, dataSource: %v, expected: %v",
					testName, test.dataSource, test.want)
			}
			if !reflect.DeepEqual(pvc.Spec.DataSourceRef, test.wantRef) {
				t.Errorf("data source ref does not match, test: %s, dataSourceRef: %v, expected: %v",
					testName, test.dataSourceRef, test.wantRef)
			}
		})
	}
}
