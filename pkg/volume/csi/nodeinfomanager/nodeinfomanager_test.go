/*
Copyright 2018 The Kubernetes Authors.

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

package nodeinfomanager

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	utiltesting "k8s.io/client-go/util/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	"k8s.io/kubernetes/pkg/features"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/utils/ptr"
)

// testcase 描述 Install/Uninstall CSIDriver 的一个测试用例及其期望结果。
type testcase struct {
	name             string
	driverName       string
	existingNode     *v1.Node
	existingCSINode  *storage.CSINode
	inputNodeID      string
	inputTopology    map[string]string
	inputVolumeLimit int64
	expectedNode     *v1.Node
	expectedCSINode  *storage.CSINode
	expectFail       bool
	hasModified      bool
	migratedPlugins  map[string](func() bool)
}

// nodeIDMap 表示驱动名称到节点 ID 的映射。
type nodeIDMap map[string]string

// topologyKeyMap 表示驱动名称到拓扑键列表的映射。
type topologyKeyMap map[string][]string

// labelMap 表示节点标签的键值对集合。
type labelMap map[string]string

// TestInstallCSIDriver 用各种已存在的 Node 和/或 CSINode 对象测试 InstallCSIDriver。
// 以下各用例中，Node 注解与 CSINode 里的节点 ID 保持一致。
func TestInstallCSIDriver(t *testing.T) {
	testcases := []testcase{
		{
			name:         "empty node",
			driverName:   "com.example.csi.driver1",
			existingNode: generateNode(nil /* nodeIDs */, nil /* labels */, nil /*capacity*/),
			inputNodeID:  "com.example.csi/csi-node1",
			inputTopology: map[string]string{
				"com.example.csi/zone": "zoneA",
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/csi-node1"})},
					Labels:      labelMap{"com.example.csi/zone": "zoneA"},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: []string{"com.example.csi/zone"},
						},
					},
				},
			},
		},
		{
			name:       "pre-existing node info from the same driver",
			driverName: "com.example.csi.driver1",
			existingNode: generateNode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				labelMap{
					"com.example.csi/zone": "zoneA",
				},
				nil /*capacity*/),
			existingCSINode: generateCSINode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				nil, /* volumeLimits */
				topologyKeyMap{
					"com.example.csi.driver1": {"com.example.csi/zone"},
				},
			),
			inputNodeID: "com.example.csi/csi-node1",
			inputTopology: map[string]string{
				"com.example.csi/zone": "zoneA",
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/csi-node1"})},
					Labels:      labelMap{"com.example.csi/zone": "zoneA"},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: []string{"com.example.csi/zone"},
							Allocatable:  nil,
						},
					},
				},
			},
		},
		{
			name:       "pre-existing node info from the same driver, but without topology info",
			driverName: "com.example.csi.driver1",
			existingNode: generateNode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				nil /* labels */, nil /*capacity*/),
			existingCSINode: generateCSINode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				nil, /* volumeLimits */
				nil, /* topologyKeys */
			),
			inputNodeID: "com.example.csi/csi-node1",
			inputTopology: map[string]string{
				"com.example.csi/zone": "zoneA",
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/csi-node1"})},
					Labels:      labelMap{"com.example.csi/zone": "zoneA"},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: []string{"com.example.csi/zone"},
							Allocatable:  nil,
						},
					},
				},
			},
		},
		{
			name:       "pre-existing node info from different driver",
			driverName: "com.example.csi.driver1",
			existingNode: generateNode(
				nodeIDMap{
					"net.example.storage.other-driver": "net.example.storage/test-node",
				},
				labelMap{
					"net.example.storage/rack": "rack1",
				}, nil /*capacity*/),
			existingCSINode: generateCSINode(
				nodeIDMap{
					"net.example.storage.other-driver": "net.example.storage/test-node",
				},
				nil, /* volumeLimits */
				topologyKeyMap{
					"net.example.storage.other-driver": {"net.example.storage/rack"},
				},
			),
			inputNodeID: "com.example.csi/csi-node1",
			inputTopology: map[string]string{
				"com.example.csi/zone": "zoneA",
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{
						"com.example.csi.driver1":          "com.example.csi/csi-node1",
						"net.example.storage.other-driver": "net.example.storage/test-node",
					})},
					Labels: labelMap{
						"com.example.csi/zone":     "zoneA",
						"net.example.storage/rack": "rack1",
					},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "net.example.storage.other-driver",
							NodeID:       "net.example.storage/test-node",
							TopologyKeys: []string{"net.example.storage/rack"},
							Allocatable:  nil,
						},
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: []string{"com.example.csi/zone"},
							Allocatable:  nil,
						},
					},
				},
			},
		},
		{
			name:       "pre-existing node info from the same driver, but different node ID and topology values; labels should conflict",
			driverName: "com.example.csi.driver1",
			existingNode: generateNode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				labelMap{
					"com.example.csi/zone": "zoneA",
				}, nil /*capacity*/),
			existingCSINode: generateCSINode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				nil, /* volumeLimits */
				topologyKeyMap{
					"com.example.csi.driver1": {"com.example.csi/zone"},
				},
			),
			inputNodeID: "com.example.csi/csi-node1",
			inputTopology: map[string]string{
				"com.example.csi/zone": "other-zone",
			},
			expectFail: true,
		},
		{
			name:       "pre-existing node info from the same driver, but different node ID and topology keys; new labels should be added",
			driverName: "com.example.csi.driver1",
			existingNode: generateNode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				labelMap{
					"com.example.csi/zone": "zoneA",
				}, nil /*capacity*/),
			existingCSINode: generateCSINode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				nil, /* volumeLimits */
				topologyKeyMap{
					"com.example.csi.driver1": {"com.example.csi/zone"},
				},
			),
			inputNodeID: "com.example.csi/other-node",
			inputTopology: map[string]string{
				"com.example.csi/rack": "rack1",
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/other-node"})},
					Labels: labelMap{
						"com.example.csi/zone": "zoneA",
						"com.example.csi/rack": "rack1",
					},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/other-node",
							TopologyKeys: []string{"com.example.csi/rack"},
							Allocatable:  nil,
						},
					},
				},
			},
		},
		{
			name: "pre-existing node info, but owned by previous node",
			existingNode: func() *v1.Node {
				node := generateNode(nil /*nodeIDs*/, nil /*labels*/, nil /*capacity*/)
				node.UID = types.UID("node1")
				return node
			}(),
			existingCSINode: func() *storage.CSINode {
				csiNode := generateCSINode(nil /*nodeIDs*/, nil /*volumeLimits*/, nil /*topologyKeys*/)
				csiNode.OwnerReferences[0].UID = types.UID("node2")
				return csiNode
			}(),
			migratedPlugins: map[string](func() bool){
				"com.example.csi.driver1": func() bool { return true },
			},
			inputNodeID: "com.example.csi/csi-node1",
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					UID:         types.UID("node1"),
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"": "com.example.csi/csi-node1"})},
				},
			},
			expectedCSINode: func() *storage.CSINode {
				csiNode := &storage.CSINode{
					ObjectMeta: getCSINodeObjectMeta(),
					Spec: storage.CSINodeSpec{
						Drivers: []storage.CSINodeDriver{
							{
								NodeID: "com.example.csi/csi-node1",
							},
						},
					},
				}
				csiNode.Annotations = map[string]string{v1.MigratedPluginsAnnotationKey: "com.example.csi.driver1"}
				return csiNode
			}(),
		},
		{
			name: "pre-existing node info with driver, but owned by previous node",
			existingNode: func() *v1.Node {
				node := generateNode(nil /*nodeIDs*/, nil /*labels*/, nil /*capacity*/)
				node.UID = types.UID("node1")
				return node
			}(),
			existingCSINode: func() *storage.CSINode {
				csiNode := generateCSINode(
					nodeIDMap{
						"com.example.csi.old-driver": "com.example.csi/csi-node2",
					},
					nil /*volumeLimits*/, nil, /*topologyKeys*/
				)
				csiNode.OwnerReferences[0].UID = types.UID("node2")
				return csiNode
			}(),
			driverName:  "com.example.csi.driver1",
			inputNodeID: "com.example.csi/csi-node1",
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					UID:         types.UID("node1"),
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/csi-node1"})},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							// 旧 CSINode 属于之前的节点，因此这里只应保留新驱动。
							Name:   "com.example.csi.driver1",
							NodeID: "com.example.csi/csi-node1",
						},
					},
				},
			},
		},
		{
			name:          "nil topology, empty node",
			driverName:    "com.example.csi.driver1",
			existingNode:  generateNode(nil /* nodeIDs */, nil /* labels */, nil /*capacity*/),
			inputNodeID:   "com.example.csi/csi-node1",
			inputTopology: nil,
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/csi-node1"})},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: nil,
							Allocatable:  nil,
						},
					},
				},
			},
		},
		{
			name:       "nil topology, pre-existing node info from the same driver",
			driverName: "com.example.csi.driver1",
			existingNode: generateNode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				labelMap{
					"com.example.csi/zone": "zoneA",
				}, nil /*capacity*/),
			existingCSINode: generateCSINode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				nil, /* volumeLimits */
				topologyKeyMap{
					"com.example.csi.driver1": {"com.example.csi/zone"},
				},
			),
			inputNodeID:   "com.example.csi/csi-node1",
			inputTopology: nil,
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/csi-node1"})},
					Labels: labelMap{
						"com.example.csi/zone": "zoneA",
					},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: nil,
							Allocatable:  nil,
						},
					},
				},
			},
		},
		{
			name:       "nil topology, pre-existing node info from different driver",
			driverName: "com.example.csi.driver1",
			existingNode: generateNode(
				nodeIDMap{
					"net.example.storage.other-driver": "net.example.storage/test-node",
				},
				labelMap{
					"net.example.storage/rack": "rack1",
				}, nil /*capacity*/),
			existingCSINode: generateCSINode(
				nodeIDMap{
					"net.example.storage.other-driver": "net.example.storage/test-node",
				},
				nil, /* volumeLimits */
				topologyKeyMap{
					"net.example.storage.other-driver": {"net.example.storage/rack"},
				},
			),
			inputNodeID:   "com.example.csi/csi-node1",
			inputTopology: nil,
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{
						"com.example.csi.driver1":          "com.example.csi/csi-node1",
						"net.example.storage.other-driver": "net.example.storage/test-node",
					})},
					Labels: labelMap{
						"net.example.storage/rack": "rack1",
					},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "net.example.storage.other-driver",
							NodeID:       "net.example.storage/test-node",
							TopologyKeys: []string{"net.example.storage/rack"},
							Allocatable:  nil,
						},
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: nil,
							Allocatable:  nil,
						},
					},
				},
			},
		},
		{
			name:         "empty node ID",
			driverName:   "com.example.csi.driver1",
			existingNode: generateNode(nil /* nodeIDs */, nil /* labels */, nil /*capacity*/),
			inputNodeID:  "",
			expectFail:   true,
		},
		{
			name:             "new node with valid max limit of volumes",
			driverName:       "com.example.csi.driver1",
			existingNode:     generateNode(nil /*nodeIDs*/, nil /*labels*/, nil /*capacity*/),
			inputVolumeLimit: 10,
			inputTopology:    nil,
			inputNodeID:      "com.example.csi/csi-node1",
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/csi-node1"})},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: nil,
							Allocatable: &storage.VolumeNodeResources{
								Count: ptr.To[int32](10),
							},
						},
					},
				},
			},
		},
		{
			name:             "new node with max limit of volumes",
			driverName:       "com.example.csi.driver1",
			existingNode:     generateNode(nil /*nodeIDs*/, nil /*labels*/, nil /*capacity*/),
			inputVolumeLimit: math.MaxInt32,
			inputTopology:    nil,
			inputNodeID:      "com.example.csi/csi-node1",
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/csi-node1"})},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: nil,
							Allocatable: &storage.VolumeNodeResources{
								Count: ptr.To[int32](math.MaxInt32),
							},
						},
					},
				},
			},
		},
		{
			name:             "new node with overflown max limit of volumes",
			driverName:       "com.example.csi.driver1",
			existingNode:     generateNode(nil /*nodeIDs*/, nil /*labels*/, nil /*capacity*/),
			inputVolumeLimit: math.MaxInt32 + 1,
			inputTopology:    nil,
			inputNodeID:      "com.example.csi/csi-node1",
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/csi-node1"})},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: nil,
							Allocatable: &storage.VolumeNodeResources{
								Count: ptr.To[int32](math.MaxInt32),
							},
						},
					},
				},
			},
		},
		{
			name:             "new node without max limit of volumes",
			driverName:       "com.example.csi.driver1",
			existingNode:     generateNode(nil /*nodeIDs*/, nil /*labels*/, nil /*capacity*/),
			inputVolumeLimit: 0,
			inputTopology:    nil,
			inputNodeID:      "com.example.csi/csi-node1",
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/csi-node1"})},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: nil,
						},
					},
				},
			},
		},
		{
			name:       "node with existing valid max limit of volumes",
			driverName: "com.example.csi.driver1",
			existingNode: generateNode(
				nil, /*nodeIDs*/
				nil, /*labels*/
				map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: *resource.NewScaledQuantity(4, -3),
				}),

			existingCSINode: generateCSINode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				generateVolumeLimits(10),
				nil, /* topologyKeys */
			),

			inputVolumeLimit: 20,
			inputTopology:    nil,
			inputNodeID:      "com.example.csi/csi-node1",
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"com.example.csi.driver1": "com.example.csi/csi-node1"})},
				},
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU: *resource.NewScaledQuantity(4, -3),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU: *resource.NewScaledQuantity(4, -3),
					},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "com.example.csi.driver1",
							NodeID:       "com.example.csi/csi-node1",
							TopologyKeys: nil,
							Allocatable:  generateVolumeLimits(20),
						},
					},
				},
			},
		},
	}

	test(t, true /* addNodeInfo */, testcases)
}

// generateVolumeLimits 生成卷数量上限资源对象，用于构造测试用例。
func generateVolumeLimits(i int32) *storage.VolumeNodeResources {
	return &storage.VolumeNodeResources{
		Count: ptr.To[int32](i),
	}
}

// TestUninstallCSIDriver 用各种已存在的 Node 和/或 CSINode 对象测试 UninstallCSIDriver。
func TestUninstallCSIDriver(t *testing.T) {
	testcases := []testcase{
		{
			name:         "empty node and empty CSINode",
			driverName:   "com.example.csi.driver1",
			existingNode: generateNode(nil /* nodeIDs */, nil /* labels */, nil /*capacity*/),
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node1",
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec:       storage.CSINodeSpec{},
			},
		},
		{
			name:       "pre-existing node info from the same driver",
			driverName: "com.example.csi.driver1",
			existingNode: generateNode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				labelMap{
					"com.example.csi/zone": "zoneA",
				}, nil /*capacity*/),
			existingCSINode: generateCSINode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				nil, /* volumeLimits */
				topologyKeyMap{
					"com.example.csi.driver1": {"com.example.csi/zone"},
				},
			),
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "node1",
					Labels: labelMap{"com.example.csi/zone": "zoneA"},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec:       storage.CSINodeSpec{},
			},
			hasModified: true,
		},
		{
			name:       "pre-existing node info from different driver",
			driverName: "com.example.csi.driver1",
			existingNode: generateNode(
				nodeIDMap{
					"net.example.storage.other-driver": "net.example.storage/csi-node1",
				},
				labelMap{
					"net.example.storage/zone": "zoneA",
				}, nil /*capacity*/),
			existingCSINode: generateCSINode(
				nodeIDMap{
					"net.example.storage.other-driver": "net.example.storage/csi-node1",
				},
				nil, /* volumeLimits */
				topologyKeyMap{
					"net.example.storage.other-driver": {"net.example.storage/zone"},
				},
			),
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"net.example.storage.other-driver": "net.example.storage/csi-node1"})},
					Labels:      labelMap{"net.example.storage/zone": "zoneA"},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{
					Drivers: []storage.CSINodeDriver{
						{
							Name:         "net.example.storage.other-driver",
							NodeID:       "net.example.storage/csi-node1",
							TopologyKeys: []string{"net.example.storage/zone"},
						},
					},
				},
			},
			hasModified: false,
		},
		{
			name:       "pre-existing info about the same driver in node, but empty CSINode",
			driverName: "com.example.csi.driver1",
			existingNode: generateNode(
				nodeIDMap{
					"com.example.csi.driver1": "com.example.csi/csi-node1",
				},
				nil /* labels */, nil /*capacity*/),
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node1",
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec:       storage.CSINodeSpec{},
			},
		},
		{
			name: "pre-existing info about a different driver in node, but empty CSINode",
			existingNode: generateNode(
				nodeIDMap{
					"net.example.storage.other-driver": "net.example.storage/csi-node1",
				},
				nil /* labels */, nil /*capacity*/),
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{annotationKeyNodeID: marshall(nodeIDMap{"net.example.storage.other-driver": "net.example.storage/csi-node1"})},
				},
			},
			expectedCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec:       storage.CSINodeSpec{},
			},
		},
	}

	test(t, false /* addNodeInfo */, testcases)
}

// TestSetMigrationAnnotation 验证迁移插件注解的设置逻辑。
func TestSetMigrationAnnotation(t *testing.T) {
	testcases := []struct {
		name            string
		migratedPlugins map[string](func() bool)
		existingNode    *storage.CSINode
		expectedNode    *storage.CSINode
		expectModified  bool
	}{
		{
			name: "nil migrated plugins",
			existingNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node1",
				},
			},
			expectedNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node1",
				},
			},
		},
		{
			name: "one modified plugin",
			migratedPlugins: map[string](func() bool){
				"test": func() bool { return true },
			},
			existingNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node1",
				},
			},
			expectedNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{v1.MigratedPluginsAnnotationKey: "test"},
				},
			},
			expectModified: true,
		},
		{
			name: "existing plugin",
			migratedPlugins: map[string](func() bool){
				"test": func() bool { return true },
			},
			existingNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{v1.MigratedPluginsAnnotationKey: "test"},
				},
			},
			expectedNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{v1.MigratedPluginsAnnotationKey: "test"},
				},
			},
			expectModified: false,
		},
		{
			name:            "remove plugin",
			migratedPlugins: map[string](func() bool){},
			existingNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{v1.MigratedPluginsAnnotationKey: "test"},
				},
			},
			expectedNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{},
				},
			},
			expectModified: true,
		},
		{
			name: "one modified plugin, other annotations stable",
			migratedPlugins: map[string](func() bool){
				"test": func() bool { return true },
			},
			existingNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{"other": "annotation"},
				},
			},
			expectedNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{v1.MigratedPluginsAnnotationKey: "test", "other": "annotation"},
				},
			},
			expectModified: true,
		},
		{
			name: "multiple plugins modified, other annotations stable",
			migratedPlugins: map[string](func() bool){
				"test": func() bool { return true },
				"foo":  func() bool { return false },
			},
			existingNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{"other": "annotation", v1.MigratedPluginsAnnotationKey: "foo"},
				},
			},
			expectedNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{v1.MigratedPluginsAnnotationKey: "test", "other": "annotation"},
				},
			},
			expectModified: true,
		},
		{
			name: "multiple plugins added, other annotations stable",
			migratedPlugins: map[string](func() bool){
				"test": func() bool { return true },
				"foo":  func() bool { return true },
			},
			existingNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{"other": "annotation"},
				},
			},
			expectedNode: &storage.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node1",
					Annotations: map[string]string{v1.MigratedPluginsAnnotationKey: "foo,test", "other": "annotation"},
				},
			},
			expectModified: true,
		},
	}

	for _, tc := range testcases {
		t.Logf("test case: %s", tc.name)

		modified := setMigrationAnnotation(tc.migratedPlugins, tc.existingNode)
		if modified != tc.expectModified {
			t.Errorf("Expected modified to be %v but got %v instead", tc.expectModified, modified)
		}

		if !reflect.DeepEqual(tc.expectedNode, tc.existingNode) {
			t.Errorf("Expected CSINode: %v, but got: %v", tc.expectedNode, tc.existingNode)
		}
	}
}

// TestInstallCSIDriverExistingAnnotation 验证 Node 上已有注解时驱动安装的兼容性。
func TestInstallCSIDriverExistingAnnotation(t *testing.T) {
	driverName := "com.example.csi/driver1"
	nodeID := "com.example.csi/some-node"

	testcases := []struct {
		name         string
		existingNode *v1.Node
	}{
		{
			name: "pre-existing info about the same driver in node, but empty CSINode",
			existingNode: generateNode(
				nodeIDMap{
					"com.example.csi/driver1": "com.example.csi/csi-node1",
				},
				nil /* labels */, nil /*capacity*/),
		},
		{
			name: "pre-existing info about a different driver in node, but empty CSINode",
			existingNode: generateNode(
				nodeIDMap{
					"net.example.storage/other-driver": "net.example.storage/test-node",
				},
				nil /* labels */, nil /*capacity*/),
		},
	}

	for _, tc := range testcases {
		t.Logf("test case: %q", tc.name)

		// 准备：构造测试客户端与宿主环境。
		nodeName := tc.existingNode.Name
		client := fake.NewSimpleClientset(tc.existingNode)

		tmpDir, err := utiltesting.MkTmpdir("nodeinfomanager-test")
		if err != nil {
			t.Fatalf("can't create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)
		host := volumetest.NewFakeVolumeHostWithCSINodeName(t,
			tmpDir,
			client,
			nil,
			nodeName,
			nil,
			nil,
		)

		nim := NewNodeInfoManager(types.NodeName(nodeName), host, nil)

		// 执行：创建 CSINode 并安装驱动。
		_, err = nim.CreateCSINode()
		if err != nil {
			t.Errorf("expected no error from creating CSINodeinfo but got: %v", err)
			continue
		}
		err = nim.InstallCSIDriver(driverName, nodeID, 0 /* maxVolumeLimit */, nil) // TODO test maxVolumeLimit
		if err != nil {
			t.Errorf("expected no error from InstallCSIDriver call but got: %v", err)
			continue
		}

		// 断言：校验 CSINode 中的驱动信息。
		nodeInfo, err := client.StorageV1().CSINodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
		if err != nil {
			t.Errorf("error getting CSINode: %v", err)
			continue
		}

		driver := nodeInfo.Spec.Drivers[0]
		if driver.Name != driverName || driver.NodeID != nodeID {
			t.Errorf("expected Driver to be %q and NodeID to be %q, but got: %q:%q", driverName, nodeID, driver.Name, driver.NodeID)
		}
	}
}

// getClientSet 根据给定的 Node 和 CSINode 构造 fake 客户端。
func getClientSet(existingNode *v1.Node, existingCSINode *storage.CSINode) *fake.Clientset {
	objects := []runtime.Object{}
	if existingNode != nil {
		objects = append(objects, existingNode)
	}
	if existingCSINode != nil {
		objects = append(objects, existingCSINode)
	}
	return fake.NewSimpleClientset(objects...)
}

// test 依次执行给定的测试用例，校验 Node/CSINode 的最终状态。
func test(t *testing.T, addNodeInfo bool, testcases []testcase) {
	for _, tc := range testcases {
		t.Logf("test case: %q", tc.name)

		//// 准备：构造测试客户端与宿主环境。
		nodeName := tc.existingNode.Name
		client := getClientSet(tc.existingNode, tc.existingCSINode)

		tmpDir, err := utiltesting.MkTmpdir("nodeinfomanager-test")
		if err != nil {
			t.Fatalf("can't create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)
		host := volumetest.NewFakeVolumeHostWithCSINodeName(t,
			tmpDir,
			client,
			nil,
			nodeName,
			nil,
			nil,
		)
		nim := NewNodeInfoManager(types.NodeName(nodeName), host, tc.migratedPlugins)

		//// 执行：根据用例安装或卸载驱动。
		nim.CreateCSINode()
		if addNodeInfo {
			err = nim.InstallCSIDriver(tc.driverName, tc.inputNodeID, tc.inputVolumeLimit, tc.inputTopology)
		} else {
			err = nim.UninstallCSIDriver(tc.driverName)
		}

		//// 断言：校验操作结果及对象状态。
		if tc.expectFail {
			if err == nil {
				t.Errorf("expected an error from InstallCSIDriver call but got none")
			}
			continue
		} else if err != nil {
			t.Errorf("expected no error from InstallCSIDriver call but got: %v", err)
			continue
		}

		actions := client.Actions()

		var node *v1.Node
		if action := hasPatchAction(actions); action != nil {
			node, err = applyNodeStatusPatch(tc.existingNode, action.(clienttesting.PatchActionImpl).GetPatch())
			assert.NoError(t, err)
		} else {
			node, err = client.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
			assert.NoError(t, err)
		}

		if node == nil {
			t.Errorf("error getting node: %v", err)
			continue
		}

		if !helper.Semantic.DeepEqual(node, tc.expectedNode) {
			t.Errorf("expected Node %v; got: %v", tc.expectedNode, node)
		}

		// 校验 CSINode 对象。
		nodeInfo, err := client.StorageV1().CSINodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				t.Errorf("error getting CSINode: %v", err)
			}
			continue
		}
		if !helper.Semantic.DeepEqual(nodeInfo, tc.expectedCSINode) {
			t.Errorf("expected CSINode %v; got: %v", tc.expectedCSINode, nodeInfo)
		}

		if !addNodeInfo && tc.existingCSINode != nil && tc.existingNode != nil {
			if tc.hasModified && helper.Semantic.DeepEqual(nodeInfo, tc.existingCSINode) {
				t.Errorf("existing CSINode %v; got: %v", tc.existingCSINode, nodeInfo)
			}
			if !tc.hasModified && !helper.Semantic.DeepEqual(nodeInfo, tc.existingCSINode) {
				t.Errorf("existing CSINode %v; got: %v", tc.existingCSINode, nodeInfo)
			}
		}
	}
}

// generateNode 根据节点 ID、标签和容量构造测试用 Node 对象。
func generateNode(nodeIDs, labels map[string]string, capacity map[v1.ResourceName]resource.Quantity) *v1.Node {
	var annotations map[string]string
	if len(nodeIDs) > 0 {
		b, _ := json.Marshal(nodeIDs)
		annotations = map[string]string{annotationKeyNodeID: string(b)}
	}
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node1",
			Annotations: annotations,
			Labels:      labels,
		},
	}

	if len(capacity) > 0 {
		node.Status.Capacity = v1.ResourceList(capacity)
		node.Status.Allocatable = v1.ResourceList(capacity)
	}
	return node
}

// marshall 将节点 ID 映射序列化为 JSON 字符串。
func marshall(nodeIDs nodeIDMap) string {
	b, _ := json.Marshal(nodeIDs)
	return string(b)
}

// generateCSINode 根据驱动节点 ID、卷上限和拓扑键构造测试用 CSINode 对象。
func generateCSINode(nodeIDs nodeIDMap, volumeLimits *storage.VolumeNodeResources, topologyKeys topologyKeyMap) *storage.CSINode {
	nodeDrivers := []storage.CSINodeDriver{}
	for k, nodeID := range nodeIDs {
		dspec := storage.CSINodeDriver{
			Name:        k,
			NodeID:      nodeID,
			Allocatable: volumeLimits,
		}
		if top, exists := topologyKeys[k]; exists {
			dspec.TopologyKeys = top
		}
		nodeDrivers = append(nodeDrivers, dspec)
	}

	return &storage.CSINode{
		ObjectMeta: getCSINodeObjectMeta(),
		Spec: storage.CSINodeSpec{
			Drivers: nodeDrivers,
		},
	}
}

// getCSINodeObjectMeta 返回测试用 CSINode 的对象元数据。
func getCSINodeObjectMeta() metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name: "node1",
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: nodeKind.Version,
				Kind:       nodeKind.Kind,
				Name:       "node1",
			},
		},
	}
}

// applyNodeStatusPatch 将 strategic merge patch 应用到 Node 上，返回更新后的对象。
func applyNodeStatusPatch(originalNode *v1.Node, patch []byte) (*v1.Node, error) {
	original, err := json.Marshal(originalNode)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal original node %#v: %v", originalNode, err)
	}
	updated, err := strategicpatch.StrategicMergePatch(original, patch, v1.Node{})
	if err != nil {
		return nil, fmt.Errorf("failed to apply strategic merge patch %q on node %#v: %v",
			patch, originalNode, err)
	}
	updatedNode := &v1.Node{}
	if err := json.Unmarshal(updated, updatedNode); err != nil {
		return nil, fmt.Errorf("failed to unmarshal updated node %q: %v", updated, err)
	}
	return updatedNode, nil
}

// hasPatchAction 返回动作列表中第一个 patch 动作，没有则返回 nil。
func hasPatchAction(actions []clienttesting.Action) clienttesting.Action {
	for _, action := range actions {
		if action.GetVerb() == "patch" {
			return action
		}
	}
	return nil
}

// TestUpdateCSINodeStorageHealth 验证 CSINode 存储健康状态的更新逻辑。
func TestUpdateCSINodeStorageHealth(t *testing.T) {
	driver1 := "com.example.csi.driver1"
	driver2 := "com.example.csi.driver2"
	cond1 := storage.StorageHealthCondition{
		Status:  storage.StorageDegraded,
		Reason:  "DiskSlow",
		Message: "disk is slow",
	}
	cond1UpdatedMsg := storage.StorageHealthCondition{
		Status:  storage.StorageDegraded,
		Reason:  "DiskSlow",
		Message: "disk is still slow",
	}
	cond1New := storage.StorageHealthCondition{
		Status: storage.StorageUnreachable,
		Reason: "NetworkDown",
	}
	cond2 := storage.StorageHealthCondition{
		Status: storage.StorageDegraded,
		Reason: "OtherIssue",
	}
	cond1Block := cond1
	cond1Block.AccessMode = ptr.To(v1.ReadWriteOnce)
	cond1Block.VolumeMode = ptr.To(v1.PersistentVolumeBlock)
	cond1Filesystem := cond1
	cond1Filesystem.AccessMode = ptr.To(v1.ReadWriteOnce)
	cond1Filesystem.VolumeMode = ptr.To(v1.PersistentVolumeFilesystem)
	health1 := storage.StorageHealth{Name: driver1, HealthConditions: []storage.StorageHealthCondition{cond1}}
	health1New := storage.StorageHealth{Name: driver1, HealthConditions: []storage.StorageHealthCondition{cond1New}}
	health2 := storage.StorageHealth{Name: driver2, HealthConditions: []storage.StorageHealthCondition{cond2}}
	health1Filesystem := storage.StorageHealth{Name: driver1, HealthConditions: []storage.StorageHealthCondition{cond1Filesystem}}

	testcases := []struct {
		name            string
		featureEnabled  bool
		existingCSINode *storage.CSINode
		driverName      string
		conditions      []storage.StorageHealthCondition
		expectStatus    []storage.StorageHealth
		expectUpdate    bool
	}{
		{
			name:           "feature gate disabled - no-op",
			featureEnabled: false,
			existingCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec:       storage.CSINodeSpec{Drivers: []storage.CSINodeDriver{{Name: driver1, NodeID: "n1"}}},
			},
			driverName:   driver1,
			conditions:   []storage.StorageHealthCondition{cond1},
			expectStatus: nil,
			expectUpdate: false,
		},
		{
			name:           "set health for driver with empty status",
			featureEnabled: true,
			existingCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec:       storage.CSINodeSpec{Drivers: []storage.CSINodeDriver{{Name: driver1, NodeID: "n1"}}},
			},
			driverName:   driver1,
			conditions:   []storage.StorageHealthCondition{cond1},
			expectStatus: []storage.StorageHealth{health1},
			expectUpdate: true,
		},
		{
			name:           "preserve other drivers' health",
			featureEnabled: true,
			existingCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{Drivers: []storage.CSINodeDriver{
					{Name: driver1, NodeID: "n1"},
					{Name: driver2, NodeID: "n2"},
				}},
				Status: storage.CSINodeStatus{StorageHealth: []storage.StorageHealth{health2}},
			},
			driverName:   driver1,
			conditions:   []storage.StorageHealthCondition{cond1},
			expectStatus: []storage.StorageHealth{health2, health1},
			expectUpdate: true,
		},
		{
			name:           "replace existing driver conditions",
			featureEnabled: true,
			existingCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec:       storage.CSINodeSpec{Drivers: []storage.CSINodeDriver{{Name: driver1, NodeID: "n1"}}},
				Status:     storage.CSINodeStatus{StorageHealth: []storage.StorageHealth{health1}},
			},
			driverName:   driver1,
			conditions:   []storage.StorageHealthCondition{cond1New},
			expectStatus: []storage.StorageHealth{health1New},
			expectUpdate: true,
		},
		{
			name:           "replace driver conditions preserves other drivers",
			featureEnabled: true,
			existingCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{Drivers: []storage.CSINodeDriver{
					{Name: driver1, NodeID: "n1"},
					{Name: driver2, NodeID: "n2"},
				}},
				Status: storage.CSINodeStatus{StorageHealth: []storage.StorageHealth{health1, health2}},
			},
			driverName:   driver1,
			conditions:   []storage.StorageHealthCondition{cond1New},
			expectStatus: []storage.StorageHealth{health2, health1New},
			expectUpdate: true,
		},
		{
			name:           "no-op when identity unchanged",
			featureEnabled: true,
			existingCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec:       storage.CSINodeSpec{Drivers: []storage.CSINodeDriver{{Name: driver1, NodeID: "n1"}}},
				Status:     storage.CSINodeStatus{StorageHealth: []storage.StorageHealth{health1}},
			},
			driverName:   driver1,
			conditions:   []storage.StorageHealthCondition{cond1UpdatedMsg},
			expectStatus: []storage.StorageHealth{health1},
			expectUpdate: false,
		},
		{
			name:           "capability change updates conditions",
			featureEnabled: true,
			existingCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec:       storage.CSINodeSpec{Drivers: []storage.CSINodeDriver{{Name: driver1, NodeID: "n1"}}},
				Status: storage.CSINodeStatus{StorageHealth: []storage.StorageHealth{
					{Name: driver1, HealthConditions: []storage.StorageHealthCondition{cond1Block}},
				}},
			},
			driverName:   driver1,
			conditions:   []storage.StorageHealthCondition{cond1Filesystem},
			expectStatus: []storage.StorageHealth{health1Filesystem},
			expectUpdate: true,
		},
		{
			name:           "no-op when duplicate reports share the same identities",
			featureEnabled: true,
			existingCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec:       storage.CSINodeSpec{Drivers: []storage.CSINodeDriver{{Name: driver1, NodeID: "n1"}}},
				Status:     storage.CSINodeStatus{StorageHealth: []storage.StorageHealth{health1}},
			},
			driverName:   driver1,
			conditions:   []storage.StorageHealthCondition{cond1, cond1},
			expectStatus: []storage.StorageHealth{health1},
			expectUpdate: false,
		},
		{
			name:           "clear driver conditions",
			featureEnabled: true,
			existingCSINode: &storage.CSINode{
				ObjectMeta: getCSINodeObjectMeta(),
				Spec: storage.CSINodeSpec{Drivers: []storage.CSINodeDriver{
					{Name: driver1, NodeID: "n1"},
					{Name: driver2, NodeID: "n2"},
				}},
				Status: storage.CSINodeStatus{StorageHealth: []storage.StorageHealth{health1, health2}},
			},
			driverName:   driver1,
			conditions:   nil,
			expectStatus: []storage.StorageHealth{health2},
			expectUpdate: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.CSIVolumeHealth, tc.featureEnabled)

			existingNode := generateNode(nil, nil, nil)
			client := getClientSet(existingNode, tc.existingCSINode)

			tmpDir, err := utiltesting.MkTmpdir("nodeinfomanager-health-test")
			if err != nil {
				t.Fatalf("can't create temp dir: %v", err)
			}
			defer func() {
				err := os.RemoveAll(tmpDir)
				if err != nil {
					t.Errorf("error removing tmpdir: %v", err)
				}
			}()

			host := volumetest.NewFakeVolumeHostWithCSINodeName(t, tmpDir, client, nil, existingNode.Name, nil, nil)
			nim := NewNodeInfoManager(types.NodeName(existingNode.Name), host, nil).(*nodeInfoManager)
			nim.nodeID = existingNode.UID

			actionsBefore := len(client.Actions())
			err = nim.UpdateCSINodeStorageHealth(tc.driverName, tc.conditions)
			if err != nil {
				t.Fatalf("UpdateCSINodeStorageHealth returned error: %v", err)
			}

			gotUpdate := false
			for _, action := range client.Actions()[actionsBefore:] {
				if action.GetVerb() == "update" && action.GetSubresource() == "status" {
					gotUpdate = true
					break
				}
			}
			if gotUpdate != tc.expectUpdate {
				t.Errorf("expected status update=%v, got %v", tc.expectUpdate, gotUpdate)
			}

			got, err := client.StorageV1().CSINodes().Get(context.TODO(), existingNode.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("error getting CSINode: %v", err)
			}
			if !reflect.DeepEqual(got.Status.StorageHealth, tc.expectStatus) {
				t.Errorf("StorageHealth mismatch:\n got: %#v\nwant: %#v", got.Status.StorageHealth, tc.expectStatus)
			}
		})
	}
}
