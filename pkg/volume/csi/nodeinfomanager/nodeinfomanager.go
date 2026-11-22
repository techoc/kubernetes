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

// Package nodeinfomanager 提供内部函数，为相应的 CSI 驱动在 Kubernetes 节点上
// 添加或删除标签，并维护 Node 与 CSINode 对象上的驱动信息。
package nodeinfomanager

import (
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"time"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
)

const (
	// annotationKeyNodeID 是节点注解的名称，其值为驱动名到节点 ID 的 JSON 映射。
	annotationKeyNodeID = "csi.volume.kubernetes.io/nodeid"
)

var (
	// nodeKind 描述 Node 对象的 Kind，用于校验 CSINode 的属主引用。
	nodeKind = v1.SchemeGroupVersion.WithKind("Node")
	// updateBackoff 定义更新 Node/CSINode 时的指数退避策略，缓解并发更新冲突。
	updateBackoff = wait.Backoff{
		Steps:    4,
		Duration: 10 * time.Millisecond,
		Factor:   5.0,
		Jitter:   0.1,
	}
)

// nodeInfoManager 持有更新 Node 与 CSINode 对象所需的一组公共依赖。
type nodeInfoManager struct {
	nodeName        types.NodeName
	nodeID          types.UID
	volumeHost      volume.VolumeHost
	migratedPlugins map[string](func() bool)
	// lock 保护对 Node 对象的并发修改，避免更新冲突。
	lock sync.Mutex
}

// nodeUpdateFunc 对 Node 执行一次变换。
// 若无需更新，必须原样返回输入的 Node 对象，以便调用方跳过写入。
type nodeUpdateFunc func(*v1.Node) (newNode *v1.Node, updated bool, err error)

// Interface 定义管理节点标签与 CSI 驱动信息的操作集合。
type Interface interface {
	// CreateCSINode 为当前节点创建 CSINode 对象，并写入迁移注解。
	CreateCSINode() (*storagev1.CSINode, error)

	// InitializeCSINodeWithAnnotation 创建或更新 CSINode 对象，写入 CSI 迁移所需的注解。
	InitializeCSINodeWithAnnotation() error

	// InstallCSIDriver 在集群中记录指定名称 CSI 驱动上报的节点信息。
	// 允许并发调用 InstallCSIDriver()，但不得与本接口的其他方法调用交错。
	InstallCSIDriver(driverName string, driverNodeID string, maxVolumeLimit int64, topology map[string]string) error

	// UpdateCSIDriver 更新 CSINode 对象中的 CSIDrivers 字段。
	UpdateCSIDriver(driverName string, driverNodeID string, maxAttachLimit int64, topology map[string]string) error

	// UninstallCSIDriver 从集群中移除指定名称 CSI 驱动的节点信息。
	// 允许并发调用 UninstallCSIDriver()，但不得与本接口的其他方法调用交错。
	UninstallCSIDriver(driverName string) error

	// UpdateCSINodeStorageHealth 用给定条件更新 CSINode.Status.StorageHealth 中该驱动的条目。
	// 其他驱动的条目保持不变。若该驱动的条件标识集合未变，则不做任何操作。
	UpdateCSINodeStorageHealth(driverName string, conditions []storagev1.StorageHealthCondition) error
}

// NewNodeInfoManager 初始化并返回一个 nodeInfoManager。
func NewNodeInfoManager(
	nodeName types.NodeName,
	volumeHost volume.VolumeHost,
	migratedPlugins map[string](func() bool)) Interface {
	return &nodeInfoManager{
		nodeName:        nodeName,
		volumeHost:      volumeHost,
		migratedPlugins: migratedPlugins,
	}
}

// InstallCSIDriver 更新 Node 对象中的节点 ID 注解以及 CSINode 对象中的 CSIDrivers 字段。
// 若 CSINode 对象尚不存在，则先创建它。
// 并行调用 InstallCSIDriver() 时，部分调用可能遇到 Node 或 CSINode 更新冲突，
// 此时会通过退避重试相应的更新。
func (nim *nodeInfoManager) InstallCSIDriver(driverName string, driverNodeID string, maxAttachLimit int64, topology map[string]string) error {
	if driverNodeID == "" {
		return fmt.Errorf("error adding CSI driver node info: driverNodeID must not be empty")
	}

	// 先依次更新 Node 上的节点 ID 注解与拓扑标签，再更新 CSINode。
	nodeUpdateFuncs := []nodeUpdateFunc{
		updateNodeIDInNode(driverName, driverNodeID),
		updateTopologyLabels(topology),
	}

	err := nim.updateNode(nodeUpdateFuncs...)
	if err != nil {
		return fmt.Errorf("error updating Node object with CSI driver node info: %v", err)
	}

	err = nim.updateCSINode(driverName, driverNodeID, maxAttachLimit, topology)
	if err != nil {
		return fmt.Errorf("error updating CSINode object with CSI driver node info: %v", err)
	}

	return nil
}

// UpdateCSIDriver 更新 CSINode 对象中的 CSIDrivers 字段。
func (nim *nodeInfoManager) UpdateCSIDriver(driverName string, driverNodeID string, maxAttachLimit int64, topology map[string]string) error {
	err := nim.updateCSINode(driverName, driverNodeID, maxAttachLimit, topology)
	if err != nil {
		return fmt.Errorf("error updating CSINode object with CSI driver node info: %w", err)
	}
	return nil
}

// UninstallCSIDriver 从 Node 对象移除节点 ID 注解，并从 CSINode 对象移除对应的 CSIDrivers 条目。
// 若 CSINode 对象已不含任何 CSIDriver，则将其删除。
// 并行调用 UninstallCSIDriver() 时，部分调用可能遇到 Node 或 CSINode 更新冲突，
// 此时会通过退避重试相应的更新。
func (nim *nodeInfoManager) UninstallCSIDriver(driverName string) error {
	err := nim.uninstallDriverFromCSINode(driverName)
	if err != nil {
		return fmt.Errorf("error uninstalling CSI driver from CSINode object %v", err)
	}

	err = nim.updateNode(
		removeNodeIDFromNode(driverName),
	)
	if err != nil {
		return fmt.Errorf("error removing CSI driver node info from Node object %v", err)
	}
	return nil
}

// updateNode 通过指数退避重试，依次应用给定的更新函数来更新 Node 对象。
func (nim *nodeInfoManager) updateNode(updateFuncs ...nodeUpdateFunc) error {
	var updateErrs []error
	// 更新冲突属于可重试错误，收集每次失败原因以便最终一并报告。
	err := wait.ExponentialBackoff(updateBackoff, func() (bool, error) {
		if err := nim.tryUpdateNode(updateFuncs...); err != nil {
			updateErrs = append(updateErrs, err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("error updating node: %v; caused by: %v", err, utilerrors.NewAggregate(updateErrs))
	}
	return nil
}

// tryUpdateNode 依次应用给定的更新函数来修改 Node 对象，并尝试写回。
// 由于 updateFuncs 按顺序执行，后续函数需考虑前面函数的改动以避免冲突。
// 例如多个函数更新同一字段时，最终以最后一个函数的更新为准。
func (nim *nodeInfoManager) tryUpdateNode(updateFuncs ...nodeUpdateFunc) error {
	nim.lock.Lock()
	defer nim.lock.Unlock()

	// 写回前先读取最新的 Node，避免覆盖其他写入方已有的改动。

	kubeClient := nim.volumeHost.GetKubeClient()
	if kubeClient == nil {
		return fmt.Errorf("error getting kube client")
	}

	nodeClient := kubeClient.CoreV1().Nodes()
	originalNode, err := nodeClient.Get(context.TODO(), string(nim.nodeName), metav1.GetOptions{})
	if err != nil {
		return err
	}
	node := originalNode.DeepCopy()

	needUpdate := false
	for _, update := range updateFuncs {
		newNode, updated, err := update(node)
		if err != nil {
			return err
		}
		node = newNode
		needUpdate = needUpdate || updated
	}

	if needUpdate {
		// 直接更新整个 Node 无法写入 status，只能用 PatchNodeStatus 同时更新状态与标签/注解。
		_, _, updateErr := nodeutil.PatchNodeStatus(kubeClient.CoreV1(), types.NodeName(node.Name), originalNode, node)
		return updateErr
	}

	return nil
}

// buildNodeIDMapFromAnnotation 解析 Node 注解中的驱动名到节点 ID 映射。
// 保证只要不返回错误，所得 map 就非 nil，便于调用方直接写入。
func buildNodeIDMapFromAnnotation(node *v1.Node) (map[string]string, error) {
	var previousAnnotationValue string
	if node.ObjectMeta.Annotations != nil {
		previousAnnotationValue =
			node.ObjectMeta.Annotations[annotationKeyNodeID]
	}

	var existingDriverMap map[string]string
	if previousAnnotationValue != "" {
		// 注解值以 JSON 存储，解析失败时直接报错，避免覆盖已有数据。
		if err := json.Unmarshal([]byte(previousAnnotationValue), &existingDriverMap); err != nil {
			return nil, fmt.Errorf(
				"failed to parse node's %q annotation value (%q) err=%v",
				annotationKeyNodeID,
				previousAnnotationValue,
				err)
		}
	}

	if existingDriverMap == nil {
		return make(map[string]string), nil
	}
	return existingDriverMap, nil
}

// updateNodeIDInNode 返回一个函数，用给定的节点 ID 信息更新 Node 对象上的注解。
func updateNodeIDInNode(
	csiDriverName string,
	csiDriverNodeID string) nodeUpdateFunc {
	return func(node *v1.Node) (*v1.Node, bool, error) {
		existingDriverMap, err := buildNodeIDMapFromAnnotation(node)
		if err != nil {
			return nil, false, err
		}

		if val, ok := existingDriverMap[csiDriverName]; ok {
			if val == csiDriverNodeID {
				// 注解中已有相同的值，无需更新，避免无谓的写操作。
				return node, false, nil
			}
		}

		// 新增或覆盖该驱动对应的节点 ID。
		existingDriverMap[csiDriverName] = csiDriverNodeID
		jsonObj, err := json.Marshal(existingDriverMap)
		if err != nil {
			return nil, false, fmt.Errorf(
				"error while marshalling node ID map updated with driverName=%q, nodeID=%q: %v",
				csiDriverName,
				csiDriverNodeID,
				err)
		}

		if node.ObjectMeta.Annotations == nil {
			node.ObjectMeta.Annotations = make(map[string]string)
		}
		node.ObjectMeta.Annotations[annotationKeyNodeID] = string(jsonObj)

		return node, true, nil
	}
}

// removeNodeIDFromNode 返回一个函数，从 Node 对象的注解中移除指定驱动的节点 ID 信息。
func removeNodeIDFromNode(csiDriverName string) nodeUpdateFunc {
	return func(node *v1.Node) (*v1.Node, bool, error) {
		var previousAnnotationValue string
		if node.ObjectMeta.Annotations != nil {
			previousAnnotationValue =
				node.ObjectMeta.Annotations[annotationKeyNodeID]
		}

		if previousAnnotationValue == "" {
			return node, false, nil
		}

		// 注解值以 JSON 存储；解析失败时报错，避免误删其他驱动的数据。
		existingDriverMap := map[string]string{}
		if err := json.Unmarshal([]byte(previousAnnotationValue), &existingDriverMap); err != nil {
			return nil, false, fmt.Errorf(
				"failed to parse node's %q annotation value (%q) err=%v",
				annotationKeyNodeID,
				previousAnnotationValue,
				err)
		}

		if _, ok := existingDriverMap[csiDriverName]; !ok {
			// 注解中本就没有该驱动，无需更新，避免无谓的写操作。
			return node, false, nil
		}

		// 删除该驱动的条目；若已无驱动则顺带移除整个注解。
		delete(existingDriverMap, csiDriverName)
		if len(existingDriverMap) == 0 {
			delete(node.ObjectMeta.Annotations, annotationKeyNodeID)
		} else {
			jsonObj, err := json.Marshal(existingDriverMap)
			if err != nil {
				return nil, false, fmt.Errorf(
					"failed while trying to remove key %q from node %q annotation. Existing data: %v",
					csiDriverName,
					annotationKeyNodeID,
					previousAnnotationValue)
			}

			node.ObjectMeta.Annotations[annotationKeyNodeID] = string(jsonObj)
		}

		return node, true, nil
	}
}

// updateTopologyLabels 返回一个函数，用给定的拓扑信息更新 Node 对象的标签。
func updateTopologyLabels(topology map[string]string) nodeUpdateFunc {
	return func(node *v1.Node) (*v1.Node, bool, error) {
		if len(topology) == 0 {
			return node, false, nil
		}

		// 已有标签与驱动上报值冲突时报错，避免悄悄改写其他来源的拓扑信息。
		for k, v := range topology {
			if curVal, exists := node.Labels[k]; exists && curVal != v {
				return nil, false, fmt.Errorf("detected topology value collision: driver reported %q:%q but existing label is %q:%q", k, v, k, curVal)
			}
		}

		if node.Labels == nil {
			node.Labels = make(map[string]string)
		}
		for k, v := range topology {
			node.Labels[k] = v
		}
		return node, true, nil
	}
}

// updateCSINode 通过指数退避重试，将驱动信息写入 CSINode 对象。
func (nim *nodeInfoManager) updateCSINode(
	driverName string,
	driverNodeID string,
	maxAttachLimit int64,
	topology map[string]string) error {

	csiKubeClient := nim.volumeHost.GetKubeClient()
	if csiKubeClient == nil {
		return fmt.Errorf("error getting CSI client")
	}

	var updateErrs []error
	// 冲突可重试；记录每次失败原因以便最终定位问题。
	err := wait.ExponentialBackoff(updateBackoff, func() (bool, error) {
		if err := nim.tryUpdateCSINode(csiKubeClient, driverName, driverNodeID, maxAttachLimit, topology); err != nil {
			updateErrs = append(updateErrs, err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("error updating CSINode: %v; caused by: %v", err, utilerrors.NewAggregate(updateErrs))
	}
	return nil
}

// tryUpdateCSINode 尝试一次 CSINode 更新：对象不存在时先创建，并校验属主后安装驱动信息。
func (nim *nodeInfoManager) tryUpdateCSINode(
	csiKubeClient clientset.Interface,
	driverName string,
	driverNodeID string,
	maxAttachLimit int64,
	topology map[string]string) error {

	nim.lock.Lock()
	defer nim.lock.Unlock()

	nodeInfo, err := csiKubeClient.StorageV1().CSINodes().Get(context.TODO(), string(nim.nodeName), metav1.GetOptions{})
	// 节点首次接入 CSI 驱动时 CSINode 还不存在，需要先创建再更新。
	if nodeInfo == nil || errors.IsNotFound(err) {
		nodeInfo, err = nim.CreateCSINode()
	}
	if err != nil {
		return err
	}

	if err = nim.ensureNodeOwnsCSINode(nodeInfo); err != nil {
		return err
	}

	return nim.installDriverToCSINode(nodeInfo, driverName, driverNodeID, maxAttachLimit, topology)
}

// InitializeCSINodeWithAnnotation 通过指数退避重试，确保 CSINode 对象存在且带有 CSI 迁移注解。
func (nim *nodeInfoManager) InitializeCSINodeWithAnnotation() error {
	csiKubeClient := nim.volumeHost.GetKubeClient()
	if csiKubeClient == nil {
		return goerrors.New("error getting CSI client")
	}

	var lastErr error
	// 失败时记录最后一次错误，便于退避耗尽后给出具体原因。
	err := wait.ExponentialBackoff(updateBackoff, func() (bool, error) {
		if lastErr = nim.tryInitializeCSINodeWithAnnotation(csiKubeClient); lastErr != nil {
			klog.V(2).Infof("Failed to publish CSINode: %v", lastErr)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("error updating CSINode annotation: %v; caused by: %v", err, lastErr)
	}

	return nil
}

// tryInitializeCSINodeWithAnnotation 尝试一次 CSINode 初始化，并同步 CSI 迁移注解。
func (nim *nodeInfoManager) tryInitializeCSINodeWithAnnotation(csiKubeClient clientset.Interface) error {
	nim.lock.Lock()
	defer nim.lock.Unlock()

	node, err := csiKubeClient.CoreV1().Nodes().Get(context.TODO(), string(nim.nodeName), metav1.GetOptions{})
	if err != nil {
		return err
	}
	nim.nodeID = node.UID

	nodeInfo, err := csiKubeClient.StorageV1().CSINodes().Get(context.TODO(), string(nim.nodeName), metav1.GetOptions{})
	if nodeInfo == nil || errors.IsNotFound(err) {
		// 新建 CSINode 时 CreateCSINode 会一并写入迁移注解，无需后续更新。
		_, err = nim.CreateCSINode()
		return err
	} else if err != nil {
		return err
	}

	if err = nim.ensureNodeOwnsCSINode(nodeInfo); err != nil {
		return err
	}

	annotationModified := setMigrationAnnotation(nim.migratedPlugins, nodeInfo)

	if annotationModified {
		_, err := csiKubeClient.StorageV1().CSINodes().Update(context.TODO(), nodeInfo, metav1.UpdateOptions{})
		return err
	}
	return nil

}

// ensureNodeOwnsCSINode 确保当前 CSINode 对象属于本 nodeInfoManager 所代表的节点。
// 若不属于，则删除该 CSINode 对象并返回错误。
func (nim *nodeInfoManager) ensureNodeOwnsCSINode(nodeInfo *storagev1.CSINode) error {
	if ok, csiNodeOwnerID := nim.nodeOwnsCSINode(nodeInfo); !ok {
		klog.V(2).Infof("existing CSINode %q is owned by different node (oldNodeID=%q, newNodeID=%q), cleaning up...", nodeInfo.Name, csiNodeOwnerID, nim.nodeID)
		err := nim.DeleteCSINode()
		if err != nil {
			return fmt.Errorf("error deleting existing CSINode %q: %w", nodeInfo.Name, err)
		}
		// 返回错误以触发重试，让下一次尝试重新创建 CSINode 对象。
		return fmt.Errorf("CSINode %q was owned by different node (oldNodeID=%q, newNodeID=%q), deleted it", nodeInfo.Name, csiNodeOwnerID, nim.nodeID)
	}
	return nil
}

// nodeOwnsCSINode 检查 CSINode 对象是否属于本 nodeInfoManager 所代表的节点。
// 同时返回当前属主的 UID。
func (nim *nodeInfoManager) nodeOwnsCSINode(nodeInfo *storagev1.CSINode) (bool, types.UID) {
	var csiNodeOwnerID types.UID
	var found bool
	for _, ownerRef := range nodeInfo.OwnerReferences {
		if ownerRef.Kind != nodeKind.Kind {
			continue
		}

		csiNodeOwnerID = ownerRef.UID
		if ownerRef.Name != string(nim.nodeName) {
			continue
		}

		if csiNodeOwnerID == nim.nodeID {
			found = true
			break
		}
	}
	return found, csiNodeOwnerID
}

// CreateCSINode 为当前节点创建 CSINode 对象，设置节点属主引用并写入迁移注解。
func (nim *nodeInfoManager) CreateCSINode() (*storagev1.CSINode, error) {

	csiKubeClient := nim.volumeHost.GetKubeClient()
	if csiKubeClient == nil {
		return nil, fmt.Errorf("error getting CSI client")
	}

	nodeInfo := &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(nim.nodeName),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: nodeKind.Version,
					Kind:       nodeKind.Kind,
					Name:       string(nim.nodeName),
					UID:        nim.nodeID,
				},
			},
		},
		Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{},
		},
	}

	setMigrationAnnotation(nim.migratedPlugins, nodeInfo)

	return csiKubeClient.StorageV1().CSINodes().Create(context.TODO(), nodeInfo, metav1.CreateOptions{})
}

// DeleteCSINode 删除当前节点对应的 CSINode 对象。
func (nim *nodeInfoManager) DeleteCSINode() error {

	csiKubeClient := nim.volumeHost.GetKubeClient()
	if csiKubeClient == nil {
		return fmt.Errorf("error getting CSI client")
	}

	return csiKubeClient.StorageV1().CSINodes().Delete(context.TODO(), string(nim.nodeName), metav1.DeleteOptions{})
}

// setMigrationAnnotation 根据 migratedPlugins 的当前状态刷新 CSINode 上的迁移插件注解。
// 若注解内容发生变化则返回 true，调用方需据此写回对象。
func setMigrationAnnotation(migratedPlugins map[string](func() bool), nodeInfo *storagev1.CSINode) (modified bool) {
	if migratedPlugins == nil {
		return false
	}

	nodeInfoAnnotations := nodeInfo.GetAnnotations()
	// 注解 map 可能为 nil，先建一个可写的副本，避免直接改到 nil map。
	if nodeInfoAnnotations == nil {
		nodeInfoAnnotations = map[string]string{}
	}

	var oldAnnotationSet sets.Set[string]
	mpa := nodeInfoAnnotations[v1.MigratedPluginsAnnotationKey]
	tok := strings.Split(mpa, ",")
	if len(mpa) == 0 {
		oldAnnotationSet = sets.New[string]()
	} else {
		oldAnnotationSet = sets.New[string](tok...)
	}

	newAnnotationSet := sets.New[string]()
	for pluginName, migratedFunc := range migratedPlugins {
		if migratedFunc() {
			newAnnotationSet.Insert(pluginName)
		}
	}

	// 集合相等时无需写回，避免无谓的更新与冲突。
	if oldAnnotationSet.Equal(newAnnotationSet) {
		return false
	}

	nas := strings.Join(sets.List[string](newAnnotationSet), ",")
	if len(nas) != 0 {
		nodeInfoAnnotations[v1.MigratedPluginsAnnotationKey] = nas
	} else {
		delete(nodeInfoAnnotations, v1.MigratedPluginsAnnotationKey)
	}

	nodeInfo.Annotations = nodeInfoAnnotations
	return true
}

// keepAllocatableCount 判断新的 maxAttachLimit 是否无需更新 CSINode：
// 当且仅当当前的 attach 上限与之相同时返回 true。
func keepAllocatableCount(driverInfoSpec storagev1.CSINodeDriver, maxAttachLimit int64) bool {
	if maxAttachLimit == 0 {
		return driverInfoSpec.Allocatable == nil || driverInfoSpec.Allocatable.Count == nil
	}

	return driverInfoSpec.Allocatable != nil && driverInfoSpec.Allocatable.Count != nil && int64(*driverInfoSpec.Allocatable.Count) == maxAttachLimit
}

// installDriverToCSINode 将驱动信息写入 CSINode.Spec.Drivers，并在必要时同步迁移注解。
func (nim *nodeInfoManager) installDriverToCSINode(
	nodeInfo *storagev1.CSINode,
	driverName string,
	driverNodeID string,
	maxAttachLimit int64,
	topology map[string]string) error {

	csiKubeClient := nim.volumeHost.GetKubeClient()
	if csiKubeClient == nil {
		return fmt.Errorf("error getting CSI client")
	}

	topologyKeys := sets.KeySet[string, string](topology)

	specModified := true
	// 复制驱动列表并剔除同名驱动，再用新信息重新追加，保证规格是最新且无重复的。
	newDriverSpecs := []storagev1.CSINodeDriver{}
	for _, driverInfoSpec := range nodeInfo.Spec.Drivers {
		if driverInfoSpec.Name == driverName {
			if driverInfoSpec.NodeID == driverNodeID &&
				sets.New[string](driverInfoSpec.TopologyKeys...).Equal(topologyKeys) &&
				keepAllocatableCount(driverInfoSpec, maxAttachLimit) {
				specModified = false
			}
		} else {
			// 同名驱动不复制，稍后以新规格追加。
			newDriverSpecs = append(newDriverSpecs, driverInfoSpec)
		}
	}

	annotationModified := setMigrationAnnotation(nim.migratedPlugins, nodeInfo)

	// 规格与注解都没变化时不写回，避免无谓的更新与冲突。
	if !specModified && !annotationModified {
		return nil
	}

	// 追加新的驱动规格。
	driverSpec := storagev1.CSINodeDriver{
		Name:         driverName,
		NodeID:       driverNodeID,
		TopologyKeys: sets.List[string](topologyKeys),
	}

	if maxAttachLimit > 0 {
		// Allocatable.Count 是 int32，超出范围只能截断，否则无法写入对象。
		if maxAttachLimit > math.MaxInt32 {
			klog.Warningf("Exceeded max supported attach limit value, truncating it to %d", math.MaxInt32)
			maxAttachLimit = math.MaxInt32
		}
		m := int32(maxAttachLimit)
		driverSpec.Allocatable = &storagev1.VolumeNodeResources{Count: &m}
	} else if maxAttachLimit != 0 {
		klog.Errorf("Invalid attach limit value %d cannot be added to CSINode object for %q", maxAttachLimit, driverName)
	}

	newDriverSpecs = append(newDriverSpecs, driverSpec)
	nodeInfo.Spec.Drivers = newDriverSpecs

	_, err := csiKubeClient.StorageV1().CSINodes().Update(context.TODO(), nodeInfo, metav1.UpdateOptions{})
	return err
}

// uninstallDriverFromCSINode 通过指数退避重试，从 CSINode 中移除指定驱动。
func (nim *nodeInfoManager) uninstallDriverFromCSINode(
	csiDriverName string) error {

	csiKubeClient := nim.volumeHost.GetKubeClient()
	if csiKubeClient == nil {
		return fmt.Errorf("error getting CSI client")
	}

	var updateErrs []error
	// 冲突可重试；记录每次失败原因以便最终定位问题。
	err := wait.ExponentialBackoff(updateBackoff, func() (bool, error) {
		if err := nim.tryUninstallDriverFromCSINode(csiKubeClient, csiDriverName); err != nil {
			updateErrs = append(updateErrs, err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("error updating CSINode: %v; caused by: %v", err, utilerrors.NewAggregate(updateErrs))
	}
	return nil
}

// tryUninstallDriverFromCSINode 尝试一次从 CSINode 中移除指定驱动；对象不存在时视为成功。
func (nim *nodeInfoManager) tryUninstallDriverFromCSINode(
	csiKubeClient clientset.Interface,
	csiDriverName string) error {

	nim.lock.Lock()
	defer nim.lock.Unlock()

	nodeInfoClient := csiKubeClient.StorageV1().CSINodes()
	nodeInfo, err := nodeInfoClient.Get(context.TODO(), string(nim.nodeName), metav1.GetOptions{})
	if err != nil && errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	hasModified := false
	// 原地过滤驱动列表：复用底层数组，去掉名为 csiDriverName 的条目。
	drivers := nodeInfo.Spec.Drivers[:0]
	for _, driver := range nodeInfo.Spec.Drivers {
		if driver.Name != csiDriverName {
			drivers = append(drivers, driver)
		} else {
			// 找到了目标驱动，标记列表已改动，稍后需写回。
			hasModified = true
		}
	}

	// 没有改动时不写回，避免无谓的更新与冲突。
	if !hasModified {
		return nil
	}
	nodeInfo.Spec.Drivers = drivers

	_, err = nodeInfoClient.Update(context.TODO(), nodeInfo, metav1.UpdateOptions{})

	return err // 不包装错误，保持原始错误类型以便调用方判断。

}

// UpdateCSINodeStorageHealth 用给定条件更新 CSINode.Status.StorageHealth 中该驱动的条目。
// 其他驱动的条目保持不变。
// 若该驱动的 status、reason 与卷能力标识均未变，则不做任何操作。
func (nim *nodeInfoManager) UpdateCSINodeStorageHealth(driverName string, conditions []storagev1.StorageHealthCondition) error {
	if !utilfeature.DefaultFeatureGate.Enabled(features.CSIVolumeHealth) {
		return nil
	}

	csiKubeClient := nim.volumeHost.GetKubeClient()
	if csiKubeClient == nil {
		return goerrors.New("error getting CSI client")
	}

	var updateErrs []error
	err := wait.ExponentialBackoff(updateBackoff, func() (bool, error) {
		if err := nim.tryUpdateCSINodeStorageHealth(csiKubeClient, driverName, conditions); err != nil {
			updateErrs = append(updateErrs, err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("error updating CSINode status: %w; caused by: %w", err, utilerrors.NewAggregate(updateErrs))
	}
	return nil
}

// tryUpdateCSINodeStorageHealth 尝试一次更新 CSINode 中该驱动的存储健康状态。
func (nim *nodeInfoManager) tryUpdateCSINodeStorageHealth(
	csiKubeClient clientset.Interface,
	driverName string,
	conditions []storagev1.StorageHealthCondition) error {

	nim.lock.Lock()
	defer nim.lock.Unlock()

	nodeInfo, err := csiKubeClient.StorageV1().CSINodes().Get(context.TODO(), string(nim.nodeName), metav1.GetOptions{})
	if err != nil {
		return err
	}

	if err = nim.ensureNodeOwnsCSINode(nodeInfo); err != nil {
		return err
	}

	var existingDriverConds []storagev1.StorageHealthCondition
	// 保留其他驱动的状态，只替换目标驱动，避免更新时丢失其健康信息。
	otherDriversHealth := make([]storagev1.StorageHealth, 0, len(nodeInfo.Status.StorageHealth))
	for _, health := range nodeInfo.Status.StorageHealth {
		if health.Name == driverName {
			existingDriverConds = health.HealthConditions
		} else {
			otherDriversHealth = append(otherDriversHealth, health)
		}
	}

	// 条件标识未变时不写回，避免重复上报造成无谓的状态写入。
	if storageHealthIdentityEqual(existingDriverConds, conditions) {
		return nil
	}

	combined := make([]storagev1.StorageHealth, 0, len(otherDriversHealth)+1)
	combined = append(combined, otherDriversHealth...)
	if len(conditions) > 0 {
		combined = append(combined, storagev1.StorageHealth{Name: driverName, HealthConditions: conditions})
	}
	nodeInfo.Status.StorageHealth = combined
	_, err = csiKubeClient.StorageV1().CSINodes().UpdateStatus(context.TODO(), nodeInfo, metav1.UpdateOptions{})
	return err
}

// storageHealthIdentityEqual 判断两个条件切片的 status、reason 与卷能力标识集合是否相同。
// 忽略 message、transition time 与重复条目，避免重复或重复上报导致状态被反复写入。
func storageHealthIdentityEqual(a, b []storagev1.StorageHealthCondition) bool {
	return storageHealthIdentities(a).Equal(storageHealthIdentities(b))
}

// storageHealthIdentities 将条件切片转换为条件标识集合，用于忽略时间、消息等字段的比较。
func storageHealthIdentities(conditions []storagev1.StorageHealthCondition) sets.Set[storageHealthIdentity] {
	identities := sets.New[storageHealthIdentity]()
	for _, c := range conditions {
		identities.Insert(storageHealthConditionIdentity(c))
	}
	return identities
}

// storageHealthIdentity 是存储健康条件的身份标识，仅包含决定状态是否需要更新的字段。
type storageHealthIdentity struct {
	status, reason, accessMode, volumeMode string
}

// storageHealthConditionIdentity 从健康条件中提取身份标识；可选的访问模式与卷模式为空时留空。
func storageHealthConditionIdentity(condition storagev1.StorageHealthCondition) storageHealthIdentity {
	identity := storageHealthIdentity{
		status: string(condition.Status),
		reason: condition.Reason,
	}
	if condition.AccessMode != nil {
		identity.accessMode = string(*condition.AccessMode)
	}
	if condition.VolumeMode != nil {
		identity.volumeMode = string(*condition.VolumeMode)
	}
	return identity
}
