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

package csi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	csipbv1 "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	api "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// csiClient 定义 kubelet 与 CSI 驱动节点服务交互的接口。
type csiClient interface {
	NodeGetInfo(ctx context.Context) (
		nodeID string,
		maxVolumePerNode int64,
		accessibleTopology map[string]string,
		err error)

	// 调用方需先通过 NodeSupportsVolumeMountGroup() 检查驱动是否支持应用 FSGroup；
	// 若不支持，fsGroup 必须传 nil。
	NodePublishVolume(
		ctx context.Context,
		volumeid string,
		readOnly bool,
		stagingTargetPath string,
		targetPath string,
		accessMode api.PersistentVolumeAccessMode,
		publishContext map[string]string,
		volumeContext map[string]string,
		secrets map[string]string,
		fsType string,
		mountOptions []string,
		fsGroup *int64,
	) error

	NodeExpandVolume(ctx context.Context, rsOpts csiResizeOptions) (resource.Quantity, error)
	NodeUnpublishVolume(
		ctx context.Context,
		volID string,
		targetPath string,
	) error

	// 调用方需先通过 NodeSupportsVolumeMountGroup() 检查驱动是否支持应用 FSGroup；
	// 若不支持，fsGroup 必须传 nil。
	NodeStageVolume(ctx context.Context,
		volID string,
		publishVolumeInfo map[string]string,
		stagingTargetPath string,
		fsType string,
		accessMode api.PersistentVolumeAccessMode,
		secrets map[string]string,
		volumeContext map[string]string,
		mountOptions []string,
		fsGroup *int64,
	) error

	NodeGetVolumeStats(
		ctx context.Context,
		volID string,
		targetPath string,
	) (*volume.Metrics, error)
	// NodeGetVolumeHealth 返回卷的不健康状况；空切片表示卷健康。
	// stagingTargetPath 与 volumePublishPath 是可选的，卷从未成功 stage/publish 时可为空。
	NodeGetVolumeHealth(
		ctx context.Context,
		volID string,
		stagingTargetPath string,
		volumePublishPath string,
	) ([]api.VolumeHealthCondition, error)
	// NodeGetStorageHealth 返回驱动在本节点上的存储后端的不健康状况；空切片表示后端健康。
	NodeGetStorageHealth(
		ctx context.Context,
		secrets map[string]string,
	) ([]storagev1.StorageHealthCondition, error)
	NodeUnstageVolume(ctx context.Context, volID, stagingTargetPath string) error
	NodeSupportsStageUnstage(ctx context.Context) (bool, error)
	NodeSupportsNodeExpand(ctx context.Context) (bool, error)
	NodeSupportsVolumeStats(ctx context.Context) (bool, error)
	NodeSupportsVolumeHealth(ctx context.Context) (bool, error)
	NodeSupportsStorageHealth(ctx context.Context) (bool, error)
	NodeSupportsSingleNodeMultiWriterAccessMode(ctx context.Context) (bool, error)
	NodeSupportsVolumeMountGroup(ctx context.Context) (bool, error)
}

// csiAddr 是强类型的 CSI 驱动地址。
type csiAddr string

// csiDriverName 是强类型的 CSI 驱动名称。
type csiDriverName string

// csiDriverClient 封装了对 CSI 插件节点服务的调用。
type csiDriverClient struct {
	driverName          csiDriverName
	addr                csiAddr
	metricsManager      *MetricsManager
	nodeV1ClientCreator nodeV1ClientCreator
}

// csiResizeOptions 承载节点扩容所需的参数。
type csiResizeOptions struct {
	volumeID          string
	volumePath        string
	stagingTargetPath string
	fsType            string
	accessMode        api.PersistentVolumeAccessMode
	newSize           resource.Quantity
	mountOptions      []string
	secrets           map[string]string
}

var _ csiClient = &csiDriverClient{}

// nodeV1ClientCreator 创建 NodeClient 及其关闭函数，便于测试时替换。
type nodeV1ClientCreator func(addr csiAddr, metricsManager *MetricsManager) (
	nodeClient csipbv1.NodeClient,
	closer io.Closer,
	err error,
)

// nodeV1AccessModeMapper 将 Kubernetes 访问模式映射为 CSI v1 访问模式。
type nodeV1AccessModeMapper func(am api.PersistentVolumeAccessMode) csipbv1.VolumeCapability_AccessMode_Mode

// newV1NodeClient 创建 NodeClient 并建立 gRPC 连接，同时返回不再使用时必须调用的关闭函数。
// 这是 newCsiDriverClient 中 nodeV1ClientCreator 的默认实现。
func newV1NodeClient(addr csiAddr, metricsManager *MetricsManager) (nodeClient csipbv1.NodeClient, closer io.Closer, err error) {
	var conn *grpc.ClientConn
	conn, err = newGrpcConn(addr, metricsManager)
	if err != nil {
		return nil, nil, err
	}

	nodeClient = csipbv1.NewNodeClient(conn)
	return nodeClient, conn, nil
}

// newCsiDriverClient 根据已注册的驱动名称创建 csiDriverClient。
func newCsiDriverClient(driverName csiDriverName) (*csiDriverClient, error) {
	if driverName == "" {
		return nil, fmt.Errorf("driver name is empty")
	}

	existingDriver, driverExists := csiDrivers.Get(string(driverName))
	if !driverExists {
		return nil, fmt.Errorf("driver name %s not found in the list of registered CSI drivers", driverName)
	}

	nodeV1ClientCreator := newV1NodeClient
	return &csiDriverClient{
		driverName:          driverName,
		addr:                csiAddr(existingDriver.endpoint),
		nodeV1ClientCreator: nodeV1ClientCreator,
		metricsManager:      NewCSIMetricsManager(string(driverName)),
	}, nil
}

// NodeGetInfo 返回驱动报告的节点信息。
func (c *csiDriverClient) NodeGetInfo(ctx context.Context) (
	nodeID string,
	maxVolumePerNode int64,
	accessibleTopology map[string]string,
	err error) {
	klog.V(4).InfoS(log("calling NodeGetInfo rpc"))

	var getNodeInfoError error
	nodeID, maxVolumePerNode, accessibleTopology, getNodeInfoError = c.nodeGetInfoV1(ctx)
	if getNodeInfoError != nil {
		klog.InfoS("Error calling CSI NodeGetInfo()", "err", getNodeInfoError.Error())
	}
	return nodeID, maxVolumePerNode, accessibleTopology, getNodeInfoError
}

// nodeGetInfoV1 通过 NodeGetInfo RPC 获取节点信息。
func (c *csiDriverClient) nodeGetInfoV1(ctx context.Context) (
	nodeID string,
	maxVolumePerNode int64,
	accessibleTopology map[string]string,
	err error) {

	nodeClient, closer, err := c.nodeV1ClientCreator(c.addr, c.metricsManager)
	if err != nil {
		return "", 0, nil, err
	}
	defer closer.Close()

	res, err := nodeClient.NodeGetInfo(ctx, &csipbv1.NodeGetInfoRequest{})
	if err != nil {
		return "", 0, nil, err
	}

	topology := res.GetAccessibleTopology()
	if topology != nil {
		accessibleTopology = topology.Segments
	}
	return res.GetNodeId(), res.GetMaxVolumesPerNode(), accessibleTopology, nil
}

// NodePublishVolume 调用驱动的 NodePublishVolume RPC 将卷发布到目标路径。
func (c *csiDriverClient) NodePublishVolume(
	ctx context.Context,
	volID string,
	readOnly bool,
	stagingTargetPath string,
	targetPath string,
	accessMode api.PersistentVolumeAccessMode,
	publishContext map[string]string,
	volumeContext map[string]string,
	secrets map[string]string,
	fsType string,
	mountOptions []string,
	fsGroup *int64,
) error {
	klog.V(4).InfoS(log("calling NodePublishVolume rpc"), "volID", volID, "targetPath", targetPath)
	if volID == "" {
		return errors.New("missing volume id")
	}
	if targetPath == "" {
		return errors.New("missing target path")
	}

	if c.nodeV1ClientCreator == nil {
		return errors.New("failed to call NodePublishVolume. nodeV1ClientCreator is nil")
	}

	accessModeMapper, err := c.getNodeV1AccessModeMapper(ctx)
	if err != nil {
		return err
	}

	nodeClient, closer, err := c.nodeV1ClientCreator(c.addr, c.metricsManager)
	if err != nil {
		return err
	}
	defer closer.Close()

	req := &csipbv1.NodePublishVolumeRequest{
		VolumeId:       volID,
		TargetPath:     targetPath,
		Readonly:       readOnly,
		PublishContext: publishContext,
		VolumeContext:  volumeContext,
		Secrets:        secrets,
		VolumeCapability: &csipbv1.VolumeCapability{
			AccessMode: &csipbv1.VolumeCapability_AccessMode{
				Mode: accessModeMapper(accessMode),
			},
		},
	}
	if stagingTargetPath != "" {
		req.StagingTargetPath = stagingTargetPath
	}

	if fsType == fsTypeBlockName {
		req.VolumeCapability.AccessType = &csipbv1.VolumeCapability_Block{
			Block: &csipbv1.VolumeCapability_BlockVolume{},
		}
	} else {
		mountVolume := &csipbv1.VolumeCapability_MountVolume{
			FsType:     fsType,
			MountFlags: mountOptions,
		}
		if fsGroup != nil {
			mountVolume.VolumeMountGroup = strconv.FormatInt(*fsGroup, 10 /* base */)
		}
		req.VolumeCapability.AccessType = &csipbv1.VolumeCapability_Mount{
			Mount: mountVolume,
		}
	}

	_, err = nodeClient.NodePublishVolume(ctx, req)
	if err != nil && !isFinalError(err) {
		return volumetypes.NewUncertainProgressError(err.Error())
	}
	return err
}

// NodeExpandVolume 调用驱动的 NodeExpandVolume RPC 执行节点扩容。
func (c *csiDriverClient) NodeExpandVolume(ctx context.Context, opts csiResizeOptions) (resource.Quantity, error) {
	if c.nodeV1ClientCreator == nil {
		return opts.newSize, fmt.Errorf("version of CSI driver does not support volume expansion")
	}

	if opts.volumeID == "" {
		return opts.newSize, errors.New("missing volume id")
	}
	if opts.volumePath == "" {
		return opts.newSize, errors.New("missing volume path")
	}

	if opts.newSize.Value() < 0 {
		return opts.newSize, errors.New("size can not be less than 0")
	}

	accessModeMapper, err := c.getNodeV1AccessModeMapper(ctx)
	if err != nil {
		return opts.newSize, err
	}

	nodeClient, closer, err := c.nodeV1ClientCreator(c.addr, c.metricsManager)
	if err != nil {
		return opts.newSize, err
	}
	defer closer.Close()

	req := &csipbv1.NodeExpandVolumeRequest{
		VolumeId:      opts.volumeID,
		VolumePath:    opts.volumePath,
		CapacityRange: &csipbv1.CapacityRange{RequiredBytes: opts.newSize.Value()},
		VolumeCapability: &csipbv1.VolumeCapability{
			AccessMode: &csipbv1.VolumeCapability_AccessMode{
				Mode: accessModeMapper(opts.accessMode),
			},
		},
		Secrets: opts.secrets,
	}

	// 并非所有 CSI 驱动都支持 NodeStageUnstage，因此仅在有值时设置 StagingTargetPath。
	if opts.stagingTargetPath != "" {
		req.StagingTargetPath = opts.stagingTargetPath
	}

	if opts.fsType == fsTypeBlockName {
		req.VolumeCapability.AccessType = &csipbv1.VolumeCapability_Block{
			Block: &csipbv1.VolumeCapability_BlockVolume{},
		}
	} else {
		req.VolumeCapability.AccessType = &csipbv1.VolumeCapability_Mount{
			Mount: &csipbv1.VolumeCapability_MountVolume{
				FsType:     opts.fsType,
				MountFlags: opts.mountOptions,
			},
		}
	}

	resp, err := nodeClient.NodeExpandVolume(ctx, req)
	if err != nil {
		if !isFinalError(err) {
			return opts.newSize, volumetypes.NewUncertainProgressError(err.Error())
		}
		return opts.newSize, err
	}

	updatedQuantity := resource.NewQuantity(resp.CapacityBytes, resource.BinarySI)
	return *updatedQuantity, nil
}

// NodeUnpublishVolume 调用驱动的 NodeUnpublishVolume RPC 取消卷发布。
func (c *csiDriverClient) NodeUnpublishVolume(ctx context.Context, volID string, targetPath string) error {
	klog.V(4).InfoS(log("calling NodeUnpublishVolume rpc"), "volID", volID, "targetPath", targetPath)
	if volID == "" {
		return errors.New("missing volume id")
	}
	if targetPath == "" {
		return errors.New("missing target path")
	}
	if c.nodeV1ClientCreator == nil {
		return errors.New("nodeV1ClientCreate is nil")
	}

	nodeClient, closer, err := c.nodeV1ClientCreator(c.addr, c.metricsManager)
	if err != nil {
		return err
	}
	defer closer.Close()

	req := &csipbv1.NodeUnpublishVolumeRequest{
		VolumeId:   volID,
		TargetPath: targetPath,
	}

	_, err = nodeClient.NodeUnpublishVolume(ctx, req)
	return err
}

// NodeStageVolume 调用驱动的 NodeStageVolume RPC 将卷 stage 到临时路径。
func (c *csiDriverClient) NodeStageVolume(ctx context.Context,
	volID string,
	publishContext map[string]string,
	stagingTargetPath string,
	fsType string,
	accessMode api.PersistentVolumeAccessMode,
	secrets map[string]string,
	volumeContext map[string]string,
	mountOptions []string,
	fsGroup *int64,
) error {
	klog.V(4).InfoS(log("calling NodeStageVolume rpc"), "volID", volID, "stagingTargetPath", stagingTargetPath)
	if volID == "" {
		return errors.New("missing volume id")
	}
	if stagingTargetPath == "" {
		return errors.New("missing staging target path")
	}
	if c.nodeV1ClientCreator == nil {
		return errors.New("nodeV1ClientCreate is nil")
	}

	accessModeMapper, err := c.getNodeV1AccessModeMapper(ctx)
	if err != nil {
		return err
	}

	nodeClient, closer, err := c.nodeV1ClientCreator(c.addr, c.metricsManager)
	if err != nil {
		return err
	}
	defer closer.Close()

	req := &csipbv1.NodeStageVolumeRequest{
		VolumeId:          volID,
		PublishContext:    publishContext,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability: &csipbv1.VolumeCapability{
			AccessMode: &csipbv1.VolumeCapability_AccessMode{
				Mode: accessModeMapper(accessMode),
			},
		},
		Secrets:       secrets,
		VolumeContext: volumeContext,
	}

	if fsType == fsTypeBlockName {
		req.VolumeCapability.AccessType = &csipbv1.VolumeCapability_Block{
			Block: &csipbv1.VolumeCapability_BlockVolume{},
		}
	} else {
		mountVolume := &csipbv1.VolumeCapability_MountVolume{
			FsType:     fsType,
			MountFlags: mountOptions,
		}
		if fsGroup != nil {
			mountVolume.VolumeMountGroup = strconv.FormatInt(*fsGroup, 10 /* base */)
		}
		req.VolumeCapability.AccessType = &csipbv1.VolumeCapability_Mount{
			Mount: mountVolume,
		}
	}

	_, err = nodeClient.NodeStageVolume(ctx, req)
	if err != nil && !isFinalError(err) {
		return volumetypes.NewUncertainProgressError(err.Error())
	}
	return err
}

// NodeUnstageVolume 调用驱动的 NodeUnstageVolume RPC 取消卷的 stage。
func (c *csiDriverClient) NodeUnstageVolume(ctx context.Context, volID, stagingTargetPath string) error {
	klog.V(4).InfoS(log("calling NodeUnstageVolume rpc"), "volID", volID, "stagingTargetPath", stagingTargetPath)
	if volID == "" {
		return errors.New("missing volume id")
	}
	if stagingTargetPath == "" {
		return errors.New("missing staging target path")
	}
	if c.nodeV1ClientCreator == nil {
		return errors.New("nodeV1ClientCreate is nil")
	}

	nodeClient, closer, err := c.nodeV1ClientCreator(c.addr, c.metricsManager)
	if err != nil {
		return err
	}
	defer closer.Close()

	req := &csipbv1.NodeUnstageVolumeRequest{
		VolumeId:          volID,
		StagingTargetPath: stagingTargetPath,
	}
	_, err = nodeClient.NodeUnstageVolume(ctx, req)
	return err
}

// NodeSupportsNodeExpand 返回驱动是否支持卷扩容能力。
func (c *csiDriverClient) NodeSupportsNodeExpand(ctx context.Context) (bool, error) {
	return c.nodeSupportsCapability(ctx, csipbv1.NodeServiceCapability_RPC_EXPAND_VOLUME)
}

// NodeSupportsStageUnstage 返回驱动是否支持 stage/unstage 能力。
func (c *csiDriverClient) NodeSupportsStageUnstage(ctx context.Context) (bool, error) {
	return c.nodeSupportsCapability(ctx, csipbv1.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME)
}

// getNodeV1AccessModeMapper 按驱动能力选择访问模式映射函数。
func (c *csiDriverClient) getNodeV1AccessModeMapper(ctx context.Context) (nodeV1AccessModeMapper, error) {
	supported, err := c.NodeSupportsSingleNodeMultiWriterAccessMode(ctx)
	if err != nil {
		return nil, err
	}
	if supported {
		return asSingleNodeMultiWriterCapableCSIAccessModeV1, nil
	}
	return asCSIAccessModeV1, nil
}

// asCSIAccessModeV1 将 Kubernetes 访问模式映射为 CSI v1 访问模式。
func asCSIAccessModeV1(am api.PersistentVolumeAccessMode) csipbv1.VolumeCapability_AccessMode_Mode {
	switch am {
	case api.ReadWriteOnce:
		return csipbv1.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	case api.ReadOnlyMany:
		return csipbv1.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
	case api.ReadWriteMany:
		return csipbv1.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
	// 该映射使不支持 SINGLE_NODE_MULTI_WRITER 能力的 CSI 驱动也能配合 ReadWriteOncePod 访问模式工作。
	case api.ReadWriteOncePod:
		return csipbv1.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	}
	return csipbv1.VolumeCapability_AccessMode_UNKNOWN
}

// asSingleNodeMultiWriterCapableCSIAccessModeV1 针对支持 SINGLE_NODE_MULTI_WRITER 的驱动映射访问模式。
func asSingleNodeMultiWriterCapableCSIAccessModeV1(am api.PersistentVolumeAccessMode) csipbv1.VolumeCapability_AccessMode_Mode {
	switch am {
	case api.ReadWriteOnce:
		return csipbv1.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER
	case api.ReadOnlyMany:
		return csipbv1.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
	case api.ReadWriteMany:
		return csipbv1.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
	case api.ReadWriteOncePod:
		return csipbv1.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER
	}
	return csipbv1.VolumeCapability_AccessMode_UNKNOWN
}

// newGrpcConn 建立到 CSI 驱动 unix socket 的 gRPC 连接，并挂载指标拦截器。
func newGrpcConn(addr csiAddr, metricsManager *MetricsManager) (*grpc.ClientConn, error) {
	network := "unix"
	klog.V(4).InfoS(log("creating new gRPC connection"), "protocol", network, "endpoint", addr)

	return grpc.Dial(
		string(addr),
		grpc.WithAuthority("localhost"),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, target)
		}),
		grpc.WithChainUnaryInterceptor(metricsManager.RecordMetricsInterceptor),
	)
}

// csiClientGetter 带缓存地获取 CSI 客户端。它按驱动名初始化客户端并缓存备用。
// 当 CSI 客户端尚未被发现时（如 kubelet 重启期间），初始化会失败；使用方（如 mounter manager 与
// block mapper）可借此将 CSI 客户端初始化推迟到真正需要时。
type csiClientGetter struct {
	sync.RWMutex
	csiClient  csiClient
	driverName csiDriverName
}

// Get 返回缓存的 CSI 客户端，必要时延迟初始化。
func (c *csiClientGetter) Get() (csiClient, error) {
	c.RLock()
	if c.csiClient != nil {
		c.RUnlock()
		return c.csiClient, nil
	}
	c.RUnlock()
	c.Lock()
	defer c.Unlock()
	// 双重检查，避免并发初始化出多个客户端。
	if c.csiClient != nil {
		return c.csiClient, nil
	}
	csi, err := newCsiDriverClient(c.driverName)
	if err != nil {
		return nil, err
	}
	c.csiClient = csi
	return c.csiClient, nil
}

// NodeSupportsVolumeStats 返回驱动是否支持卷统计能力。
func (c *csiDriverClient) NodeSupportsVolumeStats(ctx context.Context) (bool, error) {
	return c.nodeSupportsCapability(ctx, csipbv1.NodeServiceCapability_RPC_GET_VOLUME_STATS)
}

// NodeSupportsSingleNodeMultiWriterAccessMode 返回驱动是否支持单节点多写访问模式。
func (c *csiDriverClient) NodeSupportsSingleNodeMultiWriterAccessMode(ctx context.Context) (bool, error) {
	return c.nodeSupportsCapability(ctx, csipbv1.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER)
}

// NodeGetVolumeStats 调用驱动的 NodeGetVolumeStats RPC 获取卷使用情况。
func (c *csiDriverClient) NodeGetVolumeStats(ctx context.Context, volID string, targetPath string) (*volume.Metrics, error) {
	klog.V(4).InfoS(log("calling NodeGetVolumeStats rpc"), "volID", volID, "targetPath", targetPath)
	if volID == "" {
		return nil, errors.New("missing volume id")
	}
	if targetPath == "" {
		return nil, errors.New("missing target path")
	}
	if c.nodeV1ClientCreator == nil {
		return nil, errors.New("nodeV1ClientCreate is nil")
	}

	nodeClient, closer, err := c.nodeV1ClientCreator(c.addr, c.metricsManager)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	req := &csipbv1.NodeGetVolumeStatsRequest{
		VolumeId:   volID,
		VolumePath: targetPath,
	}

	resp, err := nodeClient.NodeGetVolumeStats(ctx, req)
	if err != nil {
		return nil, err
	}

	metrics := &volume.Metrics{
		Used:       resource.NewQuantity(int64(0), resource.BinarySI),
		Capacity:   resource.NewQuantity(int64(0), resource.BinarySI),
		Available:  resource.NewQuantity(int64(0), resource.BinarySI),
		InodesUsed: resource.NewQuantity(int64(0), resource.BinarySI),
		Inodes:     resource.NewQuantity(int64(0), resource.BinarySI),
		InodesFree: resource.NewQuantity(int64(0), resource.BinarySI),
	}

	usages := resp.GetUsage()
	if usages == nil {
		return nil, fmt.Errorf("failed to get usage from response. usage is nil")
	}

	for _, usage := range usages {
		if usage == nil {
			continue
		}
		unit := usage.GetUnit()
		switch unit {
		case csipbv1.VolumeUsage_BYTES:
			metrics.Available = resource.NewQuantity(usage.GetAvailable(), resource.BinarySI)
			metrics.Capacity = resource.NewQuantity(usage.GetTotal(), resource.BinarySI)
			metrics.Used = resource.NewQuantity(usage.GetUsed(), resource.BinarySI)
		case csipbv1.VolumeUsage_INODES:
			metrics.InodesFree = resource.NewQuantity(usage.GetAvailable(), resource.BinarySI)
			metrics.Inodes = resource.NewQuantity(usage.GetTotal(), resource.BinarySI)
			metrics.InodesUsed = resource.NewQuantity(usage.GetUsed(), resource.BinarySI)
		default:
			klog.ErrorS(nil, "unknown unit in VolumeUsage", "unit", unit.String())
		}

	}
	return metrics, nil
}

// NodeSupportsVolumeHealth 返回驱动是否支持卷健康状态能力。
func (c *csiDriverClient) NodeSupportsVolumeHealth(ctx context.Context) (bool, error) {
	return c.nodeSupportsCapability(ctx, csipbv1.NodeServiceCapability_RPC_GET_VOLUME_HEALTH)
}

// NodeSupportsStorageHealth 返回驱动是否支持存储后端健康状态能力。
func (c *csiDriverClient) NodeSupportsStorageHealth(ctx context.Context) (bool, error) {
	return c.nodeSupportsCapability(ctx, csipbv1.NodeServiceCapability_RPC_GET_STORAGE_HEALTH)
}

// NodeGetVolumeHealth 调用驱动的 NodeGetVolumeHealth RPC 获取卷健康状况。
func (c *csiDriverClient) NodeGetVolumeHealth(ctx context.Context, volID, stagingTargetPath, volumePublishPath string) ([]api.VolumeHealthCondition, error) {
	klog.V(4).InfoS(log("calling NodeGetVolumeHealth rpc"), "volID", volID, "stagingTargetPath", stagingTargetPath, "volumePublishPath", volumePublishPath)
	if volID == "" {
		return nil, errors.New("missing volume id")
	}
	if c.nodeV1ClientCreator == nil {
		return nil, errors.New("nodeV1ClientCreate is nil")
	}

	nodeClient, closer, err := c.nodeV1ClientCreator(c.addr, c.metricsManager)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := closer.Close()
		if err != nil {
			klog.Errorf("error closing node client %v", err)
		}
	}()

	req := &csipbv1.NodeGetVolumeHealthRequest{
		VolumeId:          volID,
		StagingTargetPath: stagingTargetPath,
		VolumePublishPath: volumePublishPath,
	}
	resp, err := nodeClient.NodeGetVolumeHealth(ctx, req)
	if err != nil {
		return nil, err
	}
	return mapVolumeHealthConditions(resp.GetVolumeHealth()), nil
}

// NodeGetStorageHealth 调用驱动的 NodeGetStorageHealth RPC 获取存储后端健康状况。
func (c *csiDriverClient) NodeGetStorageHealth(ctx context.Context, secrets map[string]string) ([]storagev1.StorageHealthCondition, error) {
	klog.V(4).InfoS(log("calling NodeGetStorageHealth rpc"))
	if c.nodeV1ClientCreator == nil {
		return nil, errors.New("nodeV1ClientCreate is nil")
	}

	nodeClient, closer, err := c.nodeV1ClientCreator(c.addr, c.metricsManager)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := closer.Close()
		if err != nil {
			klog.Errorf("error closing node client: %v", err)
		}
	}()

	req := &csipbv1.NodeGetStorageHealthRequest{
		Secrets: secrets,
	}
	resp, err := nodeClient.NodeGetStorageHealth(ctx, req)
	if err != nil {
		return nil, err
	}
	return mapStorageBackendHealth(resp.GetBackendHealth())
}

// mapVolumeHealthConditions 将 CSI 卷健康状况转换为 Kubernetes 卷健康状况。
func mapVolumeHealthConditions(vh *csipbv1.VolumeHealth) []api.VolumeHealthCondition {
	if vh == nil {
		return nil
	}
	entries := vh.GetHealthStatuses()
	if len(entries) == 0 {
		return nil
	}
	out := make([]api.VolumeHealthCondition, 0, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		status, ok := mapVolumeHealthStatus(entry.GetStatus())
		if !ok {
			continue
		}
		out = append(out, api.VolumeHealthCondition{
			Status:  status,
			Reason:  entry.GetReason(),
			Message: entry.GetMessage(),
		})
	}
	return out
}

// mapVolumeHealthStatus 将 CSI 卷健康错误类型映射为 Kubernetes 状态类型。
func mapVolumeHealthStatus(status csipbv1.VolumeHealthErrorType) (api.VolumeHealthStatusType, bool) {
	switch status {
	case csipbv1.VolumeHealthErrorType_INACCESSIBLE:
		return api.VolumeHealthInaccessible, true
	case csipbv1.VolumeHealthErrorType_DATA_LOSS:
		return api.VolumeHealthDataLoss, true
	case csipbv1.VolumeHealthErrorType_DEGRADED:
		return api.VolumeHealthDegraded, true
	default:
		return "", false
	}
}

const maxStorageHealthConditions = 16

// mapStorageBackendHealth 将 CSI 存储后端健康状况转换为 Kubernetes 健康条件。
func mapStorageBackendHealth(entries []*csipbv1.NodeGetStorageHealthResponse_StorageBackendHealth) ([]storagev1.StorageHealthCondition, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	if len(entries) > maxStorageHealthConditions {
		return nil, fmt.Errorf("NodeGetStorageHealth returned %d conditions, maximum is %d", len(entries), maxStorageHealthConditions)
	}
	out := make([]storagev1.StorageHealthCondition, 0, len(entries))
	for i, entry := range entries {
		if entry == nil {
			continue
		}
		status, ok := mapStorageHealthStatus(entry.GetStatus())
		if !ok {
			continue
		}
		condition := storagev1.StorageHealthCondition{
			Status:  status,
			Reason:  entry.GetReason(),
			Message: entry.GetMessage(),
		}
		if capability := entry.GetVolumeCapability(); capability != nil {
			accessMode, volumeMode, err := mapStorageHealthVolumeCapability(capability)
			if err != nil {
				return nil, fmt.Errorf("NodeGetStorageHealth condition %d has invalid volume capability: %w", i, err)
			}
			condition.AccessMode = &accessMode
			condition.VolumeMode = &volumeMode
		}
		out = append(out, condition)
	}
	return out, nil
}

// mapStorageHealthVolumeCapability 将 CSI 卷能力转换为 Kubernetes 的访问模式与卷模式。
func mapStorageHealthVolumeCapability(capability *csipbv1.VolumeCapability) (api.PersistentVolumeAccessMode, api.PersistentVolumeMode, error) {
	var volumeMode api.PersistentVolumeMode
	switch capability.GetAccessType().(type) {
	case *csipbv1.VolumeCapability_Block:
		volumeMode = api.PersistentVolumeBlock
	case *csipbv1.VolumeCapability_Mount:
		volumeMode = api.PersistentVolumeFilesystem
	default:
		return "", "", errors.New("access type is required")
	}

	if capability.GetAccessMode() == nil {
		return "", "", errors.New("access mode is required")
	}
	var accessMode api.PersistentVolumeAccessMode
	switch capability.GetAccessMode().GetMode() {
	case csipbv1.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csipbv1.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		csipbv1.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:
		accessMode = api.ReadWriteOnce
	case csipbv1.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER:
		accessMode = api.ReadWriteOncePod
	case csipbv1.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY:
		accessMode = api.ReadOnlyMany
	case csipbv1.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		csipbv1.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
		accessMode = api.ReadWriteMany
	default:
		return "", "", fmt.Errorf("unsupported access mode %q", capability.GetAccessMode().GetMode())
	}
	return accessMode, volumeMode, nil
}

// mapStorageHealthStatus 将 CSI 存储健康错误类型映射为 Kubernetes 状态类型。
func mapStorageHealthStatus(status csipbv1.StorageHealthErrorType) (storagev1.StorageHealthStatusType, bool) {
	switch status {
	case csipbv1.StorageHealthErrorType_STORAGE_UNREACHABLE:
		return storagev1.StorageUnreachable, true
	case csipbv1.StorageHealthErrorType_STORAGE_DEGRADED:
		return storagev1.StorageDegraded, true
	default:
		return "", false
	}
}

// NodeSupportsVolumeMountGroup 返回驱动是否支持挂载时应用 volume group。
func (c *csiDriverClient) NodeSupportsVolumeMountGroup(ctx context.Context) (bool, error) {
	return c.nodeSupportsCapability(ctx, csipbv1.NodeServiceCapability_RPC_VOLUME_MOUNT_GROUP)
}

// nodeSupportsCapability 查询驱动是否声明了指定的节点服务能力。
func (c *csiDriverClient) nodeSupportsCapability(ctx context.Context, capabilityType csipbv1.NodeServiceCapability_RPC_Type) (bool, error) {
	klog.V(4).Info(log("calling NodeGetCapabilities rpc to determine if the node service has %s capability", capabilityType))
	capabilities, err := c.nodeGetCapabilities(ctx)
	if err != nil {
		return false, err
	}

	for _, capability := range capabilities {
		if capability == nil || capability.GetRpc() == nil {
			continue
		}
		if capability.GetRpc().GetType() == capabilityType {
			return true, nil
		}
	}
	return false, nil
}

// nodeGetCapabilities 调用 NodeGetCapabilities RPC 获取驱动的节点能力列表。
func (c *csiDriverClient) nodeGetCapabilities(ctx context.Context) ([]*csipbv1.NodeServiceCapability, error) {
	if c.nodeV1ClientCreator == nil {
		return []*csipbv1.NodeServiceCapability{}, errors.New("nodeV1ClientCreate is nil")
	}

	nodeClient, closer, err := c.nodeV1ClientCreator(c.addr, c.metricsManager)
	if err != nil {
		return []*csipbv1.NodeServiceCapability{}, err
	}
	defer closer.Close()

	req := &csipbv1.NodeGetCapabilitiesRequest{}
	resp, err := nodeClient.NodeGetCapabilities(ctx, req)
	if err != nil {
		if !isFinalError(err) {
			return []*csipbv1.NodeServiceCapability{}, volumetypes.NewUncertainProgressError(err.Error())
		}
		return []*csipbv1.NodeServiceCapability{}, err
	}
	return resp.GetCapabilities(), nil
}

// isFinalError 判断错误是否属于最终错误，即操作确定未在进行中。
func isFinalError(err error) bool {
	// 参考：
	// https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
	// https://github.com/container-storage-interface/spec/blob/master/spec.md
	st, ok := status.FromError(err)
	if !ok {
		// 非 gRPC 错误，说明在调用 gRPC 方法前就已失败。但无法确定此前是否有卷操作仍在进行，
		// 保守起见按非最终错误处理。
		return false
	}
	switch st.Code() {
	case codes.Canceled, // gRPC：客户端取消了请求
		codes.DeadlineExceeded,  // gRPC：超时
		codes.Unavailable,       // gRPC：服务端关闭或连接中断，之前的卷操作可能仍在进行
		codes.ResourceExhausted, // gRPC：服务端临时资源不足，之前的卷操作可能仍在进行
		codes.Aborted:           // CSI：卷上存在待处理操作
		return false
	}
	// 其余错误意味着操作要么尚未开始、要么已失败，可以确定没有在进行中。
	return true
}
