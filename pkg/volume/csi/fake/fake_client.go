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

// Package fake 提供用于测试的假 CSI 客户端实现。
// 通过预设卷 ID 可模拟超时、终态错误等场景，无需真实存储后端。
package fake

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// NodePublishTimeOut_VolumeID 是会使 NodePublish 操作超时的卷 ID。
	NodePublishTimeOut_VolumeID = "node-publish-timeout"
	// NodePublishFinalError_VolumeID 是会使 NodePublish 返回终态错误的卷 ID。
	NodePublishFinalError_VolumeID = "node-publish-final-error"

	// NodeStageTimeOut_VolumeID 是会使 NodeStage 操作超时的卷 ID。
	NodeStageTimeOut_VolumeID = "node-stage-timeout"
)

// IdentityClient 是测试用的 CSI identity 客户端。
type IdentityClient struct {
	nextErr error
}

// SetNextError 注入下次调用期望返回的错误。
func (f *IdentityClient) SetNextError(err error) {
	f.nextErr = err
}

// GetPluginInfo 返回插件信息（假实现，始终返回 nil）。
func (f *IdentityClient) GetPluginInfo(ctx context.Context, in *csipb.GetPluginInfoRequest, opts ...grpc.CallOption) (*csipb.GetPluginInfoResponse, error) {
	return nil, nil
}

// GetPluginCapabilities 实现 CSI GetPluginCapabilities 方法（假实现）。
func (f *IdentityClient) GetPluginCapabilities(ctx context.Context, in *csipb.GetPluginCapabilitiesRequest, opts ...grpc.CallOption) (*csipb.GetPluginCapabilitiesResponse, error) {
	return nil, nil
}

// Probe 实现 CSI Probe 方法（假实现）。
func (f *IdentityClient) Probe(ctx context.Context, in *csipb.ProbeRequest, opts ...grpc.CallOption) (*csipb.ProbeResponse, error) {
	return nil, nil
}

// CSIVolume 记录假客户端中某个卷的句柄、上下文和挂载路径等状态。
type CSIVolume struct {
	VolumeHandle     string
	VolumeContext    map[string]string
	Path             string
	DeviceMountPath  string
	FSType           string
	MountFlags       []string
	VolumeMountGroup string
}

// NodeClient 是测试用的 CSI node 客户端，记录已发布/已暂存的卷状态。
// 各布尔字段控制对外声明的能力，nextErr 用于注入错误。
type NodeClient struct {
	nodePublishedVolumes     map[string]CSIVolume
	nodeStagedVolumes        map[string]CSIVolume
	stageUnstageSet          bool
	expansionSet             bool
	volumeStatsSet           bool
	volumeHealthSet          bool
	storageHealthSet         bool
	SetVolumeStats           bool
	SetVolumeHealth          bool
	SetStorageHealth         bool
	singleNodeMultiWriterSet bool
	volumeMountGroupSet      bool
	nodeGetInfoResp          *csipb.NodeGetInfoResponse
	nodeVolumeStatsResp      *csipb.NodeGetVolumeStatsResponse
	FakeNodeExpansionRequest *csipb.NodeExpandVolumeRequest
	nextErr                  error
	getCapabilitiesErr       error
}

// NewNodeClient 创建假 node 客户端，用于 Staging/Unstaging 场景。
func NewNodeClient(stageUnstageSet bool) *NodeClient {
	return &NodeClient{
		nodePublishedVolumes: make(map[string]CSIVolume),
		nodeStagedVolumes:    make(map[string]CSIVolume),
		stageUnstageSet:      stageUnstageSet,
		volumeStatsSet:       true,
	}
}

// NewNodeClientWithExpansion 创建支持卷扩容的假 node 客户端。
func NewNodeClientWithExpansion(stageUnstageSet bool, expansionSet bool) *NodeClient {
	return &NodeClient{
		nodePublishedVolumes: make(map[string]CSIVolume),
		nodeStagedVolumes:    make(map[string]CSIVolume),
		stageUnstageSet:      stageUnstageSet,
		expansionSet:         expansionSet,
	}
}

// NewNodeClientWithVolumeStats 创建带卷统计能力的假 node 客户端。
func NewNodeClientWithVolumeStats(volumeStatsSet bool) *NodeClient {
	return &NodeClient{
		volumeStatsSet: volumeStatsSet,
		SetVolumeStats: true,
	}
}

// NewNodeClientWithVolumeStatsAndHealth 创建同时支持卷统计和健康检查的假 node 客户端。
func NewNodeClientWithVolumeStatsAndHealth(volumeStatsSet, volumeHealth, setVolumeStats, setVolumeHealth bool) *NodeClient {
	return &NodeClient{
		volumeStatsSet:  volumeStatsSet,
		volumeHealthSet: volumeHealth,
		SetVolumeStats:  setVolumeStats,
		SetVolumeHealth: setVolumeHealth,
	}
}

// NewNodeClientWithSingleNodeMultiWriter 创建支持单节点多写能力的假 node 客户端。
func NewNodeClientWithSingleNodeMultiWriter(singleNodeMultiWriterSet bool) *NodeClient {
	return &NodeClient{
		nodePublishedVolumes:     make(map[string]CSIVolume),
		nodeStagedVolumes:        make(map[string]CSIVolume),
		stageUnstageSet:          true,
		volumeStatsSet:           true,
		singleNodeMultiWriterSet: singleNodeMultiWriterSet,
	}
}

// NewNodeClientWithVolumeMountGroup 创建支持卷挂载组（fsGroup）能力的假 node 客户端。
func NewNodeClientWithVolumeMountGroup(stageUnstageSet, volumeMountGroupSet bool) *NodeClient {
	return &NodeClient{
		nodePublishedVolumes: make(map[string]CSIVolume),
		nodeStagedVolumes:    make(map[string]CSIVolume),
		stageUnstageSet:      stageUnstageSet,
		volumeMountGroupSet:  volumeMountGroupSet,
	}
}

// SetNextError 注入下次 RPC 调用期望返回的错误。
func (f *NodeClient) SetNextError(err error) {
	f.nextErr = err
}

// SetGetCapabilitiesErr 注入 GetCapabilities 调用返回的错误。
func (f *NodeClient) SetGetCapabilitiesErr(err error) {
	f.getCapabilitiesErr = err
}

// SetNodeGetInfoResp 设置 NodeGetInfo 的预设响应。
func (f *NodeClient) SetNodeGetInfoResp(resp *csipb.NodeGetInfoResponse) {
	f.nodeGetInfoResp = resp
}

// SetNodeVolumeStatsResp 设置 NodeGetVolumeStats 的预设响应。
func (f *NodeClient) SetNodeVolumeStatsResp(resp *csipb.NodeGetVolumeStatsResponse) {
	f.nodeVolumeStatsResp = resp
}

// GetNodePublishedVolumes 返回已发布的卷集合。
func (f *NodeClient) GetNodePublishedVolumes() map[string]CSIVolume {
	return f.nodePublishedVolumes
}

// AddNodePublishedVolume 将指定卷加入已发布集合，供测试预设状态。
func (f *NodeClient) AddNodePublishedVolume(volID, deviceMountPath string, volumeContext map[string]string) {
	f.nodePublishedVolumes[volID] = CSIVolume{
		Path:          deviceMountPath,
		VolumeContext: volumeContext,
	}
}

// GetNodeStagedVolumes 返回已暂存的卷集合。
func (f *NodeClient) GetNodeStagedVolumes() map[string]CSIVolume {
	return f.nodeStagedVolumes
}

// AddNodeStagedVolume 将指定卷加入已暂存集合，供测试预设状态。
func (f *NodeClient) AddNodeStagedVolume(volID, deviceMountPath string, volumeContext map[string]string) {
	f.nodeStagedVolumes[volID] = CSIVolume{
		Path:          deviceMountPath,
		VolumeContext: volumeContext,
	}
}

// NodePublishVolume 实现 CSI NodePublishVolume：校验参数后记录已发布卷。
// 特定卷 ID 会模拟超时或终态错误；同时创建目标路径（块设备建空文件，文件系统建目录）。
func (f *NodeClient) NodePublishVolume(ctx context.Context, req *csipb.NodePublishVolumeRequest, opts ...grpc.CallOption) (*csipb.NodePublishVolumeResponse, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}

	if req.GetVolumeId() == "" {
		return nil, errors.New("missing volume id")
	}
	if req.GetTargetPath() == "" {
		return nil, errors.New("missing target path")
	}
	fsTypes := "block|ext4|xfs|zfs"
	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	if !strings.Contains(fsTypes, fsType) {
		return nil, errors.New("invalid fstype")
	}

	if req.GetVolumeId() == NodePublishTimeOut_VolumeID {
		timeoutErr := status.Errorf(codes.DeadlineExceeded, "timeout exceeded")
		return nil, timeoutErr
	}

	if req.GetVolumeId() == NodePublishFinalError_VolumeID {
		return nil, status.Errorf(codes.Internal, "final error")
	}

	// CSI 规范要求存储插件自行创建 target_path，本假实现遵循该约定。
	if req.VolumeCapability.GetBlock() != nil {
		if err := os.WriteFile(req.TargetPath, []byte{}, 0644); err != nil {
			return nil, fmt.Errorf("cannot create target path %s for block file: %s", req.TargetPath, err)
		}
	} else {
		if err := os.MkdirAll(req.TargetPath, 0755); err != nil {
			return nil, fmt.Errorf("cannot create target directory %s for mount: %s", req.TargetPath, err)
		}
	}

	publishedVolume := CSIVolume{
		VolumeHandle:    req.GetVolumeId(),
		Path:            req.GetTargetPath(),
		DeviceMountPath: req.GetStagingTargetPath(),
		VolumeContext:   req.GetVolumeContext(),
	}
	if req.GetVolumeCapability().GetMount() != nil {
		publishedVolume.FSType = req.GetVolumeCapability().GetMount().FsType
		publishedVolume.MountFlags = req.GetVolumeCapability().GetMount().MountFlags
		publishedVolume.VolumeMountGroup = req.GetVolumeCapability().GetMount().VolumeMountGroup
	}
	f.nodePublishedVolumes[req.GetVolumeId()] = publishedVolume
	return &csipb.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume 实现 CSI NodeUnpublishVolume：删除已发布记录并清理目标路径。
func (f *NodeClient) NodeUnpublishVolume(ctx context.Context, req *csipb.NodeUnpublishVolumeRequest, opts ...grpc.CallOption) (*csipb.NodeUnpublishVolumeResponse, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}

	if req.GetVolumeId() == "" {
		return nil, errors.New("missing volume id")
	}
	if req.GetTargetPath() == "" {
		return nil, errors.New("missing target path")
	}
	delete(f.nodePublishedVolumes, req.GetVolumeId())

	// CSI 规范要求存储插件删除其在目标路径创建的文件或目录。
	if err := os.Remove(req.TargetPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove publish path %s: %s", req.TargetPath, err)
	}

	return &csipb.NodeUnpublishVolumeResponse{}, nil
}

// NodeStageVolume 实现 CSI NodeStageVolume：校验参数并记录已暂存卷。
// 特定卷 ID 会模拟超时错误。
func (f *NodeClient) NodeStageVolume(ctx context.Context, req *csipb.NodeStageVolumeRequest, opts ...grpc.CallOption) (*csipb.NodeStageVolumeResponse, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}

	if req.GetVolumeId() == "" {
		return nil, errors.New("missing volume id")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, errors.New("missing staging target path")
	}

	csiVol := CSIVolume{
		Path:          req.GetStagingTargetPath(),
		VolumeContext: req.GetVolumeContext(),
	}

	fsType := ""
	fsTypes := "block|ext4|xfs|zfs"
	mounted := req.GetVolumeCapability().GetMount()
	if mounted != nil {
		fsType = mounted.GetFsType()
		csiVol.MountFlags = mounted.GetMountFlags()
		csiVol.VolumeMountGroup = mounted.VolumeMountGroup
	}
	if !strings.Contains(fsTypes, fsType) {
		return nil, errors.New("invalid fstype")
	}

	if req.GetVolumeId() == NodeStageTimeOut_VolumeID {
		timeoutErr := status.Errorf(codes.DeadlineExceeded, "timeout exceeded")
		return nil, timeoutErr
	}

	f.nodeStagedVolumes[req.GetVolumeId()] = csiVol
	return &csipb.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume 实现 CSI NodeUnstageVolume：删除已暂存记录。
func (f *NodeClient) NodeUnstageVolume(ctx context.Context, req *csipb.NodeUnstageVolumeRequest, opts ...grpc.CallOption) (*csipb.NodeUnstageVolumeResponse, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}

	if req.GetVolumeId() == "" {
		return nil, errors.New("missing volume id")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, errors.New("missing staging target path")
	}

	delete(f.nodeStagedVolumes, req.GetVolumeId())
	return &csipb.NodeUnstageVolumeResponse{}, nil
}

// NodeExpandVolume 实现 CSI NodeExpandVolume：保存请求并返回请求的容量作为扩容结果。
func (f *NodeClient) NodeExpandVolume(ctx context.Context, req *csipb.NodeExpandVolumeRequest, opts ...grpc.CallOption) (*csipb.NodeExpandVolumeResponse, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}

	if req.GetVolumeId() == "" {
		return nil, errors.New("missing volume id")
	}
	if req.GetVolumePath() == "" {
		return nil, errors.New("missing volume path")
	}

	if req.GetCapacityRange().RequiredBytes <= 0 {
		return nil, errors.New("required bytes should be greater than 0")
	}

	f.FakeNodeExpansionRequest = req

	resp := &csipb.NodeExpandVolumeResponse{
		CapacityBytes: req.GetCapacityRange().RequiredBytes,
	}
	return resp, nil
}

// NodeGetInfo 返回预设的节点信息响应，未设置时返回 nil。
func (f *NodeClient) NodeGetInfo(ctx context.Context, in *csipb.NodeGetInfoRequest, opts ...grpc.CallOption) (*csipb.NodeGetInfoResponse, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}
	return f.nodeGetInfoResp, nil
}

// NodeGetCapabilities 根据各布尔标记拼装返回的能力列表。
// getCapabilitiesErr 非空时返回错误，用于测试失败路径。
func (f *NodeClient) NodeGetCapabilities(ctx context.Context, in *csipb.NodeGetCapabilitiesRequest, opts ...grpc.CallOption) (*csipb.NodeGetCapabilitiesResponse, error) {
	resp := &csipb.NodeGetCapabilitiesResponse{
		Capabilities: []*csipb.NodeServiceCapability{},
	}
	if f.getCapabilitiesErr != nil {
		return resp, f.getCapabilitiesErr
	}

	if f.stageUnstageSet {
		resp.Capabilities = append(resp.Capabilities, &csipb.NodeServiceCapability{
			Type: &csipb.NodeServiceCapability_Rpc{
				Rpc: &csipb.NodeServiceCapability_RPC{
					Type: csipb.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
				},
			},
		})
	}
	if f.expansionSet {
		resp.Capabilities = append(resp.Capabilities, &csipb.NodeServiceCapability{
			Type: &csipb.NodeServiceCapability_Rpc{
				Rpc: &csipb.NodeServiceCapability_RPC{
					Type: csipb.NodeServiceCapability_RPC_EXPAND_VOLUME,
				},
			},
		})
	}

	if f.volumeStatsSet {
		resp.Capabilities = append(resp.Capabilities, &csipb.NodeServiceCapability{
			Type: &csipb.NodeServiceCapability_Rpc{
				Rpc: &csipb.NodeServiceCapability_RPC{
					Type: csipb.NodeServiceCapability_RPC_GET_VOLUME_STATS,
				},
			},
		})
	}

	if f.volumeHealthSet {
		resp.Capabilities = append(resp.Capabilities, &csipb.NodeServiceCapability{
			Type: &csipb.NodeServiceCapability_Rpc{
				Rpc: &csipb.NodeServiceCapability_RPC{
					Type: csipb.NodeServiceCapability_RPC_GET_VOLUME_HEALTH,
				},
			},
		})
	}

	if f.storageHealthSet {
		resp.Capabilities = append(resp.Capabilities, &csipb.NodeServiceCapability{
			Type: &csipb.NodeServiceCapability_Rpc{
				Rpc: &csipb.NodeServiceCapability_RPC{
					Type: csipb.NodeServiceCapability_RPC_GET_STORAGE_HEALTH,
				},
			},
		})
	}

	if f.singleNodeMultiWriterSet {
		resp.Capabilities = append(resp.Capabilities, &csipb.NodeServiceCapability{
			Type: &csipb.NodeServiceCapability_Rpc{
				Rpc: &csipb.NodeServiceCapability_RPC{
					Type: csipb.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
				},
			},
		})
	}

	if f.volumeMountGroupSet {
		resp.Capabilities = append(resp.Capabilities, &csipb.NodeServiceCapability{
			Type: &csipb.NodeServiceCapability_Rpc{
				Rpc: &csipb.NodeServiceCapability_RPC{
					Type: csipb.NodeServiceCapability_RPC_VOLUME_MOUNT_GROUP,
				},
			},
		})
	}
	return resp, nil
}

/*
// NodeGetVolumeStats 实现 CSI NodeGetVolumeStats（旧版空实现，已被下方版本取代）。
func (f *NodeClient) NodeGetVolumeStats(ctx context.Context, in *csipb.NodeGetVolumeStatsRequest, opts ...grpc.CallOption) (*csipb.NodeGetVolumeStatsResponse, error) {
	return nil, nil
}
*/

// NodeGetVolumeStats 实现 CSI NodeGetVolumeStats：返回预设响应或空响应。
func (f *NodeClient) NodeGetVolumeStats(ctx context.Context, req *csipb.NodeGetVolumeStatsRequest, opts ...grpc.CallOption) (*csipb.NodeGetVolumeStatsResponse, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}
	if req.GetVolumeId() == "" {
		return nil, errors.New("missing volume id")
	}
	if req.GetVolumePath() == "" {
		return nil, errors.New("missing Volume path")
	}
	if f.nodeVolumeStatsResp != nil {
		return f.nodeVolumeStatsResp, nil
	}
	return &csipb.NodeGetVolumeStatsResponse{}, nil
}

// NodeGetVolumeHealth 实现 CSI NodeGetVolumeHealth：
// SetVolumeHealth 开启时返回降级的卷健康状态。
func (f *NodeClient) NodeGetVolumeHealth(ctx context.Context, req *csipb.NodeGetVolumeHealthRequest, opts ...grpc.CallOption) (*csipb.NodeGetVolumeHealthResponse, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}
	resp := &csipb.NodeGetVolumeHealthResponse{}
	if f.SetVolumeHealth {
		resp.VolumeHealth = &csipb.VolumeHealth{
			VolumeId: req.GetVolumeId(),
			HealthStatuses: []*csipb.VolumeHealth_VolumeHealthEntry{
				{
					Status:  csipb.VolumeHealthErrorType_DEGRADED,
					Reason:  "FakeHealthIssue",
					Message: "fake volume health issue",
				},
			},
		}
	}
	return resp, nil
}

// NodeGetStorageHealth 实现 CSI NodeGetStorageHealth：
// SetStorageHealth 开启时返回降级的存储后端健康状态。
func (f *NodeClient) NodeGetStorageHealth(ctx context.Context, req *csipb.NodeGetStorageHealthRequest, opts ...grpc.CallOption) (*csipb.NodeGetStorageHealthResponse, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}
	resp := &csipb.NodeGetStorageHealthResponse{}
	if f.SetStorageHealth {
		resp.BackendHealth = []*csipb.NodeGetStorageHealthResponse_StorageBackendHealth{
			{
				Status:  csipb.StorageHealthErrorType_STORAGE_DEGRADED,
				Reason:  "FakeStorageHealthIssue",
				Message: "fake storage backend health issue",
			},
		}
	}
	return resp, nil
}

// NewNodeClientWithStorageHealth 创建带存储健康检查能力的假 node 客户端。
func NewNodeClientWithStorageHealth(storageHealth, setStorageHealth bool) *NodeClient {
	return &NodeClient{
		storageHealthSet: storageHealth,
		SetStorageHealth: setStorageHealth,
	}
}

// ControllerClient 是测试用的 CSI controller 客户端，可注入错误和能力列表。
type ControllerClient struct {
	nextCapabilities []*csipb.ControllerServiceCapability
	nextErr          error
}

// SetNextError 注入下次 RPC 调用期望返回的错误。
func (f *ControllerClient) SetNextError(err error) {
	f.nextErr = err
}

// SetNextCapabilities 注入期望返回的能力列表。
func (f *ControllerClient) SetNextCapabilities(caps []*csipb.ControllerServiceCapability) {
	f.nextCapabilities = caps
}

// ControllerGetCapabilities 返回预设能力列表，未设置时默认包含 PUBLISH_UNPUBLISH_VOLUME。
func (f *ControllerClient) ControllerGetCapabilities(ctx context.Context, in *csipb.ControllerGetCapabilitiesRequest, opts ...grpc.CallOption) (*csipb.ControllerGetCapabilitiesResponse, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}

	if f.nextCapabilities == nil {
		f.nextCapabilities = []*csipb.ControllerServiceCapability{
			{
				Type: &csipb.ControllerServiceCapability_Rpc{
					Rpc: &csipb.ControllerServiceCapability_RPC{
						Type: csipb.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
					},
				},
			},
		}
	}
	return &csipb.ControllerGetCapabilitiesResponse{
		Capabilities: f.nextCapabilities,
	}, nil
}

// CreateVolume 实现 CSI CreateVolume（假实现）。
func (f *ControllerClient) CreateVolume(ctx context.Context, in *csipb.CreateVolumeRequest, opts ...grpc.CallOption) (*csipb.CreateVolumeResponse, error) {
	return nil, nil
}

// DeleteVolume 实现 CSI DeleteVolume（假实现）。
func (f *ControllerClient) DeleteVolume(ctx context.Context, in *csipb.DeleteVolumeRequest, opts ...grpc.CallOption) (*csipb.DeleteVolumeResponse, error) {
	return nil, nil
}

// ControllerPublishVolume 实现 CSI ControllerPublishVolume（假实现）。
func (f *ControllerClient) ControllerPublishVolume(ctx context.Context, in *csipb.ControllerPublishVolumeRequest, opts ...grpc.CallOption) (*csipb.ControllerPublishVolumeResponse, error) {
	return nil, nil
}

// ControllerUnpublishVolume 实现 CSI ControllerUnpublishVolume（假实现）。
func (f *ControllerClient) ControllerUnpublishVolume(ctx context.Context, in *csipb.ControllerUnpublishVolumeRequest, opts ...grpc.CallOption) (*csipb.ControllerUnpublishVolumeResponse, error) {
	return nil, nil
}

// ValidateVolumeCapabilities 实现 CSI ValidateVolumeCapabilities（假实现）。
func (f *ControllerClient) ValidateVolumeCapabilities(ctx context.Context, in *csipb.ValidateVolumeCapabilitiesRequest, opts ...grpc.CallOption) (*csipb.ValidateVolumeCapabilitiesResponse, error) {
	return nil, nil
}

// ListVolumes 实现 CSI ListVolumes（假实现）。
func (f *ControllerClient) ListVolumes(ctx context.Context, in *csipb.ListVolumesRequest, opts ...grpc.CallOption) (*csipb.ListVolumesResponse, error) {
	return nil, nil
}

// GetCapacity 实现 CSI GetCapacity（假实现）。
func (f *ControllerClient) GetCapacity(ctx context.Context, in *csipb.GetCapacityRequest, opts ...grpc.CallOption) (*csipb.GetCapacityResponse, error) {
	return nil, nil
}
