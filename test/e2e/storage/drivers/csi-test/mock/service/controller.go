/*
Copyright 2021 The Kubernetes Authors.

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

package service

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"strconv"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/klog/v2"
)

const (
	MaxStorageCapacity = tib
	ReadOnlyKey        = "readonly"
)

func (s *service) CreateVolume(
	ctx context.Context,
	req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {

	if len(req.Name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	if req.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	// 检查 volume 是否已存在。
	if i, v := s.findVolByName(ctx, req.Name); i >= 0 {
		// 请求的 volume 名称已存在，需检查现有 volume 的容量是否不小于新请求的容量。
		if v.GetCapacityBytes() < req.GetCapacityRange().GetRequiredBytes() {
			return nil, status.Error(codes.AlreadyExists,
				fmt.Sprintf("Volume with name %s already exists", req.GetName()))
		}
		return &csi.CreateVolumeResponse{Volume: v}, nil
	}

	// 未指定容量时使用 100GiB
	capacity := gib100
	if cr := req.CapacityRange; cr != nil {
		if rb := cr.RequiredBytes; rb > 0 {
			capacity = rb
		}
		if lb := cr.LimitBytes; lb > 0 {
			capacity = lb
		}
	}
	// 检查是否超过最大可用容量
	if capacity >= MaxStorageCapacity {
		return nil, status.Errorf(codes.OutOfRange, "Requested capacity %d exceeds maximum allowed %d", capacity, MaxStorageCapacity)
	}

	var v *csi.Volume
	// 如果提供了 content source，则从它创建 volume。
	if req.GetVolumeContentSource() != nil {
		switch req.GetVolumeContentSource().GetType().(type) {
		case *csi.VolumeContentSource_Snapshot:
			sid := req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()
			// 检查 source snapshot 是否存在。
			if snapID, _ := s.snapshots.FindSnapshot("id", sid); snapID >= 0 {
				v = s.newVolumeFromSnapshot(req.Name, capacity, snapID)
			} else {
				return nil, status.Errorf(codes.NotFound, "Requested source snapshot %s not found", sid)
			}
		case *csi.VolumeContentSource_Volume:
			vid := req.GetVolumeContentSource().GetVolume().GetVolumeId()
			// 检查 source volume 是否存在。
			if volID, _ := s.findVolNoLock("id", vid); volID >= 0 {
				v = s.newVolumeFromVolume(req.Name, capacity, volID)
			} else {
				return nil, status.Errorf(codes.NotFound, "Requested source volume %s not found", vid)
			}
		}
	} else {
		v = s.newVolume(req.Name, capacity)
	}

	// 将创建的 volume 加入 service 的内存 volume 切片。
	s.volsRWL.Lock()
	defer s.volsRWL.Unlock()
	s.vols = append(s.vols, v)
	MockVolumes[v.GetVolumeId()] = Volume{
		VolumeCSI:       v,
		NodeID:          "",
		ISStaged:        false,
		ISPublished:     false,
		StageTargetPath: "",
		TargetPath:      "",
	}

	if hookVal, hookMsg := s.execHook("CreateVolumeEnd"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	return &csi.CreateVolumeResponse{Volume: v}, nil
}

func (s *service) DeleteVolume(
	ctx context.Context,
	req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {

	s.volsRWL.Lock()
	defer s.volsRWL.Unlock()

	//  未指定 volume 时返回错误
	if len(req.VolumeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}

	if hookVal, hookMsg := s.execHook("DeleteVolumeStart"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	// volume 不存在时返回幂等响应。
	i, _ := s.findVolNoLock("id", req.VolumeId)
	if i < 0 {
		return &csi.DeleteVolumeResponse{}, nil
	}

	// 这种删除方式保留元素顺序，并避免潜在的内存泄漏。
	// 切片元素本身可能不是指针，但结构体中的字段是指针。
	copy(s.vols[i:], s.vols[i+1:])
	s.vols[len(s.vols)-1] = nil
	s.vols = s.vols[:len(s.vols)-1]
	klog.V(5).InfoS("mock delete volume", "volumeID", req.VolumeId)

	if hookVal, hookMsg := s.execHook("DeleteVolumeEnd"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

func (s *service) ControllerPublishVolume(
	ctx context.Context,
	req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {

	if s.config.DisableAttach {
		return nil, status.Error(codes.Unimplemented, "ControllerPublish is not supported")
	}

	if len(req.VolumeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}
	if len(req.NodeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Node ID cannot be empty")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	if req.NodeId != s.nodeID {
		return nil, status.Errorf(codes.NotFound, "Not matching Node ID %s to Mock Node ID %s", req.NodeId, s.nodeID)
	}

	if hookVal, hookMsg := s.execHook("ControllerPublishVolumeStart"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	s.volsRWL.Lock()
	defer s.volsRWL.Unlock()

	i, v := s.findVolNoLock("id", req.VolumeId)
	if i < 0 {
		return nil, status.Error(codes.NotFound, req.VolumeId)
	}

	// devPathKey 是 volume attributes 中的键；当 volume 被 controller
	// publish 到指定节点时，其值会被设为 mock 设备路径。
	devPathKey := path.Join(req.NodeId, "dev")

	// 检查 volume 是否已 publish。
	if device := v.VolumeContext[devPathKey]; device != "" {
		var volRo bool
		var roVal string
		if ro, ok := v.VolumeContext[ReadOnlyKey]; ok {
			roVal = ro
		}

		if roVal == "true" {
			volRo = true
		} else {
			volRo = false
		}

		// 检查 readonly 标志与 publish 请求是否兼容。
		if req.GetReadonly() != volRo {
			return nil, status.Error(codes.AlreadyExists, "Volume published but has incompatible readonly flag")
		}

		return &csi.ControllerPublishVolumeResponse{
			PublishContext: map[string]string{
				"device":   device,
				"readonly": roVal,
			},
		}, nil
	}

	// 仅在设置了 attach 上限时，publish 前检查该上限。
	if s.config.AttachLimit > 0 && s.getAttachCount(devPathKey) >= s.config.AttachLimit {
		return nil, status.Errorf(codes.ResourceExhausted, "Cannot attach any more volumes to this node")
	}

	var roVal string
	if req.GetReadonly() {
		roVal = "true"
	} else {
		roVal = "false"
	}

	// publish 该 volume。
	device := "/dev/mock"
	v.VolumeContext[devPathKey] = device
	v.VolumeContext[ReadOnlyKey] = roVal
	s.vols[i] = v

	if volInfo, ok := MockVolumes[req.VolumeId]; ok {
		volInfo.ISControllerPublished = true
		MockVolumes[req.VolumeId] = volInfo
	}

	if hookVal, hookMsg := s.execHook("ControllerPublishVolumeEnd"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	return &csi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{
			"device":   device,
			"readonly": roVal,
		},
	}, nil
}

func (s *service) ControllerUnpublishVolume(
	ctx context.Context,
	req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {

	if s.config.DisableAttach {
		return nil, status.Error(codes.Unimplemented, "ControllerPublish is not supported")
	}

	if len(req.VolumeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}
	nodeID := req.NodeId
	if len(nodeID) == 0 {
		// 按 Spec，node id 为空不算错误
		nodeID = s.nodeID
	}

	if req.NodeId != s.nodeID {
		return nil, status.Errorf(codes.NotFound, "Node ID %s does not match to expected Node ID %s", req.NodeId, s.nodeID)
	}

	if hookVal, hookMsg := s.execHook("ControllerUnpublishVolumeStart"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	s.volsRWL.Lock()
	defer s.volsRWL.Unlock()

	i, v := s.findVolNoLock("id", req.VolumeId)
	if i < 0 {
		// 不算错误：不存在的 volume 自然未被 publish。
		// 另见 https://github.com/kubernetes-csi/external-attacher/pull/165
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	// devPathKey 是 volume attributes 中的键；当 volume 被 controller
	// publish 到指定节点时，其值会被设为 mock 设备路径。
	devPathKey := path.Join(nodeID, "dev")

	// 检查 volume 是否已 unpublish。
	if v.VolumeContext[devPathKey] == "" {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	// unpublish 该 volume。
	delete(v.VolumeContext, devPathKey)
	delete(v.VolumeContext, ReadOnlyKey)
	s.vols[i] = v

	if hookVal, hookMsg := s.execHook("ControllerUnpublishVolumeEnd"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (s *service) ValidateVolumeCapabilities(
	ctx context.Context,
	req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {

	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}
	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, req.VolumeId)
	}
	i, _ := s.findVolNoLock("id", req.VolumeId)
	if i < 0 {
		return nil, status.Error(codes.NotFound, req.VolumeId)
	}

	if hookVal, hookMsg := s.execHook("ValidateVolumeCapabilities"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: req.GetVolumeCapabilities(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

func (s *service) ControllerGetVolume(
	ctx context.Context,
	req *csi.ControllerGetVolumeRequest) (
	*csi.ControllerGetVolumeResponse, error) {

	if hookVal, hookMsg := s.execHook("GetVolumeStart"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	resp := &csi.ControllerGetVolumeResponse{
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{},
	}
	i, v := s.findVolByID(ctx, req.VolumeId)
	if i < 0 {
		return resp, status.Error(codes.NotFound, req.VolumeId)
	}

	resp.Volume = v
	if !s.config.DisableAttach {
		resp.Status.PublishedNodeIds = []string{
			s.nodeID,
		}
	}

	if hookVal, hookMsg := s.execHook("GetVolumeEnd"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	return resp, nil
}

func (s *service) ListVolumes(
	ctx context.Context,
	req *csi.ListVolumesRequest) (
	*csi.ListVolumesResponse, error) {

	if hookVal, hookMsg := s.execHook("ListVolumesStart"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	// 将 mock volume 复制到新切片，避免在整个 ListVolumes RPC 期间
	// 一直持有 service 的 volume 切片锁。
	var vols []*csi.Volume
	func() {
		s.volsRWL.RLock()
		defer s.volsRWL.RUnlock()
		vols = make([]*csi.Volume, len(s.vols))
		copy(vols, s.vols)
	}()

	var (
		ulenVols      = int32(len(vols))
		maxEntries    = req.MaxEntries
		startingToken int32
	)

	if v := req.StartingToken; v != "" {
		i, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return nil, status.Errorf(
				codes.Aborted,
				"startingToken=%s: %v",
				v, err)
		}
		startingToken = int32(i)
	}

	if startingToken > ulenVols {
		return nil, status.Errorf(
			codes.Aborted,
			"startingToken=%d > len(vols)=%d",
			startingToken, ulenVols)
	}

	// 计算剩余条目数。
	rem := ulenVols - startingToken

	// 若 maxEntries 为 0 或大于剩余条目数，则将其设为剩余条目数。
	if maxEntries == 0 || maxEntries > rem {
		maxEntries = rem
	}

	var (
		i       int
		j       = startingToken
		entries = make(
			[]*csi.ListVolumesResponse_Entry,
			maxEntries)
	)

	for i = 0; i < len(entries); i++ {
		volumeStatus := &csi.ListVolumesResponse_VolumeStatus{}

		if !s.config.DisableAttach {
			volumeStatus.PublishedNodeIds = []string{
				s.nodeID,
			}
		}

		entries[i] = &csi.ListVolumesResponse_Entry{
			Volume: vols[j],
			Status: volumeStatus,
		}
		j++
	}

	var nextToken string
	if n := startingToken + int32(i); n < ulenVols {
		nextToken = fmt.Sprintf("%d", n)
	}

	if hookVal, hookMsg := s.execHook("ListVolumesEnd"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	return &csi.ListVolumesResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

func (s *service) GetCapacity(
	ctx context.Context,
	req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse, error) {

	if hookVal, hookMsg := s.execHook("GetCapacity"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	return &csi.GetCapacityResponse{
		AvailableCapacity: MaxStorageCapacity,
	}, nil
}

func (s *service) ControllerGetCapabilities(
	ctx context.Context,
	req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse, error) {

	if hookVal, hookMsg := s.execHook("ControllerGetCapabilitiesStart"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	caps := []*csi.ControllerServiceCapability{
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_CAPACITY,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_PUBLISH_READONLY,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_VOLUME_HEALTH,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_MODIFY_VOLUME,
				},
			},
		},
	}

	if !s.config.DisableAttach {
		caps = append(caps, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
				},
			},
		})
	}

	if !s.config.DisableControllerExpansion {
		caps = append(caps, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
				},
			},
		})
	}

	if hookVal, hookMsg := s.execHook("ControllerGetCapabilitiesEnd"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: caps,
	}, nil
}

func (s *service) CreateSnapshot(ctx context.Context,
	req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	// 检查参数
	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot Name cannot be empty")
	}
	if len(req.GetSourceVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot SourceVolumeId cannot be empty")
	}

	// 检查 snapshot 是否已存在。
	if i, v := s.snapshots.FindSnapshot("name", req.GetName()); i >= 0 {
		// 请求的 snapshot 名称已存在
		if v.SnapshotCSI.GetSourceVolumeId() != req.GetSourceVolumeId() || !reflect.DeepEqual(v.Parameters, req.GetParameters()) {
			return nil, status.Error(codes.AlreadyExists,
				fmt.Sprintf("Snapshot with name %s already exists", req.GetName()))
		}
		return &csi.CreateSnapshotResponse{Snapshot: v.SnapshotCSI}, nil
	}

	// 创建 snapshot 并加入 service 的内存 snapshot 切片。
	snapshot := s.newSnapshot(req.GetName(), req.GetSourceVolumeId(), req.GetParameters())
	s.snapshots.Add(snapshot)

	if hookVal, hookMsg := s.execHook("CreateSnapshotEnd"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	return &csi.CreateSnapshotResponse{Snapshot: snapshot.SnapshotCSI}, nil
}

func (s *service) DeleteSnapshot(ctx context.Context,
	req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {

	//  未指定 snapshot 时返回错误
	if len(req.SnapshotId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID cannot be empty")
	}

	if hookVal, hookMsg := s.execHook("DeleteSnapshotStart"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	// snapshot 不存在时返回幂等响应。
	i, _ := s.snapshots.FindSnapshot("id", req.SnapshotId)
	if i < 0 {
		return &csi.DeleteSnapshotResponse{}, nil
	}

	// 这种删除方式保留元素顺序，并避免潜在的内存泄漏。
	// 切片元素本身可能不是指针，但结构体中的字段是指针。
	s.snapshots.Delete(i)
	klog.V(5).InfoS("mock delete snapshot", "snapshotId", req.SnapshotId)

	if hookVal, hookMsg := s.execHook("DeleteSnapshotEnd"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func (s *service) ListSnapshots(ctx context.Context,
	req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {

	if hookVal, hookMsg := s.execHook("ListSnapshots"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	// 情况 1：SnapshotId 非空，返回与该 snapshot id 匹配的 snapshot。
	if len(req.GetSnapshotId()) != 0 {
		return getSnapshotById(s, req)
	}

	// 情况 2：SourceVolumeId 非空，返回与该 source volume id 匹配的 snapshot。
	if len(req.GetSourceVolumeId()) != 0 {
		return getSnapshotByVolumeId(s, req)
	}

	// 情况 3：未设置任何参数，返回所有 snapshot。
	return getAllSnapshots(s, req)
}

func (s *service) ControllerExpandVolume(
	ctx context.Context,
	req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if len(req.VolumeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}

	if req.CapacityRange == nil {
		return nil, status.Error(codes.InvalidArgument, "Request capacity cannot be empty")
	}

	if hookVal, hookMsg := s.execHook("ControllerExpandVolumeStart"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	s.volsRWL.Lock()
	defer s.volsRWL.Unlock()

	i, v := s.findVolNoLock("id", req.VolumeId)
	if i < 0 {
		return nil, status.Error(codes.NotFound, req.VolumeId)
	}

	if s.config.DisableOnlineExpansion && MockVolumes[v.GetVolumeId()].ISControllerPublished {
		return nil, status.Error(codes.FailedPrecondition, "volume is published and online volume expansion is not supported")
	}

	requestBytes := req.CapacityRange.RequiredBytes

	if v.CapacityBytes > requestBytes {
		return nil, status.Error(codes.InvalidArgument, "cannot change volume capacity to a smaller size")
	}

	resp := &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         requestBytes,
		NodeExpansionRequired: s.config.NodeExpansionRequired,
	}

	// 检查 volume 当前容量是否已满足请求大小。
	if v.CapacityBytes == requestBytes {
		klog.V(5).InfoS("volume capacity sufficient, no need to expand", "requested", requestBytes, "current", v.CapacityBytes, "volumeID", v.VolumeId)
		return resp, nil
	}

	// 将 volume 容量更新为请求的大小。
	v.CapacityBytes = requestBytes
	s.vols[i] = v

	if hookVal, hookMsg := s.execHook("ControllerExpandVolumeEnd"); hookVal != codes.OK {
		return nil, status.Error(hookVal, hookMsg)
	}

	return resp, nil
}

func (s *service) ControllerModifyVolume(
	ctx context.Context,
	req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	// todo: 添加 modifyVolume 测试时实现该功能
	resp := &csi.ControllerModifyVolumeResponse{}
	return resp, nil
}

func getSnapshotById(s *service, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	if len(req.GetSnapshotId()) != 0 {
		i, snapshot := s.snapshots.FindSnapshot("id", req.GetSnapshotId())
		if i < 0 {
			return &csi.ListSnapshotsResponse{}, nil
		}

		if len(req.GetSourceVolumeId()) != 0 {
			if snapshot.SnapshotCSI.GetSourceVolumeId() != req.GetSourceVolumeId() {
				return &csi.ListSnapshotsResponse{}, nil
			}
		}

		return &csi.ListSnapshotsResponse{
			Entries: []*csi.ListSnapshotsResponse_Entry{
				{
					Snapshot: snapshot.SnapshotCSI,
				},
			},
		}, nil
	}
	return nil, nil
}

func getSnapshotByVolumeId(s *service, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	if len(req.GetSourceVolumeId()) != 0 {
		i, snapshot := s.snapshots.FindSnapshot("sourceVolumeId", req.SourceVolumeId)
		if i < 0 {
			return &csi.ListSnapshotsResponse{}, nil
		}
		return &csi.ListSnapshotsResponse{
			Entries: []*csi.ListSnapshotsResponse_Entry{
				{
					Snapshot: snapshot.SnapshotCSI,
				},
			},
		}, nil
	}
	return nil, nil
}

func getAllSnapshots(s *service, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	// 将 mock snapshot 复制到新切片，避免在整个 ListSnapshots RPC 期间
	// 一直持有 service 的 snapshot 切片锁。
	readyToUse := true
	snapshots := s.snapshots.List(readyToUse)

	var (
		ulenSnapshots = int32(len(snapshots))
		maxEntries    = req.MaxEntries
		startingToken int32
	)

	if v := req.StartingToken; v != "" {
		i, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return nil, status.Errorf(
				codes.Aborted,
				"startingToken=%s: %v",
				v, err)
		}
		startingToken = int32(i)
	}

	if startingToken > ulenSnapshots {
		return nil, status.Errorf(
			codes.Aborted,
			"startingToken=%d > len(snapshots)=%d",
			startingToken, ulenSnapshots)
	}

	// 计算剩余条目数。
	rem := ulenSnapshots - startingToken

	// 若 maxEntries 为 0 或大于剩余条目数，则将其设为剩余条目数。
	if maxEntries == 0 || maxEntries > rem {
		maxEntries = rem
	}

	var (
		i       int
		j       = startingToken
		entries = make(
			[]*csi.ListSnapshotsResponse_Entry,
			maxEntries)
	)

	for i = 0; i < len(entries); i++ {
		entries[i] = &csi.ListSnapshotsResponse_Entry{
			Snapshot: snapshots[j],
		}
		j++
	}

	var nextToken string
	if n := startingToken + int32(i); n < ulenSnapshots {
		nextToken = fmt.Sprintf("%d", n)
	}

	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}
