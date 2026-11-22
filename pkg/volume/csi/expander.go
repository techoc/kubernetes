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

package csi

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	api "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

var _ volume.NodeExpandableVolumePlugin = &csiPlugin{}

// RequiresFSResize 表示 CSI 插件需要由 kubelet 执行文件系统扩容。
func (c *csiPlugin) RequiresFSResize() bool {
	return true
}

// NodeExpand 调用 CSI 驱动的 NodeExpandVolume 完成节点侧卷扩容。
func (c *csiPlugin) NodeExpand(resizeOptions volume.NodeResizeOptions) (bool, error) {
	klog.V(4).Info(log("Expander.NodeExpand(%s)", resizeOptions.DeviceMountPath))
	csiSource, err := getCSISourceFromSpec(resizeOptions.VolumeSpec)
	if err != nil {
		return false, errors.New(log("Expander.NodeExpand failed to get CSI persistent source: %v", err))
	}

	csClient, err := newCsiDriverClient(csiDriverName(csiSource.Driver))
	if err != nil {
		// 驱动不存在按瞬时错误处理，详见
		// https://github.com/kubernetes/kubernetes/issues/120268。
		return false, volumetypes.NewTransientOperationFailure(err.Error())
	}
	fsVolume, err := util.CheckVolumeModeFilesystem(resizeOptions.VolumeSpec)
	if err != nil {
		return false, errors.New(log("Expander.NodeExpand failed to check VolumeMode of source: %v", err))
	}

	return c.nodeExpandWithClient(resizeOptions, csiSource, csClient, fsVolume)
}

// nodeExpandWithClient 使用传入的客户端执行扩容，并把 CSI 错误分类为 kubelet 可重试类型。
func (c *csiPlugin) nodeExpandWithClient(
	resizeOptions volume.NodeResizeOptions,
	csiSource *api.CSIPersistentVolumeSource,
	csClient csiClient,
	fsVolume bool) (bool, error) {
	driverName := csiSource.Driver

	ctx, cancel := createCSIOperationContext(resizeOptions.VolumeSpec, csiTimeout)
	defer cancel()

	nodeExpandSet, err := csClient.NodeSupportsNodeExpand(ctx)
	if err != nil {
		return false, fmt.Errorf("Expander.NodeExpand failed to check if node supports expansion : %v", err)
	}

	if !nodeExpandSet {
		return false, volumetypes.NewOperationNotSupportedError(fmt.Sprintf("NodeExpand is not supported by the CSI driver %s", driverName))
	}

	pv := resizeOptions.VolumeSpec.PersistentVolume
	if pv == nil {
		return false, fmt.Errorf("Expander.NodeExpand failed to find associated PersistentVolume for plugin %s", c.GetPluginName())
	}
	nodeExpandSecrets := map[string]string{}
	expandClient := c.host.GetKubeClient()

	if csiSource.NodeExpandSecretRef != nil {
		nodeExpandSecrets, err = getCredentialsFromSecret(expandClient, csiSource.NodeExpandSecretRef)
		if err != nil {
			return false, fmt.Errorf("expander.NodeExpand failed to get NodeExpandSecretRef %s/%s: %v",
				csiSource.NodeExpandSecretRef.Namespace, csiSource.NodeExpandSecretRef.Name, err)
		}
	}

	opts := csiResizeOptions{
		volumePath:        resizeOptions.DeviceMountPath,
		stagingTargetPath: resizeOptions.DeviceStagePath,
		volumeID:          csiSource.VolumeHandle,
		newSize:           resizeOptions.NewSize,
		fsType:            csiSource.FSType,
		accessMode:        api.ReadWriteOnce,
		mountOptions:      pv.Spec.MountOptions,
		secrets:           nodeExpandSecrets,
	}

	if !fsVolume {
		// 块设备没有挂载点，resizeOptions.DeviceMountPath 为空，因此扩容请求
		// 中的 volumePath 使用 DevicePath。
		opts.volumePath = resizeOptions.DevicePath
		opts.fsType = fsTypeBlockName
	}

	if pv.Spec.AccessModes != nil {
		opts.accessMode = pv.Spec.AccessModes[0]
	}

	_, err = csClient.NodeExpandVolume(ctx, opts)
	if err != nil {
		if inUseError(err) {
			failedConditionErr := fmt.Errorf("Expander.NodeExpand failed to expand the volume : %w", volumetypes.NewFailedPreconditionError(err.Error()))
			return false, failedConditionErr
		}

		if isInfeasibleError(err) {
			infeasibleError := volumetypes.NewInfeasibleError(fmt.Sprintf("Expander.NodeExpand failed to expand the volume %s", err.Error()))
			return false, infeasibleError
		}
		return false, fmt.Errorf("Expander.NodeExpand failed to expand the volume : %w", err)
	}
	return true, nil
}

func inUseError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		// 非 gRPC 错误，不做分类。
		return false
	}
	// FailedPrecondition 表示驱动不支持对使用中的卷扩容，
	// 详见 https://github.com/container-storage-interface/spec/blob/master/spec.md#controllerexpandvolume-errors。
	return st.Code() == codes.FailedPrecondition
}

// IsInfeasibleError 判断 gRPC 错误是否表示 CSI 操作不可行（终止性错误）。
// 它返回最终错误的子集：所有不可行错误都是最终错误。
func isInfeasibleError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		// 非 gRPC 错误说明失败发生在调用 gRPC 之前；由于无法判断是否有
		// 其他卷操作在进行中，保守起见不视为不可行错误。
		return false
	}
	switch st.Code() {
	case codes.InvalidArgument,
		codes.OutOfRange,
		codes.NotFound:
		return true
	}
	// 其余错误表示操作未开始或已失败，确定不是不可行错误。
	return false
}
