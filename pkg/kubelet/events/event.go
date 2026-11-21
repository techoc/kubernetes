/*
Copyright 2014 The Kubernetes Authors.

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

package events

// 容器事件原因列表。
const (
	CreatedContainer        = "Created"
	StartedContainer        = "Started"
	FailedToCreateContainer = "Failed"
	FailedToStartContainer  = "Failed"
	KillingContainer        = "Killing"
	PreemptContainer        = "Preempting"
	BackOffStartContainer   = "BackOff"
	ExceededGracePeriod     = "ExceededGracePeriod"
)

// Pod 事件原因列表。
const (
	FailedToKillPod                 = "FailedKillPod"
	FailedToCreatePodContainer      = "FailedCreatePodContainer"
	FailedToMakePodDataDirectories  = "Failed"
	NetworkNotReady                 = "NetworkNotReady"
	ResizeDeferred                  = "ResizeDeferred"
	ResizeInfeasible                = "ResizeInfeasible"
	ResizeCompleted                 = "ResizeCompleted"
	ResizeStarted                   = "ResizeStarted"
	ResizeError                     = "ResizeError"
	FailedNodeDeclaredFeaturesCheck = "FailedNodeDeclaredFeaturesCheck"
)

// 镜像事件原因列表。
const (
	PullingImage            = "Pulling"
	PulledImage             = "Pulled"
	FailedToPullImage       = "Failed"
	FailedToInspectImage    = "InspectFailed"
	ErrImageNeverPullPolicy = "ErrImageNeverPull"
	BackOffPullImage        = "BackOff"
)

// kubelet 事件原因列表。
const (
	NodeReady                            = "NodeReady"
	NodeNotReady                         = "NodeNotReady"
	NodeSchedulable                      = "NodeSchedulable"
	NodeNotSchedulable                   = "NodeNotSchedulable"
	StartingKubelet                      = "Starting"
	KubeletSetupFailed                   = "KubeletSetupFailed"
	FailedAttachVolume                   = "FailedAttachVolume"
	FailedMountVolume                    = "FailedMount"
	VolumeResizeFailed                   = "VolumeResizeFailed"
	VolumeResizeSuccess                  = "VolumeResizeSuccessful"
	FileSystemResizeFailed               = "FileSystemResizeFailed"
	VolumePermissionChangeInProgress     = "VolumePermissionChangeInProgress"
	FileSystemResizeSuccess              = "FileSystemResizeSuccessful"
	FailedMapVolume                      = "FailedMapVolume"
	WarnAlreadyMountedVolume             = "AlreadyMountedVolume"
	SuccessfulAttachVolume               = "SuccessfulAttachVolume"
	SuccessfulMountVolume                = "SuccessfulMountVolume"
	NodeRebooted                         = "Rebooted"
	NodeShutdown                         = "Shutdown"
	ContainerGCFailed                    = "ContainerGCFailed"
	ImageGCFailed                        = "ImageGCFailed"
	FailedNodeAllocatableEnforcement     = "FailedNodeAllocatableEnforcement"
	SuccessfulNodeAllocatableEnforcement = "NodeAllocatableEnforced"
	SandboxChanged                       = "SandboxChanged"
	FailedCreatePodSandBox               = "FailedCreatePodSandBox"
	FailedStatusPodSandBox               = "FailedPodSandBoxStatus"
	FailedMountOnFilesystemMismatch      = "FailedMountOnFilesystemMismatch"
	FailedPrepareDynamicResources        = "FailedPrepareDynamicResources"
	PossibleMemoryBackedVolumesOnDisk    = "PossibleMemoryBackedVolumesOnDisk"
	CgroupV1                             = "CgroupV1"
)

// 镜像管理器事件原因列表。
const (
	InvalidImageID      = "InvalidImageID"
	InvalidDiskCapacity = "InvalidDiskCapacity"
	FreeDiskSpaceFailed = "FreeDiskSpaceFailed"
)

// 探针事件原因列表。
const (
	ContainerUnhealthy    = "Unhealthy"
	ContainerProbeWarning = "ProbeWarning"
)

// Pod worker 事件原因列表。
const (
	FailedSync = "FailedSync"
)

// 配置事件原因列表。
const (
	FailedValidation = "FailedValidation"
)

// 生命周期钩子事件原因列表。
const (
	FailedPostStartHook = "FailedPostStartHook"
	FailedPreStopHook   = "FailedPreStopHook"
)
