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

package framework

import (
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
)

const (
	// MinFileSize 表示测试用的最小文件大小（1 MiB）
	MinFileSize = 1 * e2evolume.MiB

	// FileSizeSmall 表示测试用的小文件大小（1 MiB）
	FileSizeSmall = 1 * e2evolume.MiB
	// FileSizeMedium 表示测试用的中等文件大小（100 MiB）
	FileSizeMedium = 100 * e2evolume.MiB
	// FileSizeLarge 表示测试用的大文件大小（1 GiB）
	FileSizeLarge = 1 * e2evolume.GiB
)

// TestVolType 表示 TestSuite 中要测试的卷类型
type TestVolType string

var (
	// InlineVolume 表示在 volumeSource 中内联使用的卷类型
	InlineVolume TestVolType = "InlineVolume"
	// PreprovisionedPV 表示预置 Persistent Volume 的卷类型
	PreprovisionedPV TestVolType = "PreprovisionedPV"
	// DynamicPV 表示动态配置 Persistent Volume 的卷类型
	DynamicPV TestVolType = "DynamicPV"
	// CSIInlineVolume 表示内联定义、由 CSI driver 提供的卷类型。
	CSIInlineVolume TestVolType = "CSIInlineVolume"
	// GenericEphemeralVolume 表示内联定义、通过 PVC 配置的卷类型。
	GenericEphemeralVolume TestVolType = "GenericEphemeralVolume"
)

// TestSnapshotType 表示 TestSuite 中要测试的快照类型
type TestSnapshotType string

var (
	// DynamicCreatedSnapshot 表示动态创建的快照类型
	DynamicCreatedSnapshot TestSnapshotType = "DynamicSnapshot"
	// PreprovisionedCreatedSnapshot 表示预置的快照类型
	PreprovisionedCreatedSnapshot TestSnapshotType = "PreprovisionedSnapshot"
	// VolumeGroupSnapshot 表示动态创建的 volumegroupsnapshot 的组快照类型
	VolumeGroupSnapshot TestSnapshotType = "VolumeGroupSnapshot"
	// PreprovisionedCreatedVolumeGroupSnapshot 表示预置的 volumegroupsnapshot 类型
	PreprovisionedCreatedVolumeGroupSnapshot TestSnapshotType = "PreprovisionedVolumeGroupSnapshot"
)

// TestSnapshotDeletionPolicy 表示 snapshot class 的删除策略
type TestSnapshotDeletionPolicy string

var (
	// DeleteSnapshot 表示删除策略
	DeleteSnapshot TestSnapshotDeletionPolicy = "Delete"
	// RetainSnapshot 表示保留策略
	RetainSnapshot TestSnapshotDeletionPolicy = "Retain"
)

func (t TestSnapshotDeletionPolicy) String() string {
	return string(t)
}

// TestPattern 表示 TestSuite 中要测试的一组参数组合
type TestPattern struct {
	Name                   string                      // TestPattern 的名称
	TestTags               []interface{}               // framework.It 的额外参数，例如 framework.WithDisruptive()
	VolType                TestVolType                 // 卷的 Volume type
	FsType                 string                      // 卷的 Fstype
	VolMode                v1.PersistentVolumeMode     // 卷的 PersistentVolumeMode
	SnapshotType           TestSnapshotType            // 快照的 Snapshot type
	SnapshotDeletionPolicy TestSnapshotDeletionPolicy  // snapshot class 的 Deletion policy
	BindingMode            storagev1.VolumeBindingMode // 卷的 VolumeBindingMode
	AllowExpansion         bool                        // StorageClass 的 AllowVolumeExpansion 标志
}

var (
	// 默认 fsType 的定义

	// DefaultFsInlineVolume 对应 "Inline-volume (default fs)" 的 TestPattern
	DefaultFsInlineVolume = TestPattern{
		Name:    "Inline-volume (default fs)",
		VolType: InlineVolume,
	}
	// DefaultFsCSIEphemeralVolume 对应 "CSI Ephemeral-volume (default fs)" 的 TestPattern
	DefaultFsCSIEphemeralVolume = TestPattern{
		Name:    "CSI Ephemeral-volume (default fs)",
		VolType: CSIInlineVolume,
	}
	// DefaultFsGenericEphemeralVolume 对应 "Generic Ephemeral-volume (default fs)" 的 TestPattern
	DefaultFsGenericEphemeralVolume = TestPattern{
		Name:           "Generic Ephemeral-volume (default fs)",
		VolType:        GenericEphemeralVolume,
		AllowExpansion: true,
	}
	// DefaultFsPreprovisionedPV 对应 "Pre-provisioned PV (default fs)" 的 TestPattern
	DefaultFsPreprovisionedPV = TestPattern{
		Name:    "Pre-provisioned PV (default fs)",
		VolType: PreprovisionedPV,
	}
	// DefaultFsDynamicPV 对应 "Dynamic PV (default fs)" 的 TestPattern
	DefaultFsDynamicPV = TestPattern{
		Name:                   "Dynamic PV (default fs)",
		VolType:                DynamicPV,
		SnapshotType:           DynamicCreatedSnapshot,
		SnapshotDeletionPolicy: DeleteSnapshot,
	}

	// ext3 的定义

	// Ext3InlineVolume 对应 "Inline-volume (ext3)" 的 TestPattern
	Ext3InlineVolume = TestPattern{
		Name:    "Inline-volume (ext3)",
		VolType: InlineVolume,
		FsType:  "ext3",
	}
	// Ext3CSIEphemeralVolume 对应 "CSI Ephemeral-volume (ext3)" 的 TestPattern
	Ext3CSIEphemeralVolume = TestPattern{
		Name:    "CSI Ephemeral-volume (ext3)",
		VolType: CSIInlineVolume,
		FsType:  "ext3",
	}
	// Ext3GenericEphemeralVolume 对应 "Generic Ephemeral-volume (ext3)" 的 TestPattern
	Ext3GenericEphemeralVolume = TestPattern{
		Name:    "Generic Ephemeral-volume (ext3)",
		VolType: GenericEphemeralVolume,
		FsType:  "ext3",
	}
	// Ext3PreprovisionedPV 对应 "Pre-provisioned PV (ext3)" 的 TestPattern
	Ext3PreprovisionedPV = TestPattern{
		Name:    "Pre-provisioned PV (ext3)",
		VolType: PreprovisionedPV,
		FsType:  "ext3",
	}
	// Ext3DynamicPV 对应 "Dynamic PV (ext3)" 的 TestPattern
	Ext3DynamicPV = TestPattern{
		Name:    "Dynamic PV (ext3)",
		VolType: DynamicPV,
		FsType:  "ext3",
	}

	// ext4 的定义

	// Ext4InlineVolume 对应 "Inline-volume (ext4)" 的 TestPattern
	Ext4InlineVolume = TestPattern{
		Name:    "Inline-volume (ext4)",
		VolType: InlineVolume,
		FsType:  "ext4",
	}
	// Ext4CSIEphemeralVolume 对应 "CSI Ephemeral-volume (ext4)" 的 TestPattern
	Ext4CSIEphemeralVolume = TestPattern{
		Name:    "CSI Ephemeral-volume (ext4)",
		VolType: CSIInlineVolume,
		FsType:  "ext4",
	}
	// Ext4GenericEphemeralVolume 对应 "Generic Ephemeral-volume (ext4)" 的 TestPattern
	Ext4GenericEphemeralVolume = TestPattern{
		Name:    "Generic Ephemeral-volume (ext4)",
		VolType: GenericEphemeralVolume,
		FsType:  "ext4",
	}
	// Ext4PreprovisionedPV 对应 "Pre-provisioned PV (ext4)" 的 TestPattern
	Ext4PreprovisionedPV = TestPattern{
		Name:    "Pre-provisioned PV (ext4)",
		VolType: PreprovisionedPV,
		FsType:  "ext4",
	}
	// Ext4DynamicPV 对应 "Dynamic PV (ext4)" 的 TestPattern
	Ext4DynamicPV = TestPattern{
		Name:                   "Dynamic PV (ext4)",
		VolType:                DynamicPV,
		FsType:                 "ext4",
		SnapshotType:           DynamicCreatedSnapshot,
		SnapshotDeletionPolicy: DeleteSnapshot,
	}

	// xfs 的定义

	// XfsInlineVolume 对应 "Inline-volume (xfs)" 的 TestPattern
	XfsInlineVolume = TestPattern{
		Name:     "Inline-volume (xfs)",
		VolType:  InlineVolume,
		FsType:   "xfs",
		TestTags: []interface{}{framework.WithSlow()},
	}
	// XfsCSIEphemeralVolume 对应 "CSI Ephemeral-volume (xfs)" 的 TestPattern
	XfsCSIEphemeralVolume = TestPattern{
		Name:     "CSI Ephemeral-volume (xfs)",
		VolType:  CSIInlineVolume,
		FsType:   "xfs",
		TestTags: []interface{}{framework.WithSlow()},
	}
	// XfsGenericEphemeralVolume 对应 "Generic Ephemeral-volume (xfs)" 的 TestPattern
	XfsGenericEphemeralVolume = TestPattern{
		Name:     "Generic Ephemeral-volume (xfs)",
		VolType:  GenericEphemeralVolume,
		FsType:   "xfs",
		TestTags: []interface{}{framework.WithSlow()},
	}
	// XfsPreprovisionedPV 对应 "Pre-provisioned PV (xfs)" 的 TestPattern
	XfsPreprovisionedPV = TestPattern{
		Name:     "Pre-provisioned PV (xfs)",
		VolType:  PreprovisionedPV,
		FsType:   "xfs",
		TestTags: []interface{}{framework.WithSlow()},
	}
	// XfsDynamicPV 对应 "Dynamic PV (xfs)" 的 TestPattern
	XfsDynamicPV = TestPattern{
		Name:                   "Dynamic PV (xfs)",
		VolType:                DynamicPV,
		FsType:                 "xfs",
		TestTags:               []interface{}{framework.WithSlow()},
		SnapshotType:           DynamicCreatedSnapshot,
		SnapshotDeletionPolicy: DeleteSnapshot,
		AllowExpansion:         true,
	}

	// ntfs 的定义

	// NtfsInlineVolume 对应 "Inline-volume (ntfs)" 的 TestPattern
	NtfsInlineVolume = TestPattern{
		Name:     "Inline-volume (ntfs)",
		VolType:  InlineVolume,
		FsType:   "ntfs",
		TestTags: []interface{}{feature.Windows},
	}
	// NtfsCSIEphemeralVolume 对应 "CSI Ephemeral-volume (ntfs)" 的 TestPattern
	NtfsCSIEphemeralVolume = TestPattern{
		Name:     "CSI Ephemeral-volume (ntfs) [alpha]",
		VolType:  CSIInlineVolume,
		FsType:   "ntfs",
		TestTags: []interface{}{feature.Windows},
	}
	// NtfsGenericEphemeralVolume 对应 "Generic Ephemeral-volume (ntfs)" 的 TestPattern
	NtfsGenericEphemeralVolume = TestPattern{
		Name:     "Generic Ephemeral-volume (ntfs)",
		VolType:  GenericEphemeralVolume,
		FsType:   "ntfs",
		TestTags: []interface{}{feature.Windows},
	}
	// NtfsPreprovisionedPV 对应 "Pre-provisioned PV (ntfs)" 的 TestPattern
	NtfsPreprovisionedPV = TestPattern{
		Name:     "Pre-provisioned PV (ntfs)",
		VolType:  PreprovisionedPV,
		FsType:   "ntfs",
		TestTags: []interface{}{feature.Windows},
	}
	// NtfsDynamicPV 对应 "Dynamic PV (ntfs)" 的 TestPattern
	NtfsDynamicPV = TestPattern{
		Name:                   "Dynamic PV (ntfs)",
		VolType:                DynamicPV,
		FsType:                 "ntfs",
		TestTags:               []interface{}{feature.Windows},
		SnapshotDeletionPolicy: DeleteSnapshot,
		SnapshotType:           DynamicCreatedSnapshot,
	}

	// filesystem volume mode 的定义

	// FsVolModePreprovisionedPV 对应 "Pre-provisioned PV (filesystem)" 的 TestPattern
	FsVolModePreprovisionedPV = TestPattern{
		Name:    "Pre-provisioned PV (filesystem volmode)",
		VolType: PreprovisionedPV,
		VolMode: v1.PersistentVolumeFilesystem,
	}
	// FsVolModeDynamicPV 对应 "Dynamic PV (filesystem)" 的 TestPattern
	FsVolModeDynamicPV = TestPattern{
		Name:    "Dynamic PV (filesystem volmode)",
		VolType: DynamicPV,
		VolMode: v1.PersistentVolumeFilesystem,
	}

	// block volume mode 的定义

	// BlockVolModePreprovisionedPV 对应 "Pre-provisioned PV (block)" 的 TestPattern
	BlockVolModePreprovisionedPV = TestPattern{
		Name:    "Pre-provisioned PV (block volmode)",
		VolType: PreprovisionedPV,
		VolMode: v1.PersistentVolumeBlock,
	}
	// BlockVolModeDynamicPV 对应 "Dynamic PV (block)" 的 TestPattern
	BlockVolModeDynamicPV = TestPattern{
		Name:                   "Dynamic PV (block volmode)",
		VolType:                DynamicPV,
		VolMode:                v1.PersistentVolumeBlock,
		SnapshotType:           DynamicCreatedSnapshot,
		SnapshotDeletionPolicy: DeleteSnapshot,
	}
	// BlockVolModeGenericEphemeralVolume 用于 raw block 模式下的 generic ephemeral inline volume。
	BlockVolModeGenericEphemeralVolume = TestPattern{
		Name:           "Generic Ephemeral-volume (block volmode) (late-binding)",
		VolType:        GenericEphemeralVolume,
		VolMode:        v1.PersistentVolumeBlock,
		BindingMode:    storagev1.VolumeBindingWaitForFirstConsumer,
		AllowExpansion: true,
	}

	// snapshot metadata 的定义
	SnapshotMetadata = TestPattern{
		Name:                   "SnapshotMetadata",
		VolType:                DynamicPV,
		VolMode:                v1.PersistentVolumeBlock,
		SnapshotType:           DynamicCreatedSnapshot,
		SnapshotDeletionPolicy: DeleteSnapshot,
	}

	// snapshot 场景的定义

	// DynamicSnapshotDelete 对应 "Dynamic snapshot" 的 TestPattern
	DynamicSnapshotDelete = TestPattern{
		Name:                   "Dynamic Snapshot (delete policy)",
		SnapshotType:           DynamicCreatedSnapshot,
		SnapshotDeletionPolicy: DeleteSnapshot,
		VolType:                DynamicPV,
	}

	// VolumeGroupSnapshotDelete 对应 "VolumeGroupSnapshot" 的 TestPattern
	VolumeGroupSnapshotDelete = TestPattern{
		Name:                   " (delete policy)",
		SnapshotType:           VolumeGroupSnapshot,
		SnapshotDeletionPolicy: DeleteSnapshot,
		VolType:                DynamicPV,
	}

	// VolumeGroupSnapshotRetain 对应 "VolumeGroupSnapshot" 的 TestPattern
	VolumeGroupSnapshotRetain = TestPattern{
		Name:                   " (retain policy)",
		SnapshotType:           VolumeGroupSnapshot,
		SnapshotDeletionPolicy: RetainSnapshot,
		VolType:                DynamicPV,
	}

	// PreprovisionedVolumeGroupSnapshotDelete 对应 "Pre-provisioned VolumeGroupSnapshot" 的 TestPattern
	PreprovisionedVolumeGroupSnapshotDelete = TestPattern{
		Name:                   "Pre-provisioned VolumeGroupSnapshot (delete policy)",
		SnapshotType:           PreprovisionedCreatedVolumeGroupSnapshot,
		SnapshotDeletionPolicy: DeleteSnapshot,
		VolType:                DynamicPV,
	}

	// PreprovisionedVolumeGroupSnapshotRetain 对应 "Pre-provisioned VolumeGroupSnapshot" 的 TestPattern
	PreprovisionedVolumeGroupSnapshotRetain = TestPattern{
		Name:                   "Pre-provisioned VolumeGroupSnapshot (retain policy)",
		SnapshotType:           PreprovisionedCreatedVolumeGroupSnapshot,
		SnapshotDeletionPolicy: RetainSnapshot,
		VolType:                DynamicPV,
	}

	// PreprovisionedSnapshotDelete 对应 "Pre-provisioned snapshot" 的 TestPattern
	PreprovisionedSnapshotDelete = TestPattern{
		Name:                   "Pre-provisioned Snapshot (delete policy)",
		SnapshotType:           PreprovisionedCreatedSnapshot,
		SnapshotDeletionPolicy: DeleteSnapshot,
		VolType:                DynamicPV,
	}
	// EphemeralSnapshotDelete 用于 generic ephemeral volume 的快照测试，
	// 其中快照会被删除。
	EphemeralSnapshotDelete = TestPattern{
		Name:                   "Ephemeral Snapshot (delete policy)",
		SnapshotType:           DynamicCreatedSnapshot,
		SnapshotDeletionPolicy: DeleteSnapshot,
		VolType:                GenericEphemeralVolume,
	}
	// DynamicSnapshotRetain 对应 "Dynamic snapshot" 的 TestPattern
	DynamicSnapshotRetain = TestPattern{
		Name:                   "Dynamic Snapshot (retain policy)",
		SnapshotType:           DynamicCreatedSnapshot,
		SnapshotDeletionPolicy: RetainSnapshot,
		VolType:                DynamicPV,
	}
	// PreprovisionedSnapshotRetain 对应 "Pre-provisioned snapshot" 的 TestPattern
	PreprovisionedSnapshotRetain = TestPattern{
		Name:                   "Pre-provisioned Snapshot (retain policy)",
		SnapshotType:           PreprovisionedCreatedSnapshot,
		SnapshotDeletionPolicy: RetainSnapshot,
		VolType:                DynamicPV,
	}
	// EphemeralSnapshotRetain 用于 generic ephemeral volume 的快照测试，
	// 其中快照会被保留。
	EphemeralSnapshotRetain = TestPattern{
		Name:                   "Ephemeral Snapshot (retain policy)",
		SnapshotType:           DynamicCreatedSnapshot,
		SnapshotDeletionPolicy: RetainSnapshot,
		VolType:                GenericEphemeralVolume,
	}

	// 卷扩容场景的定义

	// DefaultFsDynamicPVAllowExpansion 对应 "Dynamic PV (default fs)(allowExpansion)" 的 TestPattern
	DefaultFsDynamicPVAllowExpansion = TestPattern{
		Name:           "Dynamic PV (default fs)(allowExpansion)",
		VolType:        DynamicPV,
		AllowExpansion: true,
	}

	// NtfsDynamicPVAllowExpansion 对应 "Dynamic PV (default fs)(allowExpansion)" 的 TestPattern
	NtfsDynamicPVAllowExpansion = TestPattern{
		Name:           "Dynamic PV (ntfs)(allowExpansion)",
		VolType:        DynamicPV,
		AllowExpansion: true,
		FsType:         "ntfs",
		TestTags:       []interface{}{feature.Windows},
	}

	// BlockVolModeDynamicPVAllowExpansion 对应 "Dynamic PV (block volmode)(allowExpansion)" 的 TestPattern
	BlockVolModeDynamicPVAllowExpansion = TestPattern{
		Name:           "Dynamic PV (block volmode)(allowExpansion)",
		VolType:        DynamicPV,
		VolMode:        v1.PersistentVolumeBlock,
		AllowExpansion: true,
	}

	// topology 测试的定义

	// TopologyImmediate 对应立即绑定的 TestPattern
	TopologyImmediate = TestPattern{
		Name:        "Dynamic PV (immediate binding)",
		VolType:     DynamicPV,
		BindingMode: storagev1.VolumeBindingImmediate,
	}

	// TopologyDelayed 对应延迟绑定的 TestPattern
	TopologyDelayed = TestPattern{
		Name:        "Dynamic PV (delayed binding)",
		VolType:     DynamicPV,
		BindingMode: storagev1.VolumeBindingWaitForFirstConsumer,
	}
)

// NewVolTypeMap 创建一个启用了给定 TestVolType 的 map
func NewVolTypeMap(types ...TestVolType) map[TestVolType]bool {
	m := map[TestVolType]bool{}
	for _, t := range types {
		m[t] = true
	}
	return m
}
