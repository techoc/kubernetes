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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cpconfig "k8s.io/cloud-provider/config"
	serviceconfig "k8s.io/cloud-provider/controllers/service/config"
	cmconfig "k8s.io/controller-manager/config"
	csrsigningconfig "k8s.io/kubernetes/pkg/controller/certificates/signer/config"
	cronjobconfig "k8s.io/kubernetes/pkg/controller/cronjob/config"
	daemonconfig "k8s.io/kubernetes/pkg/controller/daemon/config"
	deploymentconfig "k8s.io/kubernetes/pkg/controller/deployment/config"
	devicetaintevictionconfig "k8s.io/kubernetes/pkg/controller/devicetainteviction/config"
	disruptionconfig "k8s.io/kubernetes/pkg/controller/disruption/config"
	endpointconfig "k8s.io/kubernetes/pkg/controller/endpoint/config"
	endpointsliceconfig "k8s.io/kubernetes/pkg/controller/endpointslice/config"
	endpointslicemirroringconfig "k8s.io/kubernetes/pkg/controller/endpointslicemirroring/config"
	garbagecollectorconfig "k8s.io/kubernetes/pkg/controller/garbagecollector/config"
	jobconfig "k8s.io/kubernetes/pkg/controller/job/config"
	namespaceconfig "k8s.io/kubernetes/pkg/controller/namespace/config"
	nodeipamconfig "k8s.io/kubernetes/pkg/controller/nodeipam/config"
	nodelifecycleconfig "k8s.io/kubernetes/pkg/controller/nodelifecycle/config"
	poautosclerconfig "k8s.io/kubernetes/pkg/controller/podautoscaler/config"
	podgcconfig "k8s.io/kubernetes/pkg/controller/podgc/config"
	replicasetconfig "k8s.io/kubernetes/pkg/controller/replicaset/config"
	replicationconfig "k8s.io/kubernetes/pkg/controller/replication/config"
	resourceclaimconfig "k8s.io/kubernetes/pkg/controller/resourceclaim/config"
	resourcequotaconfig "k8s.io/kubernetes/pkg/controller/resourcequota/config"
	serviceaccountconfig "k8s.io/kubernetes/pkg/controller/serviceaccount/config"
	statefulsetconfig "k8s.io/kubernetes/pkg/controller/statefulset/config"
	ttlafterfinishedconfig "k8s.io/kubernetes/pkg/controller/ttlafterfinished/config"
	validatingadmissionpolicystatusconfig "k8s.io/kubernetes/pkg/controller/validatingadmissionpolicystatus/config"
	attachdetachconfig "k8s.io/kubernetes/pkg/controller/volume/attachdetach/config"
	ephemeralvolumeconfig "k8s.io/kubernetes/pkg/controller/volume/ephemeral/config"
	persistentvolumeconfig "k8s.io/kubernetes/pkg/controller/volume/persistentvolume/config"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KubeControllerManagerConfiguration 包含 kube-controller-manager 的全部配置。
type KubeControllerManagerConfiguration struct {
	metav1.TypeMeta

	// Generic 保存通用 controller-manager 的配置
	Generic cmconfig.GenericControllerManagerConfiguration
	// KubeCloudSharedConfiguration 保存 cloud-controller-manager 与 kube-controller-manager 共用的配置
	KubeCloudShared cpconfig.KubeCloudSharedConfiguration

	// AttachDetachControllerConfiguration 保存 AttachDetachController 相关功能的配置。
	// AttachDetachController related features.
	AttachDetachController attachdetachconfig.AttachDetachControllerConfiguration
	// CronJobControllerConfiguration 保存 CronJobController 相关功能的配置。
	// related features.
	CronJobController cronjobconfig.CronJobControllerConfiguration
	// CSRSigningControllerConfiguration 保存 CSRSigningController 相关功能的配置。
	// CSRSigningController related features.
	CSRSigningController csrsigningconfig.CSRSigningControllerConfiguration
	// DaemonSetControllerConfiguration 保存 DaemonSetController 相关功能的配置。
	// related features.
	DaemonSetController daemonconfig.DaemonSetControllerConfiguration
	// DeploymentControllerConfiguration 保存 DeploymentController 相关功能的配置。
	// DeploymentController related features.
	DeploymentController deploymentconfig.DeploymentControllerConfiguration
	// DisruptionControllerConfiguration 保存 DisruptionController 相关功能的配置。
	// DisruptionController related features.
	DisruptionController disruptionconfig.DisruptionControllerConfiguration
	// DeviceTaintEvictionControllerConfiguration 保存 DeviceTaintEvictionController 相关功能的配置。
	DeviceTaintEvictionController devicetaintevictionconfig.DeviceTaintEvictionControllerConfiguration
	// DeprecatedControllerConfiguration 包含已废弃的配置项（当前为空）。
	// features.
	DeprecatedController DeprecatedControllerConfiguration
	// EndpointControllerConfiguration 保存 EndpointController 相关功能的配置。
	// related features.
	EndpointController endpointconfig.EndpointControllerConfiguration
	// EndpointSliceControllerConfiguration 保存 EndpointSliceController 相关功能的配置。
	// EndpointSliceController related features.
	EndpointSliceController endpointsliceconfig.EndpointSliceControllerConfiguration
	// EndpointSliceMirroringControllerConfiguration 保存 EndpointSliceMirroringController 相关功能的配置。
	// EndpointSliceMirroringController related features.
	EndpointSliceMirroringController endpointslicemirroringconfig.EndpointSliceMirroringControllerConfiguration
	// EphemeralVolumeControllerConfiguration 保存 EphemeralVolumeController 相关功能的配置。
	// related features.
	EphemeralVolumeController ephemeralvolumeconfig.EphemeralVolumeControllerConfiguration
	// GarbageCollectorControllerConfiguration 保存 GarbageCollectorController 相关功能的配置。
	// GarbageCollectorController related features.
	GarbageCollectorController garbagecollectorconfig.GarbageCollectorControllerConfiguration
	// HPAControllerConfiguration 保存 HPAController 相关功能的配置。
	HPAController poautosclerconfig.HPAControllerConfiguration
	// JobControllerConfiguration 保存 JobController 相关功能的配置。
	JobController jobconfig.JobControllerConfiguration
	// LegacySATokenCleanerConfiguration 保存 LegacySATokenCleaner 相关功能的配置。
	LegacySATokenCleaner serviceaccountconfig.LegacySATokenCleanerConfiguration
	// NamespaceControllerConfiguration 保存 NamespaceController 相关功能的配置。
	// related features.
	NamespaceController namespaceconfig.NamespaceControllerConfiguration
	// NodeIPAMControllerConfiguration 保存 NodeIPAMController 相关功能的配置。
	// related features.
	NodeIPAMController nodeipamconfig.NodeIPAMControllerConfiguration
	// NodeLifecycleControllerConfiguration 保存 NodeLifecycleController 相关功能的配置。
	// NodeLifecycleController related features.
	NodeLifecycleController nodelifecycleconfig.NodeLifecycleControllerConfiguration
	// PersistentVolumeBinderControllerConfiguration 保存 PersistentVolumeBinderController 相关功能的配置。
	// PersistentVolumeBinderController related features.
	PersistentVolumeBinderController persistentvolumeconfig.PersistentVolumeBinderControllerConfiguration
	// PodGCControllerConfiguration 保存 PodGCController 相关功能的配置。
	// related features.
	PodGCController podgcconfig.PodGCControllerConfiguration
	// ReplicaSetControllerConfiguration 保存 ReplicaSetController 相关功能的配置。
	ReplicaSetController replicasetconfig.ReplicaSetControllerConfiguration
	// ReplicationControllerConfiguration 保存 ReplicationController 相关功能的配置。
	// ReplicationController related features.
	ReplicationController replicationconfig.ReplicationControllerConfiguration
	// ResourceQuotaControllerConfiguration 保存 ResourceQuotaController 相关功能的配置。
	// ResourceQuotaController related features.
	ResourceQuotaController resourcequotaconfig.ResourceQuotaControllerConfiguration
	// SAControllerConfiguration 保存 SAController 相关功能的配置。
	// related features.
	SAController serviceaccountconfig.SAControllerConfiguration
	// ServiceControllerConfiguration 保存 ServiceController 相关功能的配置。
	// related features.
	ServiceController serviceconfig.ServiceControllerConfiguration
	// StatefulSetControllerConfiguration 保存 StatefulSetController 相关功能的配置。
	// StatefulSetController related features.
	StatefulSetController statefulsetconfig.StatefulSetControllerConfiguration
	// TTLAfterFinishedControllerConfiguration 保存 TTLAfterFinishedController 相关功能的配置。
	// TTLAfterFinishedController related features.
	TTLAfterFinishedController ttlafterfinishedconfig.TTLAfterFinishedControllerConfiguration
	// ValidatingAdmissionPolicyStatusControllerConfiguration 保存 ValidatingAdmissionPolicyStatusController 相关功能的配置。
	// ValidatingAdmissionPolicyStatusController related features.
	ValidatingAdmissionPolicyStatusController validatingadmissionpolicystatusconfig.ValidatingAdmissionPolicyStatusControllerConfiguration
	// ResourceClaimControllerConfiguration 保存 ResourceClaimController 相关功能的配置。
	ResourceClaimController resourceclaimconfig.ResourceClaimControllerConfiguration
}

// DeprecatedControllerConfiguration 包含已废弃的配置项（当前为空）。
type DeprecatedControllerConfiguration struct {
}
