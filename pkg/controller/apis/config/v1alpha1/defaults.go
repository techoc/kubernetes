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

package v1alpha1

import (
	kruntime "k8s.io/apimachinery/pkg/runtime"
	serviceconfigv1alpha1 "k8s.io/cloud-provider/controllers/service/config/v1alpha1"
	cmconfigv1alpha1 "k8s.io/controller-manager/config/v1alpha1"
	kubectrlmgrconfigv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
	csrsigningconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/certificates/signer/config/v1alpha1"
	cronjobconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/cronjob/config/v1alpha1"
	daemonconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/daemon/config/v1alpha1"
	deploymentconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/deployment/config/v1alpha1"
	devicetaintevictionconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/devicetainteviction/config/v1alpha1"
	disruptionconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/disruption/config/v1alpha1"
	endpointconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/endpoint/config/v1alpha1"
	endpointsliceconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/endpointslice/config/v1alpha1"
	endpointslicemirroringconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/endpointslicemirroring/config/v1alpha1"
	garbagecollectorconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/garbagecollector/config/v1alpha1"
	jobconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/job/config/v1alpha1"
	namespaceconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/namespace/config/v1alpha1"
	nodeipamconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/nodeipam/config/v1alpha1"
	nodelifecycleconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/nodelifecycle/config/v1alpha1"
	poautosclerconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/podautoscaler/config/v1alpha1"
	podgcconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/podgc/config/v1alpha1"
	replicasetconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/replicaset/config/v1alpha1"
	replicationconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/replication/config/v1alpha1"
	resourceclaimconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/resourceclaim/config/v1alpha1"
	resourcequotaconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/resourcequota/config/v1alpha1"
	serviceaccountconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/serviceaccount/config/v1alpha1"
	statefulsetconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/statefulset/config/v1alpha1"
	ttlafterfinishedconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/ttlafterfinished/config/v1alpha1"
	validatingadmissionpolicystatusv1alpha1 "k8s.io/kubernetes/pkg/controller/validatingadmissionpolicystatus/config/v1alpha1"
	attachdetachconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/volume/attachdetach/config/v1alpha1"
	ephemeralvolumeconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/volume/ephemeral/config/v1alpha1"
	persistentvolumeconfigv1alpha1 "k8s.io/kubernetes/pkg/controller/volume/persistentvolume/config/v1alpha1"
)

// addDefaultingFuncs 将本包的默认值注册函数挂入 scheme，
// 由代码生成的 RegisterDefaults 完成具体注册。
func addDefaultingFuncs(scheme *kruntime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_KubeControllerManagerConfiguration 为 kube-controller-manager 的全部配置项设置默认值。
// 除了通用的推荐默认值，还集中覆盖各控制器的默认值；
// 集中设置可以让显式指定的非零值保持不变，仅填补未配置的字段。
func SetDefaults_KubeControllerManagerConfiguration(obj *kubectrlmgrconfigv1alpha1.KubeControllerManagerConfiguration) {
	// 这里的客户端连接默认值会覆盖 componentbaseconfigv1alpha1 自动应用的推荐值，
	// 因为它们是 kube-controller-manager 特有的，需要考虑同时运行大量控制器的负载。
	if obj.Generic.ClientConnection.QPS == 0.0 {
		obj.Generic.ClientConnection.QPS = 20.0
	}
	if obj.Generic.ClientConnection.Burst == 0 {
		obj.Generic.ClientConnection.Burst = 30
	}

	// 应用 RecommendedDefaultGenericControllerManagerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	cmconfigv1alpha1.RecommendedDefaultGenericControllerManagerConfiguration(&obj.Generic)
	// 应用 RecommendedDefaultHPAControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	attachdetachconfigv1alpha1.RecommendedDefaultAttachDetachControllerConfiguration(&obj.AttachDetachController)
	// 应用 RecommendedDefaultCSRSigningControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	csrsigningconfigv1alpha1.RecommendedDefaultCSRSigningControllerConfiguration(&obj.CSRSigningController)
	// 应用 RecommendedDefaultDaemonSetControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	daemonconfigv1alpha1.RecommendedDefaultDaemonSetControllerConfiguration(&obj.DaemonSetController)
	// 应用 RecommendedDefaultDeploymentControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	deploymentconfigv1alpha1.RecommendedDefaultDeploymentControllerConfiguration(&obj.DeploymentController)
	// 应用 RecommendedDefaultDisruptionControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	disruptionconfigv1alpha1.RecommendedDefaultDisruptionControllerConfiguration(&obj.DisruptionController)
	// 应用 RecommendedDefaultDeviceTaintEvictionControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	devicetaintevictionconfigv1alpha1.RecommendedDefaultDeviceTaintEvictionControllerConfiguration(&obj.DeviceTaintEvictionController)
	// 应用 RecommendedDefaultResourceClaimControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	resourceclaimconfigv1alpha1.RecommendedDefaultResourceClaimControllerConfiguration(&obj.ResourceClaimController)
	// 应用 RecommendedDefaultStatefulSetControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	statefulsetconfigv1alpha1.RecommendedDefaultStatefulSetControllerConfiguration(&obj.StatefulSetController)
	// 应用 RecommendedDefaultEndpointControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	endpointconfigv1alpha1.RecommendedDefaultEndpointControllerConfiguration(&obj.EndpointController)
	// 应用 RecommendedDefaultEndpointSliceControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	endpointsliceconfigv1alpha1.RecommendedDefaultEndpointSliceControllerConfiguration(&obj.EndpointSliceController)
	// 应用 RecommendedDefaultEndpointSliceMirroringControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	endpointslicemirroringconfigv1alpha1.RecommendedDefaultEndpointSliceMirroringControllerConfiguration(&obj.EndpointSliceMirroringController)
	// 应用 RecommendedDefaultEphemeralVolumeControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	ephemeralvolumeconfigv1alpha1.RecommendedDefaultEphemeralVolumeControllerConfiguration(&obj.EphemeralVolumeController)
	// 应用 RecommendedDefaultGarbageCollectorControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	garbagecollectorconfigv1alpha1.RecommendedDefaultGarbageCollectorControllerConfiguration(&obj.GarbageCollectorController)
	// 应用 RecommendedDefaultJobControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	jobconfigv1alpha1.RecommendedDefaultJobControllerConfiguration(&obj.JobController)
	// 应用 RecommendedDefaultCronJobControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	cronjobconfigv1alpha1.RecommendedDefaultCronJobControllerConfiguration(&obj.CronJobController)
	// 应用 RecommendedDefaultNamespaceControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	namespaceconfigv1alpha1.RecommendedDefaultNamespaceControllerConfiguration(&obj.NamespaceController)
	// 应用 RecommendedDefaultNodeIPAMControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	nodeipamconfigv1alpha1.RecommendedDefaultNodeIPAMControllerConfiguration(&obj.NodeIPAMController)
	// 应用 RecommendedDefaultHPAControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	poautosclerconfigv1alpha1.RecommendedDefaultHPAControllerConfiguration(&obj.HPAController)
	// 应用 RecommendedDefaultNodeLifecycleControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	nodelifecycleconfigv1alpha1.RecommendedDefaultNodeLifecycleControllerConfiguration(&obj.NodeLifecycleController)
	// 应用 RecommendedDefaultPodGCControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	podgcconfigv1alpha1.RecommendedDefaultPodGCControllerConfiguration(&obj.PodGCController)
	// 应用 RecommendedDefaultReplicaSetControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	replicasetconfigv1alpha1.RecommendedDefaultReplicaSetControllerConfiguration(&obj.ReplicaSetController)
	// 应用 RecommendedDefaultReplicationControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	replicationconfigv1alpha1.RecommendedDefaultReplicationControllerConfiguration(&obj.ReplicationController)
	// 应用 RecommendedDefaultResourceQuotaControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	resourcequotaconfigv1alpha1.RecommendedDefaultResourceQuotaControllerConfiguration(&obj.ResourceQuotaController)
	// 应用 RecommendedDefaultServiceControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	serviceconfigv1alpha1.RecommendedDefaultServiceControllerConfiguration(&obj.ServiceController)
	// 应用 RecommendedDefaultLegacySATokenCleanerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	serviceaccountconfigv1alpha1.RecommendedDefaultLegacySATokenCleanerConfiguration(&obj.LegacySATokenCleaner)
	// 应用 RecommendedDefaultSAControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	serviceaccountconfigv1alpha1.RecommendedDefaultSAControllerConfiguration(&obj.SAController)
	// 应用 RecommendedDefaultTTLAfterFinishedControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	ttlafterfinishedconfigv1alpha1.RecommendedDefaultTTLAfterFinishedControllerConfiguration(&obj.TTLAfterFinishedController)
	// 应用 RecommendedDefaultPersistentVolumeBinderControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	persistentvolumeconfigv1alpha1.RecommendedDefaultPersistentVolumeBinderControllerConfiguration(&obj.PersistentVolumeBinderController)
	// 应用 RecommendedDefaultValidatingAdmissionPolicyStatusControllerConfiguration 提供的推荐默认值，保证各控制器有合理的开箱即用配置。
	validatingadmissionpolicystatusv1alpha1.RecommendedDefaultValidatingAdmissionPolicyStatusControllerConfiguration(&obj.ValidatingAdmissionPolicyStatusController)
}
