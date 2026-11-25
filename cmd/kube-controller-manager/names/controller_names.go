/*
Copyright 2023 The Kubernetes Authors.

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

package names

// 控制器规范名称
//
// 命名约定
// 1. 所有控制器采用一致的命名方式。
// 2. 避免缩写，但广为人知的非 Kubernetes 缩写除外。
// 3. Kubernetes 资源名称应连写，不使用连字符。
//
// 变更策略
// 控制器名称视为标识符，仅在绝对必要时修改，例如旧名称不当或控制器职责发生变化。
// 更名时必须在 app.ControllerDescriptor#GetAliases 中将旧名称登记为别名，并保留所有旧别名，以保持向后兼容。
//
// 使用场景
// 以下位置应使用控制器名称常量：
//  1. 定义新的 app.ControllerDescriptor，供 app.NewControllerDescriptors 或 app.KnownControllers 使用；
//  2. 在控制器内部引用自身：
//     2.1. [TODO] 日志引用控制器时使用规范名称，例如启动和关闭日志；
//     2.2. [TODO] 发出事件时将 EventSource.Component 设置为控制器名称，通常在初始化 EventRecorder 时完成；
//     2.3. [TODO] 使用 ControllerStarted 和 ControllerStopped 注册 ControllerManagerMetrics；
//  3. 定义 "--help" 命令或生成文档所用的控制器选项：
//     3.1. 在 options.KubeControllerManagerOptions 注册选项时，用控制器名称创建 pflag.FlagSet，名称会显示在选项组标题中；
//     3.2. 参数帮助文本提到控制器名称时使用同一常量；
//  4. 为新控制器定义新服务账号；旧控制器为保持兼容，命名可能不一致。
const (
	ServiceAccountTokenController                      = "serviceaccount-token-controller"
	EndpointsController                                = "endpoints-controller"
	EndpointSliceController                            = "endpointslice-controller"
	EndpointSliceMirroringController                   = "endpointslice-mirroring-controller"
	ReplicationControllerController                    = "replicationcontroller-controller"
	PodGarbageCollectorController                      = "pod-garbage-collector-controller"
	ResourceQuotaController                            = "resourcequota-controller"
	NamespaceController                                = "namespace-controller"
	ServiceAccountController                           = "serviceaccount-controller"
	GarbageCollectorController                         = "garbage-collector-controller"
	DaemonSetController                                = "daemonset-controller"
	JobController                                      = "job-controller"
	DeploymentController                               = "deployment-controller"
	ReplicaSetController                               = "replicaset-controller"
	HorizontalPodAutoscalerController                  = "horizontal-pod-autoscaler-controller"
	DisruptionController                               = "disruption-controller"
	StatefulSetController                              = "statefulset-controller"
	CronJobController                                  = "cronjob-controller"
	CertificateSigningRequestSigningController         = "certificatesigningrequest-signing-controller"
	CertificateSigningRequestApprovingController       = "certificatesigningrequest-approving-controller"
	CertificateSigningRequestCleanerController         = "certificatesigningrequest-cleaner-controller"
	PodCertificateRequestCleanerController             = "podcertificaterequest-cleaner-controller"
	TTLController                                      = "ttl-controller"
	BootstrapSignerController                          = "bootstrap-signer-controller"
	TokenCleanerController                             = "token-cleaner-controller"
	NodeIpamController                                 = "node-ipam-controller"
	NodeLifecycleController                            = "node-lifecycle-controller"
	TaintEvictionController                            = "taint-eviction-controller"
	DeviceTaintEvictionController                      = "device-taint-eviction-controller"
	PersistentVolumeBinderController                   = "persistentvolume-binder-controller"
	PersistentVolumeAttachDetachController             = "persistentvolume-attach-detach-controller"
	PersistentVolumeExpanderController                 = "persistentvolume-expander-controller"
	ClusterRoleAggregationController                   = "clusterrole-aggregation-controller"
	PersistentVolumeClaimProtectionController          = "persistentvolumeclaim-protection-controller"
	PersistentVolumeProtectionController               = "persistentvolume-protection-controller"
	PodGroupProtectionController                       = "podgroup-protection-controller"
	TTLAfterFinishedController                         = "ttl-after-finished-controller"
	RootCACertificatePublisherController               = "root-ca-certificate-publisher-controller"
	KubeAPIServerClusterTrustBundlePublisherController = "kube-apiserver-serving-clustertrustbundle-publisher-controller"
	EphemeralVolumeController                          = "ephemeral-volume-controller"
	StorageVersionGarbageCollectorController           = "storageversion-garbage-collector-controller"
	ResourceClaimController                            = "resourceclaim-controller"
	ResourcePoolStatusRequestController                = "resourcepoolstatusrequest-controller"
	LegacyServiceAccountTokenCleanerController         = "legacy-serviceaccount-token-cleaner-controller"
	ValidatingAdmissionPolicyStatusController          = "validatingadmissionpolicy-status-controller"
	VolumeAttributesClassProtectionController          = "volumeattributesclass-protection-controller"
	ServiceCIDRController                              = "service-cidr-controller"
	StorageVersionMigratorController                   = "storage-version-migrator-controller"
	SELinuxWarningController                           = "selinux-warning-controller"
)
