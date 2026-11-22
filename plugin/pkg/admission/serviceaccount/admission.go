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

package serviceaccount

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	podutil "k8s.io/kubernetes/pkg/api/pod"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/serviceaccount"
	"k8s.io/utils/ptr"
)

const (
	// DefaultServiceAccountName 是未指定 ServiceAccount 的 Pod 所使用的默认服务账号名称。
	DefaultServiceAccountName = "default"

	// EnforceMountableSecretsAnnotation 是表示服务账号应强制校验可挂载 Secret 的注解。
	// 只有值为 true 时该注解才生效。
	EnforceMountableSecretsAnnotation = "kubernetes.io/enforce-mountable-secrets"

	// ServiceAccountVolumeName 是挂载 ServiceAccount Secret 的卷名称前缀。
	ServiceAccountVolumeName = "kube-api-access"

	// DefaultAPITokenMountPath 是 ServiceAccountToken Secret 自动挂载的路径，
	// 可通过 /var/run/secrets/kubernetes.io/serviceaccount 访问其中的 token 文件。
	DefaultAPITokenMountPath = "/var/run/secrets/kubernetes.io/serviceaccount"

	// PluginName 是本准入插件的名称。
	PluginName = "ServiceAccount"
)

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		serviceAccountAdmission := NewServiceAccount()
		return serviceAccountAdmission, nil
	})
}

var _ = admission.Interface(&Plugin{})

// Plugin 持有准入控制器使用的客户端及相关配置。
type Plugin struct {
	*admission.Handler

	// LimitSecretReferences 控制是否拒绝引用了服务账号未声明 Secret 的 Pod。
	LimitSecretReferences bool
	// MountServiceAccountToken 控制是否为 Pod 服务账号的 ServiceAccountToken 创建卷和挂载点。
	MountServiceAccountToken bool

	client kubernetes.Interface

	serviceAccountLister corev1listers.ServiceAccountLister

	generateName func(string) string
}

var _ admission.MutationInterface = &Plugin{}
var _ admission.ValidationInterface = &Plugin{}
var _ = genericadmissioninitializer.WantsExternalKubeClientSet(&Plugin{})
var _ = genericadmissioninitializer.WantsExternalKubeInformerFactory(&Plugin{})

// NewServiceAccount 返回一个基于 Pod 的 ServiceAccount 限制 Pod CREATE 请求的 admission.Interface 实现：
// 1. Pod 未指定 ServiceAccount 时，将其设为 "default"。
// 2. 确保 Pod 引用的 ServiceAccount 存在。
// 3. LimitSecretReferences 为 true 时，拒绝引用了 ServiceAccount 未声明 Secret 的 Pod。
// 4. Pod 未声明任何 ImagePullSecrets 时，补上服务账号的 ImagePullSecrets。
// 5. MountServiceAccountToken 为 true 时，为容器添加挂载服务账号 API token Secret 的 VolumeMount。
func NewServiceAccount() *Plugin {
	return &Plugin{
		Handler: admission.NewHandler(admission.Create, admission.Update),
		// TODO: 待梳理完 Secret 使用情况、补上服务账号的 Secret 引用后再启用
		LimitSecretReferences: false,
		// 自动挂载服务账号的 API token Secret。
		MountServiceAccountToken: true,

		generateName: names.SimpleNameGenerator.GenerateName,
	}
}

// SetExternalKubeClientSet 设置插件使用的客户端。
func (s *Plugin) SetExternalKubeClientSet(cl kubernetes.Interface) {
	s.client = cl
}

// SetExternalKubeInformerFactory 向插件注册 informer。
func (s *Plugin) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	serviceAccountInformer := f.Core().V1().ServiceAccounts()
	s.serviceAccountLister = serviceAccountInformer.Lister()
	s.SetReadyFunc(func() bool {
		return serviceAccountInformer.Informer().HasSynced()
	})
}

// ValidateInitialization 确保初始化所需的依赖都已注入。
func (s *Plugin) ValidateInitialization() error {
	if s.client == nil {
		return fmt.Errorf("missing client")
	}
	if s.serviceAccountLister == nil {
		return fmt.Errorf("missing serviceAccountLister")
	}
	return nil
}

// Admit 校验 Pod 是否应被准入，并在创建时补齐服务账号相关字段。
func (s *Plugin) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if shouldIgnore(a) {
		return nil
	}
	if a.GetOperation() != admission.Create {
		// 只在创建请求时修改 Pod，更新请求交给 Validate 处理。
		return nil
	}
	pod := a.GetObject().(*api.Pod)

	// 不能修改镜像 Pod 的 spec：kubelet 会发现本地 spec 与服务端不一致并删除该 Pod。
	// 但镜像 Pod 也不允许引用 ServiceAccount 或 Secret 卷，因此转交 Validate 做校验。
	if _, isMirrorPod := pod.Annotations[api.MirrorPodAnnotationKey]; isMirrorPod {
		return s.Validate(ctx, a, o)
	}

	// 设置默认服务账号时同步已废弃的别名字段（见 SetDefaults_PodSpec），
	// 保证默认值处理幂等、结果稳定。
	if len(pod.Spec.ServiceAccountName) == 0 {
		pod.Spec.ServiceAccountName = DefaultServiceAccountName
		pod.Spec.DeprecatedServiceAccount = DefaultServiceAccountName //nolint:staticcheck // SA1019 DeprecatedServiceAccount must be set for backward compatibility
	}

	serviceAccount, err := s.getServiceAccount(a.GetNamespace(), pod.Spec.ServiceAccountName)
	if err != nil {
		return admission.NewForbidden(a, fmt.Errorf("error looking up service account %s/%s: %w", a.GetNamespace(), pod.Spec.ServiceAccountName, err))
	}
	if s.MountServiceAccountToken && shouldAutomount(serviceAccount, pod) {
		s.mountServiceAccountToken(serviceAccount, pod)
	}
	if len(pod.Spec.ImagePullSecrets) == 0 {
		pod.Spec.ImagePullSecrets = make([]api.LocalObjectReference, len(serviceAccount.ImagePullSecrets))
		for i := 0; i < len(serviceAccount.ImagePullSecrets); i++ {
			pod.Spec.ImagePullSecrets[i].Name = serviceAccount.ImagePullSecrets[i].Name
		}
	}

	return s.Validate(ctx, a, o)
}

// Validate 校验服务账号相关数据的合法性。
func (s *Plugin) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if shouldIgnore(a) {
		return nil
	}

	pod := a.GetObject().(*api.Pod)

	if a.GetOperation() == admission.Update && a.GetSubresource() == "ephemeralcontainers" {
		return s.limitEphemeralContainerSecretReferences(pod, a)
	}

	if a.GetOperation() != admission.Create {
		// 只在创建请求时校验完整的 Pod spec。
		return nil
	}

	// 镜像 Pod 对可引用的资源有额外限制。
	if _, isMirrorPod := pod.Annotations[api.MirrorPodAnnotationKey]; isMirrorPod {
		if len(pod.Spec.ServiceAccountName) != 0 {
			return admission.NewForbidden(a, fmt.Errorf("a mirror pod may not reference service accounts"))
		}
		hasSecrets := false
		podutil.VisitPodSecretNames(pod, func(name string) bool {
			hasSecrets = true
			return false
		}, podutil.AllContainers)
		if hasSecrets {
			return admission.NewForbidden(a, fmt.Errorf("a mirror pod may not reference secrets"))
		}
		for _, v := range pod.Spec.Volumes {
			if proj := v.Projected; proj != nil {
				for _, projSource := range proj.Sources {
					if projSource.ServiceAccountToken != nil {
						return admission.NewForbidden(a, fmt.Errorf("a mirror pod may not use ServiceAccountToken volume projections"))
					}
				}
			}
		}
		return nil
	}

	// 常规容器 Pod 必须指定服务账号。
	if len(pod.Spec.ServiceAccountName) == 0 {
		return admission.NewForbidden(a, fmt.Errorf("no service account specified for pod %s/%s", a.GetNamespace(), pod.Name))
	}
	// 确保引用的服务账号确实存在。
	serviceAccount, err := s.getServiceAccount(a.GetNamespace(), pod.Spec.ServiceAccountName)
	if err != nil {
		return admission.NewForbidden(a, fmt.Errorf("error looking up service account %s/%s: %v", a.GetNamespace(), pod.Spec.ServiceAccountName, err))
	}

	if s.enforceMountableSecrets(serviceAccount) {
		if err := s.limitSecretReferences(serviceAccount, pod); err != nil {
			return admission.NewForbidden(a, err)
		}
	}

	return nil
}

// shouldIgnore 判断请求是否无需本插件处理：仅处理 Pod 本体及 ephemeralcontainers 子资源。
func shouldIgnore(a admission.Attributes) bool {
	if a.GetResource().GroupResource() != api.Resource("pods") || (a.GetSubresource() != "" && a.GetSubresource() != "ephemeralcontainers") {
		return true
	}
	obj := a.GetObject()
	if obj == nil {
		return true
	}
	_, ok := obj.(*api.Pod)
	if !ok {
		return true
	}

	return false
}

// shouldAutomount 决定是否自动挂载服务账号 token：Pod 的设置优先，其次是服务账号，默认开启。
func shouldAutomount(sa *corev1.ServiceAccount, pod *api.Pod) bool {
	// Pod 的显式设置优先级最高。
	if pod.Spec.AutomountServiceAccountToken != nil {
		return *pod.Spec.AutomountServiceAccountToken
	}
	// 其次看服务账号的设置。
	if sa.AutomountServiceAccountToken != nil {
		return *sa.AutomountServiceAccountToken
	}
	// 为向后兼容，默认自动挂载。
	return true
}

// enforceMountableSecrets 判断某个服务账号是否启用可挂载 Secret 校验；
// 全局开启时会覆盖单个服务账号上的设置。
func (s *Plugin) enforceMountableSecrets(serviceAccount *corev1.ServiceAccount) bool {
	if s.LimitSecretReferences {
		return true
	}

	if value, ok := serviceAccount.Annotations[EnforceMountableSecretsAnnotation]; ok {
		enforceMountableSecretCheck, _ := strconv.ParseBool(value)
		return enforceMountableSecretCheck
	}

	return false
}

// getServiceAccount 返回指定命名空间和名称的 ServiceAccount，缓存未命中时会直接查询 API。
func (s *Plugin) getServiceAccount(namespace string, name string) (*corev1.ServiceAccount, error) {
	serviceAccount, err := s.serviceAccountLister.ServiceAccounts(namespace).Get(name)
	if err == nil {
		return serviceAccount, nil
	}
	if !errors.IsNotFound(err) {
		return nil, err
	}

	// 缓存里没有，直接查 API 兜底。
	numAttempts := 1
	if name == DefaultServiceAccountName {
		// 默认服务账号由控制器自动创建，可能存在短暂延迟，多试几次。
		numAttempts = 10
	}
	retryInterval := time.Duration(rand.Int63n(100)+int64(100)) * time.Millisecond
	for i := 0; i < numAttempts; i++ {
		if i != 0 {
			time.Sleep(retryInterval)
		}
		serviceAccount, err := s.client.CoreV1().ServiceAccounts(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err == nil {
			return serviceAccount, nil
		}
		if !errors.IsNotFound(err) {
			return nil, err
		}
	}

	return nil, errors.NewNotFound(api.Resource("serviceaccount"), name)
}

// limitSecretReferences 检查 Pod 引用的所有 Secret 是否被服务账号允许。
func (s *Plugin) limitSecretReferences(serviceAccount *corev1.ServiceAccount, pod *api.Pod) error {
	// 收集服务账号声明的可挂载 Secret 名称。
	mountableSecrets := sets.NewString()
	for _, s := range serviceAccount.Secrets {
		mountableSecrets.Insert(s.Name)
	}
	for _, volume := range pod.Spec.Volumes {
		source := volume.VolumeSource
		if source.Secret == nil {
			continue
		}
		secretName := source.Secret.SecretName
		if !mountableSecrets.Has(secretName) {
			return fmt.Errorf("volume with secret.secretName=\"%s\" is not allowed because service account %s does not reference that secret", secretName, serviceAccount.Name)
		}
	}

	for _, container := range pod.Spec.InitContainers {
		for _, env := range container.Env {
			if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
				if !mountableSecrets.Has(env.ValueFrom.SecretKeyRef.Name) {
					return fmt.Errorf("init container %s with envVar %s referencing secret.secretName=\"%s\" is not allowed because service account %s does not reference that secret", container.Name, env.Name, env.ValueFrom.SecretKeyRef.Name, serviceAccount.Name)
				}
			}
		}
		for _, envFrom := range container.EnvFrom {
			if envFrom.SecretRef != nil {
				if !mountableSecrets.Has(envFrom.SecretRef.Name) {
					return fmt.Errorf("init container %s with envFrom referencing secret.secretName=\"%s\" is not allowed because service account %s does not reference that secret", container.Name, envFrom.SecretRef.Name, serviceAccount.Name)
				}
			}
		}
	}

	for _, container := range pod.Spec.Containers {
		for _, env := range container.Env {
			if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
				if !mountableSecrets.Has(env.ValueFrom.SecretKeyRef.Name) {
					return fmt.Errorf("container %s with envVar %s referencing secret.secretName=\"%s\" is not allowed because service account %s does not reference that secret", container.Name, env.Name, env.ValueFrom.SecretKeyRef.Name, serviceAccount.Name)
				}
			}
		}
		for _, envFrom := range container.EnvFrom {
			if envFrom.SecretRef != nil {
				if !mountableSecrets.Has(envFrom.SecretRef.Name) {
					return fmt.Errorf("container %s with envFrom referencing secret.secretName=\"%s\" is not allowed because service account %s does not reference that secret", container.Name, envFrom.SecretRef.Name, serviceAccount.Name)
				}
			}
		}
	}

	// 拉取镜像用的 Secret 也在校验范围内。
	pullSecrets := sets.NewString()
	for _, s := range serviceAccount.ImagePullSecrets {
		pullSecrets.Insert(s.Name)
	}
	for i, pullSecretRef := range pod.Spec.ImagePullSecrets {
		if !pullSecrets.Has(pullSecretRef.Name) {
			return fmt.Errorf(`imagePullSecrets[%d].name="%s" is not allowed because service account %s does not reference that imagePullSecret`, i, pullSecretRef.Name, serviceAccount.Name)
		}
	}
	return nil
}

// limitEphemeralContainerSecretReferences 校验临时容器引用的 Secret 是否被服务账号允许。
func (s *Plugin) limitEphemeralContainerSecretReferences(pod *api.Pod, a admission.Attributes) error {
	// 含临时容器的 Pod 同样必须指定服务账号。
	if len(pod.Spec.ServiceAccountName) == 0 {
		return admission.NewForbidden(a, fmt.Errorf("no service account specified for pod %s/%s", a.GetNamespace(), pod.Name))
	}
	// 确保引用的服务账号存在。
	serviceAccount, err := s.getServiceAccount(a.GetNamespace(), pod.Spec.ServiceAccountName)
	if err != nil {
		return admission.NewForbidden(a, fmt.Errorf("error looking up service account %s/%s: %w", a.GetNamespace(), pod.Spec.ServiceAccountName, err))
	}
	if !s.enforceMountableSecrets(serviceAccount) {
		return nil
	}
	// 收集服务账号声明的可挂载 Secret，供临时容器逐一校验。
	mountableSecrets := sets.NewString()
	for _, s := range serviceAccount.Secrets {
		mountableSecrets.Insert(s.Name)
	}
	for _, container := range pod.Spec.EphemeralContainers {
		for _, env := range container.Env {
			if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
				if !mountableSecrets.Has(env.ValueFrom.SecretKeyRef.Name) {
					return fmt.Errorf("ephemeral container %s with envVar %s referencing secret.secretName=\"%s\" is not allowed because service account %s does not reference that secret", container.Name, env.Name, env.ValueFrom.SecretKeyRef.Name, serviceAccount.Name)
				}
			}
		}
		for _, envFrom := range container.EnvFrom {
			if envFrom.SecretRef != nil {
				if !mountableSecrets.Has(envFrom.SecretRef.Name) {
					return fmt.Errorf("ephemeral container %s with envFrom referencing secret.secretName=\"%s\" is not allowed because service account %s does not reference that secret", container.Name, envFrom.SecretRef.Name, serviceAccount.Name)
				}
			}
		}
	}
	return nil
}

// mountServiceAccountToken 为 Pod 的所有容器挂载服务账号 token 卷，必要时创建该卷。
func (s *Plugin) mountServiceAccountToken(serviceAccount *corev1.ServiceAccount, pod *api.Pod) {
	// 若已有 token 卷则复用其名称，避免重复添加。
	tokenVolumeName := ""
	hasTokenVolume := false
	allVolumeNames := sets.NewString()
	for _, volume := range pod.Spec.Volumes {
		allVolumeNames.Insert(volume.Name)
		if strings.HasPrefix(volume.Name, ServiceAccountVolumeName+"-") {
			tokenVolumeName = volume.Name
			hasTokenVolume = true
			break
		}
	}

	// 没有现成的 token 卷时，生成一个名称备用。
	if len(tokenVolumeName) == 0 {
		tokenVolumeName = s.generateName(ServiceAccountVolumeName + "-")
	}

	// 构造标准的 VolumeMount 模板。
	volumeMount := api.VolumeMount{
		Name:      tokenVolumeName,
		ReadOnly:  true,
		MountPath: DefaultAPITokenMountPath,
	}

	// 所有容器都挂载 API token 卷；若确实添加了挂载，则需要保证卷存在。
	needsTokenVolume := false
	for i, container := range pod.Spec.InitContainers {
		existingContainerMount := false
		for _, volumeMount := range container.VolumeMounts {
			// 容器已占用默认挂载路径时不能再挂载 token，跳过以免冲突。
			if volumeMount.MountPath == DefaultAPITokenMountPath {
				existingContainerMount = true
				break
			}
		}
		if !existingContainerMount {
			pod.Spec.InitContainers[i].VolumeMounts = append(pod.Spec.InitContainers[i].VolumeMounts, volumeMount)
			needsTokenVolume = true
		}
	}
	for i, container := range pod.Spec.Containers {
		existingContainerMount := false
		for _, volumeMount := range container.VolumeMounts {
			// 容器已占用默认挂载路径时不能再挂载 token，跳过以免冲突。
			if volumeMount.MountPath == DefaultAPITokenMountPath {
				existingContainerMount = true
				break
			}
		}
		if !existingContainerMount {
			pod.Spec.Containers[i].VolumeMounts = append(pod.Spec.Containers[i].VolumeMounts, volumeMount)
			needsTokenVolume = true
		}
	}

	// 没有现成卷且确有容器需要挂载时才创建，避免无谓地改动 Pod。
	if !hasTokenVolume && needsTokenVolume {
		pod.Spec.Volumes = append(pod.Spec.Volumes, api.Volume{
			Name: tokenVolumeName,
			VolumeSource: api.VolumeSource{
				Projected: TokenVolumeSource(),
			},
		})
	}
}

// TokenVolumeSource 返回服务账号 token 的 projected 卷源。
func TokenVolumeSource() *api.ProjectedVolumeSource {
	return &api.ProjectedVolumeSource{
		// 显式设置默认值，见 #104464
		DefaultMode: ptr.To[int32](corev1.ProjectedVolumeSourceDefaultMode),
		Sources: []api.VolumeProjection{
			{
				ServiceAccountToken: &api.ServiceAccountTokenProjection{
					Path:              "token",
					ExpirationSeconds: ptr.To[int64](serviceaccount.WarnOnlyBoundTokenExpirationSeconds),
				},
			},
			{
				ConfigMap: &api.ConfigMapProjection{
					LocalObjectReference: api.LocalObjectReference{
						Name: "kube-root-ca.crt",
					},
					Items: []api.KeyToPath{
						{
							Key:  "ca.crt",
							Path: "ca.crt",
						},
					},
				},
			},
			{
				DownwardAPI: &api.DownwardAPIProjection{
					Items: []api.DownwardAPIVolumeFile{
						{
							Path: "namespace",
							FieldRef: &api.ObjectFieldSelector{
								APIVersion: "v1",
								FieldPath:  "metadata.namespace",
							},
						},
					},
				},
			},
		},
	}
}
