/*
Copyright 2016 The Kubernetes Authors.

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

package images

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	crierrors "k8s.io/cri-api/pkg/errors"
	"k8s.io/kubernetes/pkg/credentialprovider"
	credentialproviderplugin "k8s.io/kubernetes/pkg/credentialprovider/plugin"
	credentialprovidersecrets "k8s.io/kubernetes/pkg/credentialprovider/secrets"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/images/pullmanager"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/util/parsers"
	"k8s.io/utils/ptr"
)

// init 在包加载阶段注册镜像管理器使用的指标。
func init() {
	registerMetrics()
}

// ImagePodPullingTimeRecorder 负责记录 pod 镜像拉取的开始与结束时间，
// 供镜像拉取耗时统计使用。
type ImagePodPullingTimeRecorder interface {
	RecordImageStartedPulling(podUID types.UID)
	RecordImageFinishedPulling(podUID types.UID)
}

// imageManager 提供镜像拉取相关功能，覆盖镜像预检查、凭证查找、实际拉取与退避重试。
type imageManager struct {
	recorder         record.EventRecorder
	imageService     kubecontainer.ImageService
	imagePullManager pullmanager.ImagePullManager
	backOff          *flowcontrol.Backoff
	prevPullErrMsg   sync.Map

	// puller 负责检查镜像是否存在，并相应上报 "image pulling"、"image pulled" 事件。
	puller      imagePuller
	nodeKeyring credentialprovider.DockerKeyring

	podPullingTimeRecorder ImagePodPullingTimeRecorder
}

// 编译期断言：imageManager 实现了 ImageManager 接口。
var _ ImageManager = &imageManager{}

// NewImageManager 创建一个新的 ImageManager 实例。
// 构造时先对镜像服务做限流，再根据是否串行化选择串行或并行拉取器。
func NewImageManager(
	recorder record.EventRecorder,
	nodeKeyring credentialprovider.DockerKeyring,
	imageService kubecontainer.ImageService,
	imagePullManager pullmanager.ImagePullManager,
	imageBackOff *flowcontrol.Backoff,
	serialized bool,
	maxParallelImagePulls *int32,
	qps float32,
	burst int,
	podPullingTimeRecorder ImagePodPullingTimeRecorder,
) ImageManager {

	imageService = throttleImagePulling(imageService, qps, burst)

	var puller imagePuller
	if serialized {
		puller = newSerialImagePuller(imageService)
	} else {
		puller = newParallelImagePuller(imageService, maxParallelImagePulls)
	}
	return &imageManager{
		recorder:               recorder,
		imageService:           imageService,
		imagePullManager:       imagePullManager,
		nodeKeyring:            nodeKeyring,
		backOff:                imageBackOff,
		puller:                 puller,
		podPullingTimeRecorder: podPullingTimeRecorder,
	}
}

// imagePullPrecheck 根据拉取策略检查镜像是否存在，
// 返回（镜像引用、镜像是否已存在、错误消息、错误），并记录相关错误日志。
func (m *imageManager) imagePullPrecheck(
	ctx context.Context,
	objRef *v1.ObjectReference,
	logPrefix string,
	pullPolicy v1.PullPolicy,
	spec *kubecontainer.ImageSpec,
	requestedImage string,
) (imageRef string, imageFound *bool, msg string, err error) {
	switch pullPolicy {
	case v1.PullAlways:
		return "", nil, msg, nil
	case v1.PullIfNotPresent, v1.PullNever:
		imageRef, err = m.imageService.GetImageRef(ctx, *spec)
		if err != nil {
			msg = fmt.Sprintf("Failed to inspect image %q: %v", imageRef, err)
			m.logIt(klog.FromContext(ctx), objRef, v1.EventTypeWarning, events.FailedToInspectImage, logPrefix, msg)
			return "", nil, msg, ErrImageInspect
		}
	}

	imageFound = ptr.To(len(imageRef) > 0)
	if !*imageFound && pullPolicy == v1.PullNever {
		msg, err = m.imageNotPresentOnNeverPolicyError(klog.FromContext(ctx), logPrefix, objRef, requestedImage)
		return "", ptr.To(false), msg, err
	}

	return imageRef, imageFound, msg, nil
}

// logIt 通过对象引用记录事件；当 ref 为 nil 时，改用传入的 logger 记录日志。
func (m *imageManager) logIt(logger klog.Logger, objRef *v1.ObjectReference, eventtype, event, prefix, msg string) {
	if objRef != nil {
		m.recorder.Event(objRef, eventtype, event, msg)
		return
	}

	logMsg := fmt.Sprint(prefix, " ", msg)
	if eventtype == v1.EventTypeWarning {
		logger.Error(nil, logMsg)
		return
	}
	logger.Info(logMsg)
}

// imageNotPresentOnNeverPolicyError 在镜像不存在时上报事件，并返回需要向上传递的错误。
//
// 适用于两种场景：
//  1. 镜像不存在，且拉取策略为 Never；
//  2. 镜像存在，但当前凭证无法访问它。
//
// 我们不希望泄露"镜像存在但无法访问"这一事实，因此两种场景下保持完全一致的行为。
func (m *imageManager) imageNotPresentOnNeverPolicyError(logger klog.Logger, logPrefix string, objRef *v1.ObjectReference, requestedImage string) (string, error) {
	msg := fmt.Sprintf("Container image %q is not present with pull policy of Never", requestedImage)
	m.logIt(logger, objRef, v1.EventTypeWarning, events.ErrImageNeverPullPolicy, logPrefix, msg)
	return msg, ErrImageNeverPull
}

// EnsureImageExists 为指定 pod 和 requestedImage 拉取镜像，返回（镜像引用、错误消息、错误）。
func (m *imageManager) EnsureImageExists(ctx context.Context, objRef *v1.ObjectReference, pod *v1.Pod, requestedImage string, pullSecrets []v1.Secret, podSandboxConfig *runtimeapi.PodSandboxConfig, podRuntimeHandler string, pullPolicy v1.PullPolicy) (imageRef, message string, err error) {
	logger := klog.FromContext(ctx)
	logPrefix := fmt.Sprintf("%s/%s/%s", pod.Namespace, pod.Name, requestedImage)

	// 用于记录指标的变量。
	var (
		imagePresentLocally *bool
		imagePullRequired   *bool
	)
	defer func() {
		recordEnsureImageRequest(pullPolicy, imagePresentLocally, imagePullRequired)
	}()

	// 镜像未携带 tag 或 digest 时，需要补上默认 tag。
	image, err := applyDefaultImageTag(requestedImage)
	if err != nil {
		msg := fmt.Sprintf("Failed to apply default image tag %q: %v", requestedImage, err)
		m.logIt(logger, objRef, v1.EventTypeWarning, events.FailedToInspectImage, logPrefix, msg)
		return "", msg, ErrInvalidImageName
	}

	var podAnnotations []kubecontainer.Annotation
	for k, v := range pod.GetAnnotations() {
		podAnnotations = append(podAnnotations, kubecontainer.Annotation{
			Name:  k,
			Value: v,
		})
	}

	spec := kubecontainer.ImageSpec{
		Image:          image,
		Annotations:    podAnnotations,
		RuntimeHandler: podRuntimeHandler,
	}

	imageRef, imagePresentLocally, message, err = m.imagePullPrecheck(ctx, objRef, logPrefix, pullPolicy, &spec, requestedImage)
	if err != nil {
		return "", message, err
	}

	// 把凭证查找包装成函数，确保凭证只查找一次，且只在真正需要时才执行。
	lookupPullCredentials := m.makeLookupPullCredentialsFunc(spec.Image, pod, pullSecrets, podSandboxConfig)

	getPodCredentials := func() ([]kubeletconfiginternal.ImagePullSecret, *kubeletconfiginternal.ImagePullServiceAccount, error) {
		pullCredentials, err := lookupPullCredentials()
		if err != nil {
			return nil, nil, err
		}

		var imagePullSecrets []kubeletconfiginternal.ImagePullSecret
		// 这里不考虑 ServiceAccount 的 audience，因此在做出决策时，
		// 一个 pod 最多只会对应一个 imagePullServiceAccount。
		var imagePullServiceAccount *kubeletconfiginternal.ImagePullServiceAccount
		for _, s := range pullCredentials {
			if s.Source == nil {
				// 只关心节点本身无法访问的凭证，节点可访问的凭证无需重复处理。
				continue
			}
			switch {
			case s.Source.Secret != nil:
				imagePullSecrets = append(imagePullSecrets, kubeletconfiginternal.ImagePullSecret{
					UID:            s.Source.Secret.UID,
					Name:           s.Source.Secret.Name,
					Namespace:      s.Source.Secret.Namespace,
					CredentialHash: s.AuthConfigHash,
				})
			case s.Source.ServiceAccount != nil && imagePullServiceAccount == nil:
				imagePullServiceAccount = &kubeletconfiginternal.ImagePullServiceAccount{
					UID:       s.Source.ServiceAccount.UID,
					Name:      s.Source.ServiceAccount.Name,
					Namespace: s.Source.ServiceAccount.Namespace,
				}
			}
		}

		return imagePullSecrets, imagePullServiceAccount, nil
	}

	if imageRef != "" {
		imagePullRequired = ptr.To(false) // 若该分支之后实际发生了拉取，会立即被覆盖

		if !utilfeature.DefaultFeatureGate.Enabled(features.KubeletEnsureSecretPulledImages) {
			msg := fmt.Sprintf("Container image %q already present on machine", requestedImage)
			m.logIt(logger, objRef, v1.EventTypeNormal, events.PulledImage, logPrefix, msg)

			// 镜像可能已由其他 pod 拉取完成，若耗时记录仍在进行，需要提前结束，避免重复计时。
			m.podPullingTimeRecorder.RecordImageFinishedPulling(pod.UID)

			return imageRef, msg, nil
		}
		if pullRequired, err := m.imagePullManager.MustAttemptImagePull(ctx, requestedImage, imageRef, getPodCredentials); err != nil {
			imagePullRequired = nil
			return "", err.Error(), err
		} else if !pullRequired {
			msg := fmt.Sprintf("Container image %q already present on machine and can be accessed by the pod", requestedImage)
			m.logIt(logger, objRef, v1.EventTypeNormal, events.PulledImage, logPrefix, msg)

			// 镜像可能已由其他 pod 拉取完成，若耗时记录仍在进行，需要提前结束，避免重复计时。
			m.podPullingTimeRecorder.RecordImageFinishedPulling(pod.UID)

			return imageRef, msg, nil
		}
	}
	imagePullRequired = ptr.To(true)

	if pullPolicy == v1.PullNever {
		// imagePullPrecheck 已确认镜像存在，但经过 imagePullManager 的凭证检查后，
		// 仍然判定当前凭证无法访问它。
		msg, err := m.imageNotPresentOnNeverPolicyError(logger, logPrefix, objRef, requestedImage)
		return "", msg, err
	}

	pullCredentials, err := lookupPullCredentials()
	if err != nil {
		return "", err.Error(), err
	}

	return m.pullImage(ctx, logPrefix, objRef, pod.UID, requestedImage, spec, pullCredentials, podSandboxConfig)
}

// makeLookupPullCredentialsFunc 构造一个"仅执行一次"的凭证查找函数：
// 返回镜像仓库对应的可用凭证，且整个生命周期内只真正查找一次。
func (m *imageManager) makeLookupPullCredentialsFunc(image string, pod *v1.Pod, pullSecrets []v1.Secret, podSandboxConfig *runtimeapi.PodSandboxConfig) func() ([]credentialprovider.TrackedAuthConfig, error) {
	return sync.OnceValues(func() ([]credentialprovider.TrackedAuthConfig, error) {
		repoToPull, _, _, err := parsers.ParseImageName(image)
		if err != nil {
			return nil, err
		}

		// 组合节点内置凭证提供者与外部凭证插件提供者，构造动态 keyring。
		var podName, podNamespace, podUID string
		if utilfeature.DefaultFeatureGate.Enabled(features.KubeletServiceAccountTokenForCredentialProviders) {
			sandboxMetadata := podSandboxConfig.GetMetadata()

			podName = sandboxMetadata.Name
			podNamespace = sandboxMetadata.Namespace
			podUID = sandboxMetadata.Uid
		}

		externalCredentialProviderKeyring := credentialproviderplugin.NewExternalCredentialProviderDockerKeyring(
			podNamespace,
			podName,
			podUID,
			pod.Spec.ServiceAccountName)

		keyring, err := credentialprovidersecrets.MakeDockerKeyring(pullSecrets, credentialprovider.UnionDockerKeyring{m.nodeKeyring, externalCredentialProviderKeyring})
		if err != nil {
			return nil, err
		}

		pullCredentials, _ := keyring.Lookup(repoToPull)
		return pullCredentials, nil
	})
}

// pullImage 执行一次镜像拉取：先检查退避状态，再启动拉取并记录事件与耗时；
// 失败时登记退避，成功时上报拉取结果。
func (m *imageManager) pullImage(ctx context.Context, logPrefix string, objRef *v1.ObjectReference, podUID types.UID, image string, imgSpec kubecontainer.ImageSpec, pullCredentials []credentialprovider.TrackedAuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (imageRef, message string, err error) {
	logger := klog.FromContext(ctx)
	var pullSucceeded bool
	var finalPullCredentials *credentialprovider.TrackedAuthConfig

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletEnsureSecretPulledImages) {
		if err := m.imagePullManager.RecordPullIntent(logger, image); err != nil {
			return "", fmt.Sprintf("Failed to record image pull intent for container image %q: %v", image, err), err
		}

		defer func() {
			if pullSucceeded {
				m.imagePullManager.RecordImagePulled(ctx, image, imageRef, trackedToImagePullCreds(finalPullCredentials))
			} else {
				m.imagePullManager.RecordImagePullFailed(ctx, image)
			}
		}()
	}

	// 退避期内不再重复发起拉取，直接返回退避错误，避免对持续失败的镜像反复冲击。
	backOffKey := fmt.Sprintf("%s_%s", podUID, image)
	if m.backOff.IsInBackOffSinceUpdate(backOffKey, m.backOff.Clock.Now()) {
		msg := fmt.Sprintf("Back-off pulling image %q", image)
		m.logIt(logger, objRef, v1.EventTypeNormal, events.BackOffPullImage, logPrefix, msg)

		// 包装真实拉取中产生的错误信息，它会填充到 pod 的
		// .status.containerStatuses[*].state.waiting.message 中。
		prevPullErrMsg, ok := m.prevPullErrMsg.Load(backOffKey)
		if ok {
			msg = fmt.Sprintf("%s: %s", msg, prevPullErrMsg)
		}

		return "", msg, ErrImagePullBackOff
	}
	// 清理已结算的退避键，确保 prevPullErrMsg 不会无界增长。
	m.prevPullErrMsg.Delete(backOffKey)

	m.podPullingTimeRecorder.RecordImageStartedPulling(podUID)
	m.logIt(logger, objRef, v1.EventTypeNormal, events.PullingImage, logPrefix, fmt.Sprintf("Pulling image %q", image))
	startTime := time.Now()

	pullChan := make(chan pullResult)
	go m.puller.pullImage(ctx, imgSpec, pullCredentials, pullChan, podSandboxConfig)
	imagePullResult := <-pullChan
	if imagePullResult.err != nil {
		m.logIt(logger, objRef, v1.EventTypeWarning, events.FailedToPullImage, logPrefix, fmt.Sprintf("Failed to pull image %q: %v", image, imagePullResult.err))
		m.backOff.Next(backOffKey, m.backOff.Clock.Now())
		msg, err := evalCRIPullErr(image, imagePullResult.err)

		// 暂存真实拉取错误，用于退避期间向用户展示具体的失败原因。
		m.prevPullErrMsg.Store(backOffKey, fmt.Sprintf("%s: %s", err, msg))

		return "", msg, err
	}
	m.podPullingTimeRecorder.RecordImageFinishedPulling(podUID)
	imagePullDuration := time.Since(startTime).Truncate(time.Millisecond)
	m.logIt(logger, objRef, v1.EventTypeNormal, events.PulledImage, logPrefix, fmt.Sprintf("Successfully pulled image %q in %v (%v including waiting). Image size: %v bytes.",
		image, imagePullResult.pullDuration.Truncate(time.Millisecond), imagePullDuration, imagePullResult.imageSize))
	metrics.ImagePullDuration.WithLabelValues(metrics.GetImageSizeBucket(imagePullResult.imageSize)).Observe(imagePullDuration.Seconds())
	m.backOff.GC()
	finalPullCredentials = imagePullResult.credentialsUsed
	pullSucceeded = true

	return imagePullResult.imageRef, "", nil
}

// evalCRIPullErr 把 CRI 拉取错误转换为用户可见的错误消息与统一的错误类型；
// 无法匹配已知错误类型时，回退为通用的 ErrImagePull。
func evalCRIPullErr(imgRef string, err error) (errMsg string, errRes error) {
	// gRPC（远端运行时）错误目前不支持 errors.Is 断言，只能按错误字符串前缀匹配。
	// 参见 https://github.com/grpc/grpc-go/issues/3616
	if strings.HasPrefix(err.Error(), crierrors.ErrRegistryUnavailable.Error()) {
		errMsg = fmt.Sprintf(
			"image pull failed for %s because the registry is unavailable%s",
			imgRef,
			// 去掉错误名前缀，把 "RegistryUnavailable: 更详细的说明" 之类的消息
			// 转换为 "...because the registry is unavailable: 更详细的说明"。
			strings.TrimPrefix(err.Error(), crierrors.ErrRegistryUnavailable.Error()),
		)
		return errMsg, crierrors.ErrRegistryUnavailable
	}

	if strings.HasPrefix(err.Error(), crierrors.ErrSignatureValidationFailed.Error()) {
		errMsg = fmt.Sprintf(
			"image pull failed for %s because the signature validation failed%s",
			imgRef,
			// 去掉错误名前缀，把 "SignatureValidationFailed: 更详细的说明" 之类的消息
			// 转换为 "...because the signature validation failed: 更详细的说明"。
			strings.TrimPrefix(err.Error(), crierrors.ErrSignatureValidationFailed.Error()),
		)
		return errMsg, crierrors.ErrSignatureValidationFailed
	}

	// 没有匹配到特定错误类型时的兜底处理。
	return err.Error(), ErrImagePull
}

// applyDefaultImageTag 解析镜像字符串，若其中不含 tag 或 digest，则补上默认 tag。
func applyDefaultImageTag(image string) (string, error) {
	_, tag, digest, err := parsers.ParseImageName(image)
	if err != nil {
		return "", err
	}
	// 这里直接拼接镜像名与默认 tag，而不是使用 dockerref.WithTag(named, ...)：
	if len(digest) == 0 && len(tag) > 0 && !strings.HasSuffix(image, ":"+tag) {
		// 后者会把短名称（如 busybox）补全为 docker.io/$name。
		// 为保持 CRI 对镜像名与默认主机名无感知，不能发生这种补全，故选择手动拼接。
		image = image + ":" + tag
	}
	return image, nil
}

// trackedToImagePullCreds 把内部凭证记录转换为对外暴露的 ImagePullCredentials。
// 没有来源（节点可访问）的凭证会被标记为节点与 pod 均可访问。
func trackedToImagePullCreds(trackedCreds *credentialprovider.TrackedAuthConfig) *kubeletconfiginternal.ImagePullCredentials {
	ret := &kubeletconfiginternal.ImagePullCredentials{}
	switch {
	case trackedCreds == nil, trackedCreds.Source == nil:
		ret.NodePodsAccessible = true
	case trackedCreds.Source.Secret != nil:
		sourceSecret := trackedCreds.Source.Secret
		ret.KubernetesSecrets = []kubeletconfiginternal.ImagePullSecret{
			{
				UID:            sourceSecret.UID,
				Name:           sourceSecret.Name,
				Namespace:      sourceSecret.Namespace,
				CredentialHash: trackedCreds.AuthConfigHash,
			},
		}
	case trackedCreds.Source.ServiceAccount != nil:
		sourceServiceAccount := trackedCreds.Source.ServiceAccount
		ret.KubernetesServiceAccounts = []kubeletconfiginternal.ImagePullServiceAccount{
			{
				UID:       sourceServiceAccount.UID,
				Name:      sourceServiceAccount.Name,
				Namespace: sourceServiceAccount.Namespace,
			},
		}
	}

	return ret
}
