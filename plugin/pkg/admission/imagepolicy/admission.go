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

// Package imagepolicy 包含一个准入控制器，可配置 webhook 并将策略决策委托给它。
package imagepolicy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/api/imagepolicy/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"

	// 注册 client-go 镜像策略 API 以便 api registry 使用。
	_ "k8s.io/kubernetes/pkg/apis/imagepolicy/install"
)

// PluginName 是准入插件的名称。
const PluginName = "ImagePolicyWebhook"
const ephemeralcontainers = "ephemeralcontainers"

// AuditKeyPrefix 是本插件所有审计 key 的前缀，常见后缀见下方常量。
var AuditKeyPrefix = strings.ToLower(PluginName) + ".image-policy.k8s.io/"

const (
	// ImagePolicyFailedOpenKeySuffix 作为注解后缀时，表示 webhook 后端连接失败，
	// 镜像审查以 fail-open 方式放行。
	ImagePolicyFailedOpenKeySuffix string = "failed-open"

	// ImagePolicyAuditRequiredKeySuffix 作为注解后缀时，表示该 Pod 需要被审计。
	ImagePolicyAuditRequiredKeySuffix string = "audit-required"
)

var (
	groupVersions = []schema.GroupVersion{v1alpha1.SchemeGroupVersion}
)

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		newImagePolicyWebhook, err := NewImagePolicyWebhook(config)
		if err != nil {
			return nil, err
		}
		return newImagePolicyWebhook, nil
	})
}

// Plugin 实现 admission.Interface。
type Plugin struct {
	*admission.Handler
	webhook       *webhook.GenericWebhook
	responseCache *cache.LRUExpireCache
	allowTTL      time.Duration
	denyTTL       time.Duration
	defaultAllow  bool
}

var _ admission.ValidationInterface = &Plugin{}

// statusTTL 根据审查结果返回对应的缓存有效期。
func (a *Plugin) statusTTL(status v1alpha1.ImageReviewStatus) time.Duration {
	if status.Allowed {
		return a.allowTTL
	}
	return a.denyTTL
}

// filterAnnotations 过滤掉不匹配 *.image-policy.k8s.io/* 的注解。
func (a *Plugin) filterAnnotations(allAnnotations map[string]string) map[string]string {
	annotations := make(map[string]string)
	for k, v := range allAnnotations {
		if strings.Contains(k, ".image-policy.k8s.io/") {
			annotations[k] = v
		}
	}
	return annotations
}

// webhookError 处理 webhook 调用失败；放行还是拒绝由 defaultAllow 决定。
func (a *Plugin) webhookError(pod *api.Pod, attributes admission.Attributes, err error) error {
	if err != nil {
		klog.V(2).Infof("error contacting webhook backend: %s", err)
		if a.defaultAllow {
			attributes.AddAnnotation(AuditKeyPrefix+ImagePolicyFailedOpenKeySuffix, "true")
			// TODO(wteiken): Remove the annotation code for the 1.13 release
			annotations := pod.GetAnnotations()
			if annotations == nil {
				annotations = make(map[string]string)
			}
			annotations[api.ImagePolicyFailedOpenKey] = "true"
			pod.ObjectMeta.SetAnnotations(annotations)

			klog.V(2).Infof("resource allowed in spite of webhook backend failure")
			return nil
		}
		klog.V(2).Infof("resource not allowed due to webhook backend failure ")
		return admission.NewForbidden(attributes, err)
	}
	return nil
}

// Validate 根据请求属性作出准入决策。
func (a *Plugin) Validate(ctx context.Context, attributes admission.Attributes, o admission.ObjectInterfaces) (err error) {
	// 只处理 Pod 及 ephemeralcontainers 子资源请求。
	subresource := attributes.GetSubresource()
	if (subresource != "" && subresource != ephemeralcontainers) || attributes.GetResource().GroupResource() != api.Resource("pods") {
		return nil
	}

	pod, ok := attributes.GetObject().(*api.Pod)
	if !ok {
		return apierrors.NewBadRequest("Resource was marked with kind Pod but was unable to be converted")
	}

	// 根据操作类型收集待审查的镜像列表。
	var imageReviewContainerSpecs []v1alpha1.ImageReviewContainerSpec
	if subresource == "" {
		containers := make([]api.Container, 0, len(pod.Spec.Containers)+len(pod.Spec.InitContainers))
		containers = append(containers, pod.Spec.Containers...)
		containers = append(containers, pod.Spec.InitContainers...)
		for _, c := range containers {
			imageReviewContainerSpecs = append(imageReviewContainerSpecs, v1alpha1.ImageReviewContainerSpec{
				Image: c.Image,
			})
		}
	} else if subresource == ephemeralcontainers {
		for _, c := range pod.Spec.EphemeralContainers {
			imageReviewContainerSpecs = append(imageReviewContainerSpecs, v1alpha1.ImageReviewContainerSpec{
				Image: c.Image,
			})
		}
	}
	imageReview := v1alpha1.ImageReview{
		Spec: v1alpha1.ImageReviewSpec{
			Containers:  imageReviewContainerSpecs,
			Annotations: a.filterAnnotations(pod.Annotations),
			Namespace:   attributes.GetNamespace(),
		},
	}
	if err := a.admitPod(ctx, pod, attributes, &imageReview); err != nil {
		return admission.NewForbidden(attributes, err)
	}
	return nil
}

// admitPod 向 webhook 发起镜像审查，并将结果应用到当前请求。
func (a *Plugin) admitPod(ctx context.Context, pod *api.Pod, attributes admission.Attributes, review *v1alpha1.ImageReview) error {
	// 以审查请求内容为缓存键，避免对相同输入重复调用 webhook。
	cacheKey, err := json.Marshal(review.Spec)
	if err != nil {
		return err
	}
	if entry, ok := a.responseCache.Get(string(cacheKey)); ok {
		review.Status = entry.(v1alpha1.ImageReviewStatus)
	} else {
		result := a.webhook.WithExponentialBackoff(ctx, func() rest.Result {
			return a.webhook.RestClient.Post().Body(review).Do(ctx)
		})

		if err := result.Error(); err != nil {
			return a.webhookError(pod, attributes, err)
		}
		var statusCode int
		if result.StatusCode(&statusCode); statusCode < 200 || statusCode >= 300 {
			return a.webhookError(pod, attributes, fmt.Errorf("Error contacting webhook: %d", statusCode))
		}

		if err := result.Into(review); err != nil {
			return a.webhookError(pod, attributes, err)
		}

		a.responseCache.Add(string(cacheKey), review.Status, a.statusTTL(review.Status))
	}

	// 将 webhook 返回的审计注解透传到审计事件中。
	for k, v := range review.Status.AuditAnnotations {
		if err := attributes.AddAnnotation(AuditKeyPrefix+k, v); err != nil {
			klog.Warningf("failed to set admission audit annotation %s to %s: %v", AuditKeyPrefix+k, v, err)
		}
	}
	if !review.Status.Allowed {
		if len(review.Status.Reason) > 0 {
			return fmt.Errorf("image policy webhook backend denied one or more images: %s", review.Status.Reason)
		}
		return errors.New("one or more images rejected by webhook backend")
	}
	return nil
}

// NewImagePolicyWebhook 根据提供的配置文件创建一个新的 ImagePolicyWebhook 插件。
// 配置文件由 --admission-control-config-file 指定，webhook 部分的格式如下：
//
//	{
//	  "imagePolicy": {
//	     "kubeConfigFile": "path/to/kubeconfig/for/backend",
//	     "allowTTL": 30,           # 批准结果的缓存秒数
//	     "denyTTL": 30,            # 拒绝结果的缓存秒数
//	     "retryBackoff": 500,      # 重试间隔毫秒数
//	     "defaultAllow": true      # webhook 后端失败时的默认行为
//	  }
//	}
//
// 配置文件可以是 json 或 yaml 格式。
//
// kubeconfig 属性指向另一个 kubeconfig 格式的文件，用于指定如何连接 webhook 后端。
//
// kubeconfig 的 cluster 字段指向远程服务，user 字段指向返回结果的鉴权主体。
//
//	# clusters 指向远程服务。
//	clusters:
//	- name: name-of-remote-imagepolicy-service
//	  cluster:
//	    certificate-authority: /path/to/ca.pem      # 校验远程服务的 CA。
//	    server: https://images.example.com/policy # 待查询的远程服务 URL，必须使用 'https'。
//
//	# users 指向 API server 的 webhook 配置。
//	users:
//	- name: name-of-api-server
//	  user:
//	    client-certificate: /path/to/cert.pem # webhook 插件使用的证书
//	    client-key: /path/to/key.pem          # 与证书匹配的私钥
//
// 更多 HTTP 配置请参考 kubeconfig 文档
// http://kubernetes.io/v1.1/docs/user-guide/kubeconfig-file.html。
func NewImagePolicyWebhook(configFile io.Reader) (*Plugin, error) {
	if configFile == nil {
		return nil, fmt.Errorf("no config specified")
	}

	// TODO: move this to a versioned configuration file format
	var config AdmissionConfig
	d := yaml.NewYAMLOrJSONDecoder(configFile, 4096)
	err := d.Decode(&config)
	if err != nil {
		return nil, err
	}

	whConfig := config.ImagePolicyWebhook
	if err := normalizeWebhookConfig(&whConfig); err != nil {
		return nil, err
	}

	clientConfig, err := webhook.LoadKubeconfig(whConfig.KubeConfigFile, nil)
	if err != nil {
		return nil, err
	}
	retryBackoff := webhook.DefaultRetryBackoffWithInitialDelay(whConfig.RetryBackoff)
	gw, err := webhook.NewGenericWebhook(legacyscheme.Scheme, legacyscheme.Codecs, clientConfig, groupVersions, retryBackoff)
	if err != nil {
		return nil, err
	}
	return &Plugin{
		Handler:       admission.NewHandler(admission.Create, admission.Update),
		webhook:       gw,
		responseCache: cache.NewLRUExpireCache(1024),
		allowTTL:      whConfig.AllowTTL,
		denyTTL:       whConfig.DenyTTL,
		defaultAllow:  whConfig.DefaultAllow,
	}, nil
}
