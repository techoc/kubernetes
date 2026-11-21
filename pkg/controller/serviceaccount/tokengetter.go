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

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	admissionregistrationv1listers "k8s.io/client-go/listers/admissionregistration/v1"
	v1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/kubernetes/pkg/serviceaccount"
)

// clientGetter 通过 clientset.Interface 实现 ServiceAccountTokenGetter。
// 每个方法都先查 informer lister（本地缓存），未命中时再回退到 API client，
// 以尽量减少对 API server 的直接请求。
type clientGetter struct {
	client                               clientset.Interface
	secretLister                         v1listers.SecretLister
	serviceAccountLister                 v1listers.ServiceAccountLister
	podLister                            v1listers.PodLister
	nodeLister                           v1listers.NodeLister
	validatingWebhookConfigurationLister admissionregistrationv1listers.ValidatingWebhookConfigurationLister
	mutatingWebhookConfigurationLister   admissionregistrationv1listers.MutatingWebhookConfigurationLister
}

// NewGetterFromClient 返回一个 ServiceAccountTokenGetter，使用指定的 client 与 lister
// 获取 service account、pod、secret、node 及 webhook 配置。
// 注意：该 client 不能使用“由本 getter 获取的 service account token”进行认证，
// 否则获取 token 的过程会再次触发获取，形成递归。
func NewGetterFromClient(c clientset.Interface, secretLister v1listers.SecretLister, serviceAccountLister v1listers.ServiceAccountLister, podLister v1listers.PodLister, nodeLister v1listers.NodeLister, validatingWebhookConfigurationLister admissionregistrationv1listers.ValidatingWebhookConfigurationLister, mutatingWebhookConfigurationLister admissionregistrationv1listers.MutatingWebhookConfigurationLister) serviceaccount.ServiceAccountTokenGetter {
	return clientGetter{c, secretLister, serviceAccountLister, podLister, nodeLister, validatingWebhookConfigurationLister, mutatingWebhookConfigurationLister}
}

// GetServiceAccount 获取指定命名空间下的 ServiceAccount：先查本地 lister 缓存，未命中则回退到 API。
func (c clientGetter) GetServiceAccount(ctx context.Context, namespace, name string) (*v1.ServiceAccount, error) {
	if serviceAccount, err := c.serviceAccountLister.ServiceAccounts(namespace).Get(name); err == nil {
		return serviceAccount, nil
	}
	return c.client.CoreV1().ServiceAccounts(namespace).Get(ctx, name, metav1.GetOptions{})
}

// GetPod 获取指定命名空间下的 Pod：先查本地 lister 缓存，未命中则回退到 API。
func (c clientGetter) GetPod(ctx context.Context, namespace, name string) (*v1.Pod, error) {
	if pod, err := c.podLister.Pods(namespace).Get(name); err == nil {
		return pod, nil
	}
	return c.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
}

// GetSecret 获取指定命名空间下的 Secret：先查本地 lister 缓存，未命中则回退到 API。
func (c clientGetter) GetSecret(ctx context.Context, namespace, name string) (*v1.Secret, error) {
	if secret, err := c.secretLister.Secrets(namespace).Get(name); err == nil {
		return secret, nil
	}
	return c.client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
}

// GetNode 获取指定名称的 Node：先查本地 lister 缓存，未命中则回退到 API。
func (c clientGetter) GetNode(ctx context.Context, name string) (*v1.Node, error) {
	// 相关 feature 未启用时 nodeLister 不会被设置，此时直接返回 NotFound，
	// 避免空指针解引用，并让调用方按“节点不存在”处理。
	if c.nodeLister == nil {
		return nil, apierrors.NewNotFound(v1.Resource("nodes"), name)
	}
	if node, err := c.nodeLister.Get(name); err == nil {
		return node, nil
	}
	return c.client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
}

// GetValidatingWebhookConfiguration 获取指定名称的 ValidatingWebhookConfiguration：
// 先查本地 lister 缓存，未命中则回退到 API。
func (c clientGetter) GetValidatingWebhookConfiguration(ctx context.Context, name string) (*admissionregistrationv1.ValidatingWebhookConfiguration, error) {
	if validatingWebhookCfg, err := c.validatingWebhookConfigurationLister.Get(name); err == nil {
		return validatingWebhookCfg, nil
	}
	return c.client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, name, metav1.GetOptions{})
}

// GetMutatingWebhookConfiguration 获取指定名称的 MutatingWebhookConfiguration：
// 先查本地 lister 缓存，未命中则回退到 API。
func (c clientGetter) GetMutatingWebhookConfiguration(ctx context.Context, name string) (*admissionregistrationv1.MutatingWebhookConfiguration, error) {
	if mutatingWebhookCfg, err := c.mutatingWebhookConfigurationLister.Get(name); err == nil {
		return mutatingWebhookCfg, nil
	}
	return c.client.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, name, metav1.GetOptions{})
}
