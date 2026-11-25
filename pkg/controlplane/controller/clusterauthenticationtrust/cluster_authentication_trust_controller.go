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

package clusterauthenticationtrust

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/request/headerrequest"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	corev1informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// 本包使用的常量定义。
const (
	configMapNamespace = "kube-system"
	configMapName      = "extension-apiserver-authentication"
)

// Controller 持有控制器的运行状态。
type Controller struct {
	requiredAuthenticationData ClusterAuthenticationInfo

	configMapLister corev1listers.ConfigMapLister
	configMapClient corev1client.ConfigMapsGetter
	namespaceClient corev1client.NamespacesGetter

	// 工作队列用于去重并支持出错时方便的限流重试；这里只会放入一个条目，但仍按 namespace/name 作为 key。
	queue workqueue.TypedRateLimitingInterface[string]

	// 跟踪 kube-system ConfigMap 通知器，以便在 Run 中启动它。
	kubeSystemConfigMapInformer cache.SharedIndexInformer

	// preRunCaches 是控制循环开始前需要同步的缓存。
	preRunCaches []cache.InformerSynced
}

// ClusterAuthenticationInfo 保存将要写入公开 ConfigMap 的信息。
type ClusterAuthenticationInfo struct {
	// ClientCA 用于校验普通客户端身份。
	ClientCA dynamiccertificates.CAContentProvider

	// RequestHeaderUsernameHeaders 是本 apiserver 用于确定用户名的请求头。
	RequestHeaderUsernameHeaders headerrequest.StringSliceProvider
	// RequestHeaderUIDHeaders 是用于确定 UID 的请求头。
	RequestHeaderUIDHeaders headerrequest.StringSliceProvider
	// RequestHeaderGroupHeaders 是用于确定用户组的请求头。
	RequestHeaderGroupHeaders headerrequest.StringSliceProvider
	// RequestHeaderExtraHeaderPrefixes 是用于确定 user.extra 的请求头前缀。
	RequestHeaderExtraHeaderPrefixes headerrequest.StringSliceProvider
	// RequestHeaderAllowedNames 是允许充当前端代理的主体。
	RequestHeaderAllowedNames headerrequest.StringSliceProvider
	// RequestHeaderCA 用于校验前端代理。
	RequestHeaderCA dynamiccertificates.CAContentProvider
}

// NewClusterAuthenticationTrustController 返回一个控制器，维护 kube-system/extension-apiserver-authentication ConfigMap，
// 其中保存聚合 apiserver 被推荐（但非强制）采用的自配置方式。
func NewClusterAuthenticationTrustController(requiredAuthenticationData ClusterAuthenticationInfo, kubeClient kubernetes.Interface) *Controller {
	// 只需一个 namespace 下的少量数据，因此自建专用 informer 而非共享工厂。
	kubeSystemConfigMapInformer := corev1informers.NewConfigMapInformer(kubeClient, configMapNamespace, 12*time.Hour, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})

	c := &Controller{
		requiredAuthenticationData: requiredAuthenticationData,
		configMapLister:            corev1listers.NewConfigMapLister(kubeSystemConfigMapInformer.GetIndexer()),
		configMapClient:            kubeClient.CoreV1(),
		namespaceClient:            kubeClient.CoreV1(),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "cluster_authentication_trust_controller"},
		),
		preRunCaches:                []cache.InformerSynced{kubeSystemConfigMapInformer.HasSynced},
		kubeSystemConfigMapInformer: kubeSystemConfigMapInformer,
	}

	kubeSystemConfigMapInformer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			if cast, ok := obj.(*corev1.ConfigMap); ok {
				return cast.Namespace == configMapNamespace && cast.Name == configMapName
			}
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				if cast, ok := tombstone.Obj.(*corev1.ConfigMap); ok {
					return cast.Namespace == configMapNamespace && cast.Name == configMapName
				}
			}
			return true // 兜底放行；上述匹配检查成本很低
		},
		Handler: cache.ResourceEventHandlerFuncs{
			// informer 已带过滤器，任何回调都直接入队即可；
			// 只检查一个 ConfigMap，无需挑剔 key。
			AddFunc: func(obj interface{}) {
				c.queue.Add(keyFn())
			},
			DeleteFunc: func(obj interface{}) {
				c.queue.Add(keyFn())
			},
		},
	})

	return c
}

// syncConfigMap 将当前认证信息同步到 ConfigMap。
func (c *Controller) syncConfigMap() error {
	originalAuthConfigMap, err := c.configMapLister.ConfigMaps(configMapNamespace).Get(configMapName)
	if apierrors.IsNotFound(err) {
		originalAuthConfigMap = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: configMapNamespace, Name: configMapName},
		}
	} else if err != nil {
		return err
	}
	// 保留原对象以便更新前做差异比较。
	authConfigMap := originalAuthConfigMap.DeepCopy()

	existingAuthenticationInfo, err := getClusterAuthenticationInfoFor(originalAuthConfigMap.Data)
	if err != nil {
		return err
	}
	combinedInfo, err := combinedClusterAuthenticationInfo(existingAuthenticationInfo, c.requiredAuthenticationData)
	if err != nil {
		return err
	}
	authConfigMap.Data, err = getConfigMapDataFor(combinedInfo)
	if err != nil {
		return err
	}

	if equality.Semantic.DeepEqual(authConfigMap, originalAuthConfigMap) {
		klog.V(5).Info("no changes to configmap")
		return nil
	}
	klog.V(2).Infof("writing updated authentication info to  %s configmaps/%s", configMapNamespace, configMapName)

	if err := createNamespaceIfNeeded(c.namespaceClient, authConfigMap.Namespace); err != nil {
		return err
	}
	if err := writeConfigMap(c.configMapClient, authConfigMap); err != nil {
		return err
	}

	return nil
}

// createNamespaceIfNeeded 在命名空间不存在时创建它。
func createNamespaceIfNeeded(nsClient corev1client.NamespacesGetter, ns string) error {
	if _, err := nsClient.Namespaces().Get(context.TODO(), ns, metav1.GetOptions{}); err == nil {
		// 命名空间已存在。
		return nil
	}
	newNs := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ns,
			Namespace: "",
		},
	}
	_, err := nsClient.Namespaces().Create(context.TODO(), newNs, metav1.CreateOptions{})
	if err != nil && apierrors.IsAlreadyExists(err) {
		err = nil
	}
	return err
}

// writeConfigMap 创建或更新目标 ConfigMap。
func writeConfigMap(configMapClient corev1client.ConfigMapsGetter, required *corev1.ConfigMap) error {
	_, err := configMapClient.ConfigMaps(required.Namespace).Update(context.TODO(), required, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) {
		_, err := configMapClient.ConfigMaps(required.Namespace).Create(context.TODO(), required, metav1.CreateOptions{})
		return err
	}

	// ConfigMap 过大时清空全部内容，依赖本控制器（或其他控制器）重新写入正确数据。
	// 返回原始错误使控制器重新入队。过大指：
	//   1. 请求过大被通用请求拦截器发现
	//   2. 内容过大导致服务端返回校验错误 "Too long: must have at most 1048576 characters"
	if apierrors.IsRequestEntityTooLargeError(err) || (apierrors.IsInvalid(err) && strings.Contains(err.Error(), "Too long")) {
		if deleteErr := configMapClient.ConfigMaps(required.Namespace).Delete(context.TODO(), required.Name, metav1.DeleteOptions{}); deleteErr != nil {
			return deleteErr
		}
		return err
	}

	return err
}

// combinedClusterAuthenticationInfo 把两份认证信息合并为一份。
func combinedClusterAuthenticationInfo(lhs, rhs ClusterAuthenticationInfo) (ClusterAuthenticationInfo, error) {
	ret := ClusterAuthenticationInfo{
		RequestHeaderAllowedNames:        combineUniqueStringSlices(lhs.RequestHeaderAllowedNames, rhs.RequestHeaderAllowedNames),
		RequestHeaderExtraHeaderPrefixes: combineUniqueStringSlices(lhs.RequestHeaderExtraHeaderPrefixes, rhs.RequestHeaderExtraHeaderPrefixes),
		RequestHeaderGroupHeaders:        combineUniqueStringSlices(lhs.RequestHeaderGroupHeaders, rhs.RequestHeaderGroupHeaders),
		RequestHeaderUsernameHeaders:     combineUniqueStringSlices(lhs.RequestHeaderUsernameHeaders, rhs.RequestHeaderUsernameHeaders),
		RequestHeaderUIDHeaders:          combineUniqueStringSlices(lhs.RequestHeaderUIDHeaders, rhs.RequestHeaderUIDHeaders),
	}

	var err error
	ret.ClientCA, err = combineCertLists(lhs.ClientCA, rhs.ClientCA)
	if err != nil {
		return ClusterAuthenticationInfo{}, err
	}
	ret.RequestHeaderCA, err = combineCertLists(lhs.RequestHeaderCA, rhs.RequestHeaderCA)
	if err != nil {
		return ClusterAuthenticationInfo{}, err
	}

	return ret, nil
}

// getConfigMapDataFor 把认证信息序列化为 ConfigMap 的 data。
func getConfigMapDataFor(authenticationInfo ClusterAuthenticationInfo) (map[string]string, error) {
	data := map[string]string{}
	if authenticationInfo.ClientCA != nil {
		if caBytes := authenticationInfo.ClientCA.CurrentCABundleContent(); len(caBytes) > 0 {
			data["client-ca-file"] = string(caBytes)
		}
	}

	if authenticationInfo.RequestHeaderCA == nil {
		return data, nil
	}

	if caBytes := authenticationInfo.RequestHeaderCA.CurrentCABundleContent(); len(caBytes) > 0 {
		var err error

		// 编码错误不会自行恢复，直接失败。
		data["requestheader-username-headers"], err = jsonSerializeStringSlice(authenticationInfo.RequestHeaderUsernameHeaders.Value())
		if err != nil {
			return nil, err
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.RemoteRequestHeaderUID) && len(authenticationInfo.RequestHeaderUIDHeaders.Value()) > 0 {
			data["requestheader-uid-headers"], err = jsonSerializeStringSlice(authenticationInfo.RequestHeaderUIDHeaders.Value())
			if err != nil {
				return nil, err
			}
		}

		data["requestheader-group-headers"], err = jsonSerializeStringSlice(authenticationInfo.RequestHeaderGroupHeaders.Value())
		if err != nil {
			return nil, err
		}
		data["requestheader-extra-headers-prefix"], err = jsonSerializeStringSlice(authenticationInfo.RequestHeaderExtraHeaderPrefixes.Value())
		if err != nil {
			return nil, err
		}

		data["requestheader-client-ca-file"] = string(caBytes)
		data["requestheader-allowed-names"], err = jsonSerializeStringSlice(authenticationInfo.RequestHeaderAllowedNames.Value())
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

// getClusterAuthenticationInfoFor 从 ConfigMap data 反序列化认证信息。
func getClusterAuthenticationInfoFor(data map[string]string) (ClusterAuthenticationInfo, error) {
	ret := ClusterAuthenticationInfo{}

	var err error
	ret.RequestHeaderGroupHeaders, err = jsonDeserializeStringSlice(data["requestheader-group-headers"])
	if err != nil {
		return ClusterAuthenticationInfo{}, err
	}
	ret.RequestHeaderExtraHeaderPrefixes, err = jsonDeserializeStringSlice(data["requestheader-extra-headers-prefix"])
	if err != nil {
		return ClusterAuthenticationInfo{}, err
	}
	ret.RequestHeaderAllowedNames, err = jsonDeserializeStringSlice(data["requestheader-allowed-names"])
	if err != nil {
		return ClusterAuthenticationInfo{}, err
	}
	ret.RequestHeaderUsernameHeaders, err = jsonDeserializeStringSlice(data["requestheader-username-headers"])
	if err != nil {
		return ClusterAuthenticationInfo{}, err
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.RemoteRequestHeaderUID) {
		ret.RequestHeaderUIDHeaders, err = jsonDeserializeStringSlice(data["requestheader-uid-headers"])
		if err != nil {
			return ClusterAuthenticationInfo{}, err
		}
	}

	if caBundle := data["requestheader-client-ca-file"]; len(caBundle) > 0 {
		ret.RequestHeaderCA, err = dynamiccertificates.NewStaticCAContent("existing", []byte(caBundle))
		if err != nil {
			return ClusterAuthenticationInfo{}, err
		}
	}

	if caBundle := data["client-ca-file"]; len(caBundle) > 0 {
		ret.ClientCA, err = dynamiccertificates.NewStaticCAContent("existing", []byte(caBundle))
		if err != nil {
			return ClusterAuthenticationInfo{}, err
		}
	}

	return ret, nil
}

// jsonSerializeStringSlice 将字符串切片序列化为 JSON 字符串。
func jsonSerializeStringSlice(in []string) (string, error) {
	out, err := json.Marshal(in)
	if err != nil {
		return "", err
	}
	return string(out), err
}

// jsonDeserializeStringSlice 将 JSON 字符串反序列化为字符串切片提供者。
func jsonDeserializeStringSlice(in string) (headerrequest.StringSliceProvider, error) {
	if len(in) == 0 {
		return nil, nil
	}

	out := []string{}
	if err := json.Unmarshal([]byte(in), &out); err != nil {
		return nil, err
	}
	return headerrequest.StaticStringSlice(out), nil
}

// combineUniqueStringSlices 合并两个字符串切片并去重。
func combineUniqueStringSlices(lhs, rhs headerrequest.StringSliceProvider) headerrequest.StringSliceProvider {
	ret := []string{}
	present := sets.String{}

	if lhs != nil {
		for _, curr := range lhs.Value() {
			if present.Has(curr) {
				continue
			}
			ret = append(ret, curr)
			present.Insert(curr)
		}
	}

	if rhs != nil {
		for _, curr := range rhs.Value() {
			if present.Has(curr) {
				continue
			}
			ret = append(ret, curr)
			present.Insert(curr)
		}
	}

	return headerrequest.StaticStringSlice(ret)
}

// combineCertLists 合并两份 CA 证书列表并去重。
func combineCertLists(lhs, rhs dynamiccertificates.CAContentProvider) (dynamiccertificates.CAContentProvider, error) {
	certificates := []*x509.Certificate{}

	if lhs != nil {
		lhsCABytes := lhs.CurrentCABundleContent()
		lhsCAs, err := cert.ParseCertsPEM(lhsCABytes)
		if err != nil {
			return nil, err
		}
		certificates = append(certificates, lhsCAs...)
	}
	if rhs != nil {
		rhsCABytes := rhs.CurrentCABundleContent()
		rhsCAs, err := cert.ParseCertsPEM(rhsCABytes)
		if err != nil {
			return nil, err
		}
		certificates = append(certificates, rhsCAs...)
	}

	certificates = filterExpiredCerts(certificates...)

	finalCertificates := []*x509.Certificate{}
	// 检查重复项：n^2 复杂度，但实现简单。
	for i := range certificates {
		found := false
		for j := range finalCertificates {
			if reflect.DeepEqual(certificates[i].Raw, finalCertificates[j].Raw) {
				found = true
				break
			}
		}
		if !found {
			finalCertificates = append(finalCertificates, certificates[i])
		}
	}

	finalCABytes, err := encodeCertificates(finalCertificates...)
	if err != nil {
		return nil, err
	}

	if len(finalCABytes) == 0 {
		return nil, nil
	}
	// 该列表保持静态是合理的：组合结果只在写入前计算并每次重新生成。
	return dynamiccertificates.NewStaticCAContent("combined", finalCABytes)
}

// filterExpiredCerts 检查证书包中的证书是否全部有效（未过期），
// 返回仅含有效证书的新包；若无有效证书则返回错误。
// NotAfter 比较允许 5 分钟余量。
func filterExpiredCerts(certs ...*x509.Certificate) []*x509.Certificate {
	fiveMinutesAgo := time.Now().Add(-5 * time.Minute)

	var validCerts []*x509.Certificate
	for _, c := range certs {
		if c.NotAfter.After(fiveMinutesAgo) {
			validCerts = append(validCerts, c)
		}
	}

	return validCerts
}

// Enqueue 供其他控制循环调用，触发控制器重新协调内容。
func (c *Controller) Enqueue() {
	c.queue.Add(keyFn())
}

// Run 运行控制器直到停止。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()
	// 确保关闭工作队列，从而让 worker 退出。
	defer c.queue.ShutDown()

	klog.Infof("Starting cluster_authentication_trust_controller controller")
	defer klog.Infof("Shutting down cluster_authentication_trust_controller controller")

	// 启动范围受限的专用 informer。
	go c.kubeSystemConfigMapInformer.Run(ctx.Done())

	// 开始工作前等待辅助缓存填充完毕。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.preRunCaches...) {
		return
	}

	// 只运行一个 worker。
	go wait.Until(c.runWorker, time.Second, ctx.Done())

	// 检查开销很小，每分钟执行一次以防 fsnotify 再次失效；
	// 启动每分钟重查的定时器，同时用于快速预热控制器。
	_ = wait.PollImmediateUntil(1*time.Minute, func() (bool, error) {
		c.queue.Add(keyFn())
		return false, nil
	}, ctx.Done())

	// 等待停止信号。
	<-ctx.Done()
}

// runWorker 持续处理工作队列中的任务。
func (c *Controller) runWorker() {
	// 热循环直到收到停止信号；processNextWorkItem 会自动等待工作项，
	// 因此无需额外等待。
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理队列中的一个 key，退出时返回 false。
func (c *Controller) processNextWorkItem() bool {
	// 从队列取出下一个工作项，该 key 用于在缓存中查找对象。
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	// 必须向队列标记该工作项已处理完成。
	defer c.queue.Done(key)

	// 对该 key 执行实际处理逻辑。
	err := c.syncConfigMap()
	if err == nil {
		// 无错误时通知队列停止跟踪该 key 的历史，重置逐项限流的失败计数。
		c.queue.Forget(key)
		return true
	}

	// 出现失败必须上报，该方法支持可插拔的错误处理，
	// 例如用于集群监控。
	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", key, err))
	// 失败后应重新入队稍后处理。该方法加入退避，
	// 避免对特定项热循环（它们短期内可能仍无法成功），
	// 同时保护控制器整体（若所有操作都失败，需冷静下来，
	// 否则会饿死其他有用的工作）。
	c.queue.AddRateLimited(key)

	return true
}

// keyFn 返回工作队列中唯一的任务 key。
func keyFn() string {
	// 该格式与单 key 场景下的 DeletionHandlingMetaNamespaceKeyFunc 一致。
	return configMapNamespace + "/" + configMapName
}

// encodeCertificates 将证书编码为 PEM 字节。
func encodeCertificates(certs ...*x509.Certificate) ([]byte, error) {
	b := bytes.Buffer{}
	for _, cert := range certs {
		if err := pem.Encode(&b, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); err != nil {
			return []byte{}, err
		}
	}
	return b.Bytes(), nil
}
