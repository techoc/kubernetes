/*
Copyright 2020 The Kubernetes Authors.

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

package filters

import (
	"errors"
	"fmt"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storageversion"
	_ "k8s.io/component-base/metrics/prometheus/workqueue" // 用于 workqueue 指标注册
	"k8s.io/klog/v2"
)

// WithStorageVersionPrecondition 检查存储版本屏障是否已完成；若未完成，只放行以下请求：
// 1. 非资源请求，
// 2. 读请求，
// 3. 对 storageversion API 的写请求，
// 4. apiserver 自身发往 namespace API 的 create 请求，
// 5. 对 kube-system 命名空间下 lease API 的写请求，
// 6. StorageVersion 不处于待更新状态的资源，包括未持久化的资源。
//
// 背景：升级后必须先确认所有对象都已迁移到目标存储版本，才能安全地读写；
// 在此之前的写请求可能产生混合版本数据，让后续迁移无法判断真实版本。
// 因此这里用"拒绝写、放行读"的闸门，并以 503 提示客户端重试，而不是永久拒绝。
func WithStorageVersionPrecondition(handler http.Handler, svm storageversion.Manager, s runtime.NegotiatedSerializer) http.Handler {
	if svm == nil {
		// TODO(roycaihw): 该特性升级到 beta/GA 后改为输出警告
		// nil manager 表示屏障被关闭（例如未启用相关特性），直接透传，避免误伤。
		klog.V(2).Infof("Storage Version barrier is disabled")
		return handler
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if svm.Completed() {
			handler.ServeHTTP(w, req)
			return
		}
		ctx := req.Context()
		requestInfo, found := request.RequestInfoFrom(ctx)
		if !found {
			responsewriters.InternalError(w, req, errors.New("no RequestInfo found in the context"))
			return
		}
		// 非资源请求（/healthz、/version 等）不触碰存储，放行以免影响探活。
		if !requestInfo.IsResourceRequest {
			handler.ServeHTTP(w, req)
			return
		}
		// 读请求不会写入新版本数据，放行可以缩短升级期间的服务不可用窗口。
		if requestInfo.Verb == "get" || requestInfo.Verb == "list" || requestInfo.Verb == "watch" {
			handler.ServeHTTP(w, req)
			return
		}
		// 屏障自身依赖 storageversions 资源上报进度，必须允许写，否则会死锁。
		if requestInfo.APIGroup == "internal.apiserver.k8s.io" && requestInfo.Resource == "storageversions" {
			handler.ServeHTTP(w, req)
			return
		}
		// apiserver-identity 租约依赖于系统命名空间存在，因此允许 apiserver 自身创建命名空间。
		// 注意：有了这条例外，如果 bootstrap client 用新版本写入命名空间，
		// 而升级后的 apiserver 在更新 namespaces 的 StorageVersion 之前崩溃，
		// 存储迁移器就无法判断这些命名空间在 etcd 中以另一个版本存储。
		// 由于 bootstrap client 只创建系统命名空间、不会更新它们，这种情况只会发生在：
		// 升级后的 apiserver 是最先触发命名空间创建的实例，
		// 或者加入已有集群的升级实例需要创建新的系统命名空间
		// （除 kube-system、kube-public、kube-node-lease 之外）。
		//
		// 仅限 apiserver 自身身份且带 system:masters 组，防止普通用户借这条例外绕过屏障。
		u, hasUser := request.UserFrom(ctx)
		if requestInfo.APIGroup == "" && requestInfo.Resource == "namespaces" &&
			requestInfo.Verb == "create" && hasUser &&
			u.GetName() == user.APIServerUser && contains(u.GetGroups(), user.SystemPrivilegedGroup) {
			handler.ServeHTTP(w, req)
			return
		}
		// 放行对 kube-system 下 lease API 的写请求。storage version API 依赖 apiserver-identity
		// 租约才能工作。kube-system 中的租约要么是 apiserver-identity 租约（过期后会被垃圾回收），
		// 要么是 leader-election 租约（由系统组件周期性更新）。这两类租约都不会在 etcd 中变为陈旧数据。
		//
		// lease 写被放行是因为身份租约是屏障推进的前提，且这类数据本身不参与资源版本迁移。
		if requestInfo.APIGroup == "coordination.k8s.io" && requestInfo.Resource == "leases" &&
			requestInfo.Namespace == metav1.NamespaceSystem {
			handler.ServeHTTP(w, req)
			return
		}
		// 如果该资源的 StorageVersion 不在待更新列表中，直接放行。
		// 未持久化的资源不在待更新列表中，因此也会放行。
		gr := schema.GroupResource{Group: requestInfo.APIGroup, Resource: requestInfo.Resource}
		if !svm.PendingUpdate(gr) {
			handler.ServeHTTP(w, req)
			return
		}

		// 命中待迁移资源：返回 503 而非 500，明确这是暂时状态，客户端应重试。
		// 错误信息带上最近一次失败原因，便于运维定位迁移卡在哪里。
		gv := schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
		responsewriters.ErrorNegotiated(apierrors.NewServiceUnavailable(fmt.Sprintf("wait for storage version registration to complete for resource: %v, last seen error: %v", gr, svm.LastUpdateError(gr))), s, gv, w, req)
	})
}

// contains 判断字符串切片中是否存在目标元素；组数量很小，线性扫描即可。
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
