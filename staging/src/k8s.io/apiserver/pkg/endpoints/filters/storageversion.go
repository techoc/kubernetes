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
	_ "k8s.io/component-base/metrics/prometheus/workqueue" // for workqueue metric registration
	"k8s.io/klog/v2"
)

// WithStorageVersionPrecondition checks if the storage version barrier has
// completed, if not, it only passes the following API requests:
// 1. non-resource requests,
// 2. read requests,
// 3. write requests to the storageversion API,
// 4. create requests to the namespace API sent by apiserver itself,
// 5. write requests to the lease API in kube-system namespace,
// 6. resources whose StorageVersion is not pending update, including non-persisted resources.
//
// 背景：升级后必须先确认所有对象都已迁移到目标存储版本，才能安全地读写；
// 在此之前的写请求可能产生混合版本数据，让后续迁移无法判断真实版本。
// 因此这里用"拒绝写、放行读"的闸门，并以 503 提示客户端重试，而不是永久拒绝。
func WithStorageVersionPrecondition(handler http.Handler, svm storageversion.Manager, s runtime.NegotiatedSerializer) http.Handler {
	if svm == nil {
		// TODO(roycaihw): switch to warning after the feature graduate to beta/GA
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
		// Allow non-resource requests
		// 非资源请求（/healthz、/version 等）不触碰存储，放行以免影响探活。
		if !requestInfo.IsResourceRequest {
			handler.ServeHTTP(w, req)
			return
		}
		// Allow read requests
		// 读请求不会写入新版本数据，放行可以缩短升级期间的服务不可用窗口。
		if requestInfo.Verb == "get" || requestInfo.Verb == "list" || requestInfo.Verb == "watch" {
			handler.ServeHTTP(w, req)
			return
		}
		// Allow writes to the storage version API
		// 屏障自身依赖 storageversions 资源上报进度，必须允许写，否则会死锁。
		if requestInfo.APIGroup == "internal.apiserver.k8s.io" && requestInfo.Resource == "storageversions" {
			handler.ServeHTTP(w, req)
			return
		}
		// The system namespace is required for apiserver-identity lease to exist. Allow the apiserver
		// itself to create namespaces.
		// NOTE: with this exception, if the bootstrap client writes namespaces with a new version,
		// and the upgraded apiserver dies before updating the StorageVersion for namespaces, the
		// storage migrator won't be able to tell these namespaces are stored in a different version in etcd.
		// Because the bootstrap client only creates system namespace and doesn't update them, this can
		// only happen if the upgraded apiserver is the first apiserver that kicks off namespace creation,
		// or if an upgraded server that joins an existing cluster has new system namespaces (other
		// than kube-system, kube-public, kube-node-lease) that need to be created.
		//
		// 仅限 apiserver 自身身份且带 system:masters 组，防止普通用户借这条例外绕过屏障。
		u, hasUser := request.UserFrom(ctx)
		if requestInfo.APIGroup == "" && requestInfo.Resource == "namespaces" &&
			requestInfo.Verb == "create" && hasUser &&
			u.GetName() == user.APIServerUser && contains(u.GetGroups(), user.SystemPrivilegedGroup) {
			handler.ServeHTTP(w, req)
			return
		}
		// Allow writes to the lease API in kube-system. The storage version API depends on the
		// apiserver-identity leases to operate. Leases in kube-system are either apiserver-identity
		// lease (which gets garbage collected when stale) or leader-election leases (which gets
		// periodically updated by system components). Both types of leases won't be stale in etcd.
		//
		// lease 写被放行是因为身份租约是屏障推进的前提，且这类数据本身不参与资源版本迁移。
		if requestInfo.APIGroup == "coordination.k8s.io" && requestInfo.Resource == "leases" &&
			requestInfo.Namespace == metav1.NamespaceSystem {
			handler.ServeHTTP(w, req)
			return
		}
		// If the resource's StorageVersion is not in the to-be-updated list, let it pass.
		// Non-persisted resources are not in the to-be-updated list, so they will pass.
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
