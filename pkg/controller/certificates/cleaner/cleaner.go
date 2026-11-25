/*
Copyright 2017 The Kubernetes Authors.

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

// Package cleaner 实现自动清理器，对满足特定条件的 CSR（证书签名请求）执行垃圾回收。
// 随着自动化 CSR 申请与自动审批的普及，CSR 数量只会不断增长；证书有效期越短，增长越快，
// 因此需要定期清理以免 etcd 被无用对象堆积占满。
package cleaner

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sync"
	"time"

	capi "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	certificatesinformers "k8s.io/client-go/informers/certificates/v1"
	csrclient "k8s.io/client-go/kubernetes/typed/certificates/v1"
	certificateslisters "k8s.io/client-go/listers/certificates/v1"
	"k8s.io/klog/v2"
)

const (
	// 每隔该时间间隔列出全部 CSR 并逐条检查是否满足清理条件。
	pollingInterval = 1 * time.Hour
	// 不同状态的 CSR 超过以下时长后即可清理。
	approvedExpiration = 1 * time.Hour
	deniedExpiration   = 1 * time.Hour
	pendingExpiration  = 24 * time.Hour
)

// CSRCleanerController 回收过期 CSR（证书签名请求），避免自动创建和审批导致其无限累积。
// 满足以下任一条件时会删除 CSR：
//   - 已批准（Approved）并签发证书，且超过已签发清理时限；
//   - 已拒绝（Denied），且超过拒绝清理时限；
//   - 待处理（Pending），且超过待处理清理时限；
//   - 已批准并签发证书，但证书已过期。
type CSRCleanerController struct {
	csrClient csrclient.CertificateSigningRequestInterface
	csrLister certificateslisters.CertificateSigningRequestLister
}

// NewCSRCleanerController 创建 CSR 清理控制器，通过 informer 的 lister 读取 CSR 缓存。
func NewCSRCleanerController(
	csrClient csrclient.CertificateSigningRequestInterface,
	csrInformer certificatesinformers.CertificateSigningRequestInformer,
) *CSRCleanerController {
	return &CSRCleanerController{
		csrClient: csrClient,
		csrLister: csrInformer.Lister(),
	}
}

// Run 启动 worker 定期轮询并清理 CSR，直到 ctx 结束。
func (ccc *CSRCleanerController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting CSR cleaner controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down CSR cleaner controller")
		wg.Wait()
	}()

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, ccc.worker, pollingInterval)
		})
	}
	<-ctx.Done()
}

// worker 从 informer 缓存列出所有 CSR 并逐条尝试清理；单个失败只记录日志，不阻断其余对象。
func (ccc *CSRCleanerController) worker(ctx context.Context) {
	logger := klog.FromContext(ctx)
	csrs, err := ccc.csrLister.List(labels.Everything())
	if err != nil {
		logger.Error(err, "Unable to list CSRs")
		return
	}
	for _, csr := range csrs {
		if err := ccc.handle(ctx, csr); err != nil {
			logger.Error(err, "Error while attempting to clean CSR", "csr", csr.Name)
		}
	}
}

// handle 判断 CSR 是否满足任一清理条件，满足则调用 API 删除。
func (ccc *CSRCleanerController) handle(ctx context.Context, csr *capi.CertificateSigningRequest) error {
	logger := klog.FromContext(ctx)
	if isIssuedPastDeadline(logger, csr) ||
		isDeniedPastDeadline(logger, csr) ||
		isFailedPastDeadline(logger, csr) ||
		isPendingPastDeadline(logger, csr) ||
		isIssuedExpired(logger, csr) ||
		isApprovedUnissuedPastDeadline(logger, csr) {
		if err := ccc.csrClient.Delete(ctx, csr.Name, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("unable to delete CSR %q: %v", csr.Name, err)
		}
	}
	return nil
}

// isIssuedExpired 判断 CSR 是否已签发证书且证书的 NotAfter 时间已过。
func isIssuedExpired(logger klog.Logger, csr *capi.CertificateSigningRequest) bool {
	for _, c := range csr.Status.Conditions {
		if c.Type == capi.CertificateApproved && isIssued(csr) && isExpired(csr) {
			logger.Info("Cleaning CSR as the associated certificate is expired.", "csr", csr.Name)
			return true
		}
	}
	return false
}

// isPendingPastDeadline 判断 CSR 是否仍处于 Pending（无任何状态条件）且已超过待处理保留期限。
func isPendingPastDeadline(logger klog.Logger, csr *capi.CertificateSigningRequest) bool {
	// 没有任何 status condition 的 CSR 在 kubectl 中显示为 Pending。
	if len(csr.Status.Conditions) == 0 && isOlderThan(csr.CreationTimestamp, pendingExpiration) {
		logger.Info("Cleaning CSR as it is more than pendingExpiration duration old and unhandled.", "csr", csr.Name, "pendingExpiration", pendingExpiration)
		return true
	}
	return false
}

// isDeniedPastDeadline 判断 CSR 是否已被拒绝且拒绝时间超过保留期限。
func isDeniedPastDeadline(logger klog.Logger, csr *capi.CertificateSigningRequest) bool {
	for _, c := range csr.Status.Conditions {
		if c.Type == capi.CertificateDenied && isOlderThan(c.LastUpdateTime, deniedExpiration) {
			logger.Info("Cleaning CSR as it is more than deniedExpiration duration old and denied.", "csr", csr.Name, "deniedExpiration", deniedExpiration)
			return true
		}
	}
	return false
}

// isFailedPastDeadline 判断 CSR 是否已失败且失败时间超过保留期限。
func isFailedPastDeadline(logger klog.Logger, csr *capi.CertificateSigningRequest) bool {
	for _, c := range csr.Status.Conditions {
		if c.Type == capi.CertificateFailed && isOlderThan(c.LastUpdateTime, deniedExpiration) {
			logger.Info("Cleaning CSR as it is more than deniedExpiration duration old and failed.", "csr", csr.Name, "deniedExpiration", deniedExpiration)
			return true
		}
	}
	return false
}

// isIssuedPastDeadline 判断 CSR 是否已签发证书且审批时间超过签发保留期限。
func isIssuedPastDeadline(logger klog.Logger, csr *capi.CertificateSigningRequest) bool {
	if !isIssued(csr) {
		return false
	}
	for _, c := range csr.Status.Conditions {
		if c.Type == capi.CertificateApproved && isOlderThan(c.LastUpdateTime, approvedExpiration) {
			logger.Info("Cleaning CSR as it is more than approvedExpiration duration old and approved.", "csr", csr.Name, "approvedExpiration", approvedExpiration)
			return true
		}
	}
	return false
}

// isApprovedUnissuedPastDeadline 判断 CSR 已获批准但始终未签发证书，且批准时间已超过待处理保留期限。
func isApprovedUnissuedPastDeadline(logger klog.Logger, csr *capi.CertificateSigningRequest) bool {
	if isIssued(csr) {
		return false
	}
	for _, c := range csr.Status.Conditions {
		if c.Type == capi.CertificateApproved && isOlderThan(c.LastUpdateTime, pendingExpiration) {
			logger.Info("Cleaning CSR as it is approved but unissued for more than pendingExpiration duration.", "csr", csr.Name, "pendingExpiration", pendingExpiration)
			return true
		}
	}
	return false
}

// isOlderThan 判断时间 t 非零且距当前已超过 d；零值时间必须排除，否则会被误判为过期。
func isOlderThan(t metav1.Time, d time.Duration) bool {
	return !t.IsZero() && time.Since(t.Time) > d
}

// isIssued 判断 CSR 是否已签发。API 中没有显式的 Issued 状态；只要 CSR 关联了证书，
// kubectl 展示的状态中会隐含 Issued。
func isIssued(csr *capi.CertificateSigningRequest) bool {
	return len(csr.Status.Certificate) > 0
}

// isExpired 解析 CSR 中的证书并判断其 NotAfter 时间是否已过；解析失败按未过期处理，避免误删。
func isExpired(csr *capi.CertificateSigningRequest) bool {
	if len(csr.Status.Certificate) == 0 {
		return false
	}
	block, _ := pem.Decode(csr.Status.Certificate)
	if block == nil {
		return false
	}
	certs, err := x509.ParseCertificates(block.Bytes)
	if err != nil {
		return false
	}
	if len(certs) == 0 {
		return false
	}
	return time.Now().After(certs[0].NotAfter)
}
