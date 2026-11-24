/*
Copyright 2015 The Kubernetes Authors.

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

package portallocator

// PortAllocationOperation 封装 NodePort 分配事务：新端口立即预留以防双重分配，旧端口延迟到提交时释放；失败时撤销新预留。
// PortAllocationOperation 封装端口分配事务：为避免同一端口被重复分配，新端口立即预留，旧端口延迟释放。
// 提交时尽力释放延迟释放的端口；回滚时尽力释放本事务新分配的端口。
// 典型用法：创建事务并 defer Finish，在写入资源所有者成功后调用 Commit。
type PortAllocationOperation struct {
	pa              Interface
	allocated       []int
	releaseDeferred []int
	shouldRollback  bool
	dryRun          bool
}

// StartOperation 创建端口分配事务，跟踪本事务中的分配和释放操作。
// dryRun 为 true 时只记录操作，不实际分配或释放端口。
// StartOperation 创建事务跟踪器；DryRun 只在本地记录候选端口，不触碰真实端口池。
func StartOperation(pa Interface, dryRun bool) *PortAllocationOperation {
	op := &PortAllocationOperation{}
	op.pa = pa
	op.allocated = []int{}
	op.releaseDeferred = []int{}
	op.shouldRollback = true
	op.dryRun = dryRun
	return op
}

// Finish 通常通过 defer 调用；若事务尚未 Commit，则自动执行回滚。
func (op *PortAllocationOperation) Finish() {
	if op.shouldRollback {
		op.Rollback()
	}
}

// Rollback 尽力撤销本事务已经执行的操作。
// Rollback 释放本事务新申请的端口；延迟释放列表不会执行，因此原有 Service 端口保持占用。
func (op *PortAllocationOperation) Rollback() []error {
	if op.dryRun {
		return nil
	}

	errors := []error{}

	for _, allocated := range op.allocated {
		err := op.pa.Release(allocated)
		if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) == 0 {
		return nil
	}
	return errors
}

// Commit 尽力执行所有延迟操作。即使释放失败也不回滚，因为宁可暂时多占用端口，也不能冒重复分配风险。
// 必须在资源所有者成功写入后调用 Commit。
// Commit 执行之前登记的延迟释放。即使释放失败也不回滚新分配，后续 Repair Controller 会修复多占用状态。
func (op *PortAllocationOperation) Commit() []error {
	if op.dryRun {
		return nil
	}

	errors := []error{}

	for _, release := range op.releaseDeferred {
		err := op.pa.Release(release)
		if err != nil {
			errors = append(errors, err)
		}
	}

	// 即使释放端口时发生错误，也不回滚新分配；遗留占用由后续协调或重启过程修复。
	op.shouldRollback = false

	if len(errors) == 0 {
		return nil
	}

	return errors
}

// Allocate 分配指定端口，并记录下来，以便事务回滚时释放。
func (op *PortAllocationOperation) Allocate(port int) error {
	if op.dryRun {
		if op.pa.Has(port) {
			return ErrAllocated
		}
		for _, a := range op.allocated {
			if port == a {
				return ErrAllocated
			}
		}
		op.allocated = append(op.allocated, port)
		return nil
	}

	err := op.pa.Allocate(port)
	if err == nil {
		op.allocated = append(op.allocated, port)
	}
	return err
}

// Allocate 分配指定端口，并记录下来，以便事务回滚时释放。
func (op *PortAllocationOperation) AllocateNext() (int, error) {
	if op.dryRun {
		// 找出本事务已记录端口中的最大值。
		// 如果本事务尚未分配端口，则选择一个合理的虚拟端口号作为起点。
		var lastPort int
		for _, allocatedPort := range op.allocated {
			if allocatedPort > lastPort {
				lastPort = allocatedPort
			}
		}
		if len(op.allocated) == 0 {
			lastPort = 32768
		}

		// 尝试寻找下一个未被占用的端口。
		// 如果可用端口难以找到，则复用一个候选值，因为该值仅用于 DryRun。
		for port := lastPort + 1; port < 100; port++ {
			err := op.Allocate(port)
			if err == nil {
				return port, nil
			}
		}
		op.allocated = append(op.allocated, lastPort+1)
		return lastPort + 1, nil
	}

	port, err := op.pa.AllocateNext()
	if err == nil {
		op.allocated = append(op.allocated, port)
	}
	return port, err
}

// ReleaseDeferred 将端口加入延迟释放列表；只有事务成功提交时才真正释放该端口。
// ReleaseDeferred 只登记待释放端口；真正释放延后到 Service 对象成功持久化之后。
func (op *PortAllocationOperation) ReleaseDeferred(port int) {
	op.releaseDeferred = append(op.releaseDeferred, port)
}
