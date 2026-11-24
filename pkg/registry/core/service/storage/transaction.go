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

package storage

// transaction 表示一个子事务；当外层事务成功或失败时，需要对该子事务执行最终处理。
type transaction interface {
	// Commit 通知子事务正式完成尚未提交的变更。该方法不能返回错误，因此实现必须在内部处理异常。
	Commit()

	// Revert 通知子事务放弃或撤销尚未提交的变更。该方法不能返回错误，因此实现必须在内部处理异常。
	Revert()
}

// metaTransaction 将多个子事务组合为一个事务，并统一提交或撤销。
type metaTransaction []transaction

// Commit 将组合事务逐个提交；具体子事务决定何时真正释放延迟回收的资源。
func (mt metaTransaction) Commit() {
	for _, t := range mt {
		t.Commit()
	}
}

// Revert 将组合事务逐个撤销，通常释放在持久化失败前新预留的资源。
func (mt metaTransaction) Revert() {
	for _, t := range mt {
		t.Revert()
	}
}

// callbackTransaction 通过回调函数实现事务提交和撤销行为。
type callbackTransaction struct {
	commit func()
	revert func()
}

// Commit 执行提交回调；nil 回调表示该事务在该阶段没有额外工作。
func (cb callbackTransaction) Commit() {
	if cb.commit != nil {
		cb.commit()
	}
}

func (cb callbackTransaction) Revert() {
	if cb.revert != nil {
		cb.revert()
	}
}
