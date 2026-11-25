/*
Copyright The Kubernetes Authors.

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

package ktesting_test

import (
	"testing"
	"testing/synctest"

	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestSyncTestInit 与主包中的对应测试一致。它作为该包中唯一的测试存在，
// 因为 signal.Notify 在进程中首次调用时，在 synctest bubble 内会失败，
// 而我们希望强制构造这种情形。
func TestSyncTestInit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 即使 Deadline 在 synctest bubble 内会 panic，这里也必须能正常工作。
		// 这样我们就没有截止时间。
		tCtx := ktesting.Init(t)
		deadline, ok := tCtx.Deadline()
		if ok {
			tCtx.Errorf("Expected no deadline, got %s", deadline)
		}
		if !tCtx.IsSyncTest() {
			tCtx.Errorf("Expected to run as synctest")
		}
	})
}
