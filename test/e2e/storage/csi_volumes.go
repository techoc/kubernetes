/*
Copyright 2018 The Kubernetes Authors.

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

import (
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/storage/drivers"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	"k8s.io/kubernetes/test/e2e/storage/testsuites"
	"k8s.io/kubernetes/test/e2e/storage/utils"
)

// 在下面循环中要执行的 testDrivers 列表
var csiTestDrivers = []func() storageframework.TestDriver{
	drivers.InitHostPathCSIDriver,
	drivers.InitGcePDCSIDriver,
	// 不使用 mock driver（drivers.InitMockCSIDriver）运行测试，它不提供持久存储。
}

// 这会为 csi volume 执行 testSuites。
var _ = utils.SIGDescribe("CSI Volumes", func() {
	for _, initDriver := range csiTestDrivers {
		curDriver := initDriver()

		args := storageframework.GetDriverNameWithFeatureTags(curDriver)
		args = append(args, func() {
			storageframework.DefineTestSuites(curDriver, testsuites.CSISuites)
		})
		framework.Context(args...)
	}
})
