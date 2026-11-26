/*
Copyright 2021 The Kubernetes Authors.

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

package common

import (
	// 确保 ginkgo 扫描到这些包中的 e2e 测试
	_ "k8s.io/kubernetes/test/e2e/common/network"
	_ "k8s.io/kubernetes/test/e2e/common/node"
	_ "k8s.io/kubernetes/test/e2e/common/storage"
)
