/*
Copyright 2022 The Kubernetes Authors.

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

package image

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"
)

func TestCSIImageConfigs(t *testing.T) {
	configs := map[ImageID]Config{}
	appendCSIImageConfigs(configs)

	// 我们期望这些镜像中的每一个至少有一个条目。当不同的 YAML
	// 使用不同版本时，同一个镜像可能会有
	// 多个条目。
	//
	// 此处不检查确切的版本，因为那会重新引入
	// 需要不断更新预期版本的问题。
	// 镜像集合不应该有太大变化。
	expectedImages := []string{
		"csi-attacher",
		"csi-external-health-monitor-controller",
		"csi-node-driver-registrar",
		"csi-provisioner",
		"csi-resizer",
		"csi-snapshotter",
		"hostpathplugin",
		"livenessprobe",

		// 来自 GCP 部署。
		"gcp-compute-persistent-disk-csi-driver",

		// 用于一些 hostpath 测试。
		"busybox",

		// 用于 AnyVolumeDataSource 特性测试。
		"volume-data-source-validator",
		"hello-populator",

		// 用于 SnapshotMetadata 特性测试。
		"csi-snapshot-metadata",
	}
	actualImages := sets.NewString()
	for _, config := range configs {
		assert.NotEmpty(t, config.registry, "registry")
		assert.NotEmpty(t, config.name, "name")
		assert.NotEmpty(t, config.version, "version")
		actualImages.Insert(config.name)
	}
	assert.ElementsMatchf(t, expectedImages, actualImages.UnsortedList(), "found these images: %+v", configs)
}
