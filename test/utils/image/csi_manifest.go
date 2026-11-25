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
	"bytes"
	"fmt"
	"io/fs"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/util/yaml"
	e2etestingmanifests "k8s.io/kubernetes/test/e2e/testing-manifests"
)

// 所有镜像标签的格式均为 registry.k8s.io/sig-storage/hostpathplugin:v1.7.3。
var imageRE = regexp.MustCompile(`^(.*)/([^/:]*):(.*)$`)

// appendCSIImageConfigs 从 test/e2e/testing-manifests/storage-csi 下的 YAML 文件中
// 提取镜像仓库、名称和版本，
// 并为它们创建新的配置项。
func appendCSIImageConfigs(configs map[ImageID]Config) {
	embeddedFS := e2etestingmanifests.GetE2ETestingManifestsFS().EmbeddedFS

	// 我们添加镜像时，其 ImageID 编号从现有最大编号之后开始。
	index := ImageID(0)
	for i := range configs {
		if i > index {
			index = i
		}
	}

	err := fs.WalkDir(embeddedFS, "storage-csi", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		data, err := embeddedFS.ReadFile(path)
		if err != nil {
			return err
		}

		// 在处理单个条目之前，先按 "---" 分隔符拆分。
		// 仅适用于 .yaml。
		//
		// 我们需要自己拆分，因为我们需要访问每个原始数据块，
		// 以供 runtime.DecodeInto 使用。kubectl 对此
		// 有自己的基础设施，但那是大量代码
		// 并且有
		// 许多依赖。
		items := bytes.Split(data, []byte("\n---"))
		for i, item := range items {
			// 我们不关心实际类型是什么。只需
			// 反序列化为通用 map，然后查找镜像即可。
			var object interface{}
			if err := yaml.Unmarshal(item, &object); err != nil {
				return fmt.Errorf("decode item #%d in %s: %v",
					i, path, err)
			}

			// 会对所有镜像字符串调用。
			visit := func(value string) {
				parts := imageRE.FindStringSubmatch(value)
				if parts == nil {
					return
				}
				config := Config{parts[1], parts[2], parts[3]}
				for _, otherConfig := range configs {
					if otherConfig == config {
						return
					}
				}
				index++
				configs[index] = config
			}

			// 这些路径可匹配普通 Pod 以及更复杂的类型，
			// 如 Deployment。
			findStrings(object, visit, "spec", "containers", "image")
			findStrings(object, visit, "spec", "template", "spec", "containers", "image")

		}
		return nil

	})
	if err != nil {
		panic(err)
	}
}

// findStrings 沿指定路径递归深入到对象中。路径
// 元素即命名字段。如果某个字段引用的是列表，则会依次
// 遍历列表中的每个元素。
//
// 从概念上讲，这类似于 JSON 路径。
func findStrings(object interface{}, visit func(value string), path ...string) {
	if len(path) == 0 {
		// 找到了。不过它可能是也可能不是字符串。
		if object, ok := object.(string); ok {
			visit(object)
		}
		return
	}

	switch object := object.(type) {
	case []interface{}:
		// 如果处于列表中，则检查每个条目。
		for _, child := range object {
			findStrings(child, visit, path...)
		}
	case map[string]interface{}:
		// 尽可能沿路径深入
		if child, ok := object[path[0]]; ok {
			findStrings(child, visit, path[1:]...)
		}
	}
}
