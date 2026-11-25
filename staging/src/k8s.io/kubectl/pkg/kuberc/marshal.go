/*
Copyright 2024 The Kubernetes Authors.

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

package kuberc

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/config"
	"k8s.io/kubectl/pkg/config/scheme"
	"k8s.io/kubectl/pkg/config/v1beta1"
)

// decodePreference 遍历 kuberc 文件中的各个 YAML 文档，查找第一个受支持的 Preference 版本。
// 找到后，它会返回内部对象，以及遍历过程中累积的错误。
func decodePreference(kubercFile string) (*config.Preference, error) {
	kubercBytes, err := os.ReadFile(kubercFile)
	if err != nil {
		return nil, err
	}

	attemptedItems := 0
	reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewBuffer(kubercBytes)))
	for {
		doc, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			// 没有更多条目，到达文件末尾时预期如此。
			break
		}
		if readErr != nil {
			// 其他错误是致命错误。
			return nil, readErr
		}
		if len(bytes.TrimSpace(doc)) == 0 {
			// 空条目，忽略。
			continue
		}
		// 记录我们已尝试过
		attemptedItems++
		pref, gvk, strictDecodeErr := scheme.StrictCodecs.UniversalDecoder().Decode(doc, nil, nil)
		if strictDecodeErr != nil {
			var lenientDecodeErr error
			pref, gvk, lenientDecodeErr = scheme.LenientCodecs.UniversalDecoder().Decode(doc, nil, nil)
			if lenientDecodeErr != nil {
				// 严格解码和宽松解码都失败了。
				// 以详细日志输出包含该条目最多信息的错误，然后继续。
				klog.V(5).Infof("kuberc: strict decoding error for entry %d in %s: %v", attemptedItems, kubercFile, strictDecodeErr)
				continue
			}
		}

		// 检查期望的 GVK，如果不匹配，则输出详细日志并继续。
		expectedGK := schema.GroupKind{
			Group: config.SchemeGroupVersion.Group,
			Kind:  "Preference",
		}
		if gvk.GroupKind() != expectedGK {
			klog.V(5).Infof("kuberc: unexpected GroupVersionKind for entry %d in %s: %v", attemptedItems, kubercFile, gvk)
			continue
		}

		// 检查期望的 Go 类型，如果不匹配，则输出详细日志并继续。
		preferences, ok := pref.(*config.Preference)
		if !ok {
			klog.V(5).Infof("kuberc: unexpected object type %T for entry %d in %s", pref, attemptedItems, kubercFile)
			continue
		}

		// 我们得到了可用的首选项，可以返回。
		klog.V(5).Infof("kuberc: using entry %d (%s) in %s", attemptedItems, gvk.GroupVersion(), kubercFile)
		return preferences, strictDecodeErr
	}

	if attemptedItems > 0 {
		return nil, fmt.Errorf("no valid preferences found in %s, use --v=5 to see details", kubercFile)
	}

	// 空文档
	klog.V(5).Infof("kuberc: no preferences found in %s", kubercFile)
	return nil, nil
}

// LoadPreference 加载 kuberc 文件，并返回 v1beta1.Preference 对象。
func LoadPreference(kubercFile string) (*v1beta1.Preference, error) {
	internal, err := decodePreference(kubercFile)
	if err != nil {
		return nil, err
	}
	prefs, err := scheme.Scheme.ConvertToVersion(internal, v1beta1.SchemeGroupVersion)
	if err != nil {
		return nil, fmt.Errorf("error converting Preferences to v1beta1.Preferences: %w", err)
	}
	return prefs.(*v1beta1.Preference), nil
}

// SavePreference 将首选项保存到 kuberc 文件中。
func SavePreference(pref *v1beta1.Preference, kubercFile string, out io.Writer) error {
	dir := filepath.Dir(kubercFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}
	file, err := os.OpenFile(kubercFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to write kuberc file: %w", err)
	}

	const mediaType = runtime.ContentTypeYAML
	yamlInfo, _ := runtime.SerializerInfoForMediaType(scheme.StrictCodecs.SupportedMediaTypes(), mediaType)
	encoder := scheme.StrictCodecs.EncoderForVersion(yamlInfo.Serializer, v1beta1.SchemeGroupVersion)
	if err := encoder.Encode(pref, file); err != nil {
		return fmt.Errorf("failed to marshal preferences: %w", err)
	}

	fmt.Fprintf(out, "Updated %s\n", kubercFile) // nolint:errcheck
	return nil
}

// CreateDefaultPreference 返回带有一些默认配置的
// Preference 对象。
func CreateDefaultPreference() *v1beta1.Preference {
	return &v1beta1.Preference{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "kubectl.config.k8s.io/v1beta1",
			Kind:       "Preference",
		},
		Defaults: []v1beta1.CommandDefaults{
			{
				Command: "apply",
				Options: []v1beta1.CommandOptionDefault{
					{
						Name:    "server-side",
						Default: "true",
					},
				},
			},
			{
				Command: "delete",
				Options: []v1beta1.CommandOptionDefault{
					{
						Name:    "interactive",
						Default: "true",
					},
				},
			},
		},
	}
}
