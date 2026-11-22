/*
Copyright 2019 The Kubernetes Authors.

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

package apiserver

import (
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var swaggerMetadataDescriptions = metav1.ObjectMeta{}.SwaggerDoc()

// getColumnsForVersion 返回指定版本的列，若版本不存在则返回 nil。
// 注意：逻辑默认化后的列并不指向原始 CRD 对象。
// 不能通过逻辑默认化后的列修改原始 CRD 的列，应改为遍历原始 CRD 对象。
func getColumnsForVersion(crd *apiextensionsv1.CustomResourceDefinition, version string) ([]apiextensionsv1.CustomResourceColumnDefinition, error) {
	for _, v := range crd.Spec.Versions {
		if version == v.Name {
			return serveDefaultColumnsIfEmpty(v.AdditionalPrinterColumns), nil
		}
	}
	return nil, fmt.Errorf("version %s not found in apiextensionsv1.CustomResourceDefinition: %v", version, crd.Name)
}

// getScaleColumnsForVersion 返回期望副本数和实际副本数两列。
func getScaleColumnsForVersion(crd *apiextensionsv1.CustomResourceDefinition, version string) ([]apiextensionsv1.CustomResourceColumnDefinition, error) {
	for _, v := range crd.Spec.Versions {
		if version != v.Name {
			continue
		}
		var cols []apiextensionsv1.CustomResourceColumnDefinition
		if v.Subresources != nil && v.Subresources.Scale != nil {
			if v.Subresources.Scale.SpecReplicasPath != "" {
				cols = append(cols, apiextensionsv1.CustomResourceColumnDefinition{
					Name:        "Desired",
					Type:        "integer",
					Description: "Number of desired replicas",
					JSONPath:    ".spec.replicas",
				})
			}
			if v.Subresources.Scale.StatusReplicasPath != "" {
				cols = append(cols, apiextensionsv1.CustomResourceColumnDefinition{
					Name:        "Available",
					Type:        "integer",
					Description: "Number of actual replicas",
					JSONPath:    ".status.replicas",
				})
			}
		}
		cols = append(cols, apiextensionsv1.CustomResourceColumnDefinition{
			Name:        "Age",
			Type:        "date",
			Description: swaggerMetadataDescriptions["creationTimestamp"],
			JSONPath:    ".metadata.creationTimestamp",
		})
		return cols, nil
	}
	return nil, fmt.Errorf("version %s not found in apiextensionsv1.CustomResourceDefinition: %v", version, crd.Name)
}

// serveDefaultColumnsIfEmpty 在输入列为空时对列做逻辑默认化。
// 注意：这样得到的列并不指向原始 CRD 对象。
// 不能通过逻辑默认化后的列修改原始 CRD 的列，应改为遍历原始 CRD 对象。
func serveDefaultColumnsIfEmpty(columns []apiextensionsv1.CustomResourceColumnDefinition) []apiextensionsv1.CustomResourceColumnDefinition {
	if len(columns) > 0 {
		return columns
	}
	return []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "Age", Type: "date", Description: swaggerMetadataDescriptions["creationTimestamp"], JSONPath: ".metadata.creationTimestamp"},
	}
}
