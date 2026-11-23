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

package metadata

import "path/filepath"

// MetadataFileName returns the metadata file name for the given driver.
//
// MetadataFileName 返回给定驱动的元数据文件名，即在驱动名称后直接拼接固定后缀 MetadataFileSuffix。
func MetadataFileName(driverName string) string {
	return driverName + MetadataFileSuffix
}

// ResourceClaimFilePath returns the in-container path for a directly
// referenced ResourceClaim's metadata file.
//
// ResourceClaimFilePath 返回直接引用的 ResourceClaim 的元数据文件在容器内的路径。
// 路径由容器根目录、ResourceClaimsSubDir、claim 名称、request 名称和文件名依次拼接而成。
func ResourceClaimFilePath(driverName, claimName, requestName string) string {
	return filepath.Join(ContainerDir, ResourceClaimsSubDir, claimName, requestName, MetadataFileName(driverName))
}

// ResourceClaimTemplateFilePath returns the in-container path for a
// template-generated claim's metadata file.
//
// ResourceClaimTemplateFilePath 返回模板生成的 claim 的元数据文件在容器内的路径。
// 与 ResourceClaimFilePath 类似，但使用 ResourceClaimTemplatesSubDir 子目录，
// 并且以 Pod 本地的 claim 名称（podClaimName）而非真实 claim 名称作为目录键。
func ResourceClaimTemplateFilePath(driverName, podClaimName, requestName string) string {
	return filepath.Join(ContainerDir, ResourceClaimTemplatesSubDir, podClaimName, requestName, MetadataFileName(driverName))
}
