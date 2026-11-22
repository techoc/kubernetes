/*
Copyright 2014 The Kubernetes Authors.

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

package v1

import (
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd/api"
)

// Convert_Slice_v1_NamedCluster_To_Map_string_To_Pointer_api_Cluster 把 v1 的有序 cluster 列表
// 转换为内部以名字为键的 map。kubeconfig v1 用列表便于人工阅读与合并，
// 内部表示用 map 便于 O(1) 查找；重名会导致歧义，因此显式报错。
func Convert_Slice_v1_NamedCluster_To_Map_string_To_Pointer_api_Cluster(in *[]NamedCluster, out *map[string]*api.Cluster, s conversion.Scope) error {
	for _, curr := range *in {
		newCluster := api.NewCluster()
		if err := Convert_v1_Cluster_To_api_Cluster(&curr.Cluster, newCluster, s); err != nil {
			return err
		}
		if *out == nil {
			*out = make(map[string]*api.Cluster)
		}
		if (*out)[curr.Name] == nil {
			(*out)[curr.Name] = newCluster
		} else {
			return fmt.Errorf("error converting *[]NamedCluster into *map[string]*api.Cluster: duplicate name \"%v\" in list: %v", curr.Name, *in)
		}
	}
	return nil
}

// Convert_Map_string_To_Pointer_api_Cluster_To_Slice_v1_NamedCluster 执行反向转换。
// map 本身无序，这里对 key 排序以产出稳定、可 diff 的 kubeconfig 文件。
func Convert_Map_string_To_Pointer_api_Cluster_To_Slice_v1_NamedCluster(in *map[string]*api.Cluster, out *[]NamedCluster, s conversion.Scope) error {
	allKeys := make([]string, 0, len(*in))
	for key := range *in {
		allKeys = append(allKeys, key)
	}
	sort.Strings(allKeys)

	for _, key := range allKeys {
		newCluster := (*in)[key]
		oldCluster := Cluster{}
		if err := Convert_api_Cluster_To_v1_Cluster(newCluster, &oldCluster, s); err != nil {
			return err
		}
		namedCluster := NamedCluster{key, oldCluster}
		*out = append(*out, namedCluster)
	}
	return nil
}

// Convert_Slice_v1_NamedAuthInfo_To_Map_string_To_Pointer_api_AuthInfo 把 v1 的 authinfo 列表
// 转换为内部 map，并拒绝重名。
func Convert_Slice_v1_NamedAuthInfo_To_Map_string_To_Pointer_api_AuthInfo(in *[]NamedAuthInfo, out *map[string]*api.AuthInfo, s conversion.Scope) error {
	for _, curr := range *in {
		newAuthInfo := api.NewAuthInfo()
		if err := Convert_v1_AuthInfo_To_api_AuthInfo(&curr.AuthInfo, newAuthInfo, s); err != nil {
			return err
		}
		if *out == nil {
			*out = make(map[string]*api.AuthInfo)
		}
		if (*out)[curr.Name] == nil {
			(*out)[curr.Name] = newAuthInfo
		} else {
			return fmt.Errorf("error converting *[]NamedAuthInfo into *map[string]*api.AuthInfo: duplicate name \"%v\" in list: %v", curr.Name, *in)
		}
	}
	return nil
}

// Convert_Map_string_To_Pointer_api_AuthInfo_To_Slice_v1_NamedAuthInfo 按 key 排序后输出，
// 以保证生成的 kubeconfig 内容稳定。
func Convert_Map_string_To_Pointer_api_AuthInfo_To_Slice_v1_NamedAuthInfo(in *map[string]*api.AuthInfo, out *[]NamedAuthInfo, s conversion.Scope) error {
	allKeys := make([]string, 0, len(*in))
	for key := range *in {
		allKeys = append(allKeys, key)
	}
	sort.Strings(allKeys)

	for _, key := range allKeys {
		newAuthInfo := (*in)[key]
		oldAuthInfo := AuthInfo{}
		if err := Convert_api_AuthInfo_To_v1_AuthInfo(newAuthInfo, &oldAuthInfo, s); err != nil {
			return err
		}
		namedAuthInfo := NamedAuthInfo{key, oldAuthInfo}
		*out = append(*out, namedAuthInfo)
	}
	return nil
}

// Convert_Slice_v1_NamedContext_To_Map_string_To_Pointer_api_Context 把 v1 的 context 列表
// 转换为内部 map，并拒绝重名。
func Convert_Slice_v1_NamedContext_To_Map_string_To_Pointer_api_Context(in *[]NamedContext, out *map[string]*api.Context, s conversion.Scope) error {
	for _, curr := range *in {
		newContext := api.NewContext()
		if err := Convert_v1_Context_To_api_Context(&curr.Context, newContext, s); err != nil {
			return err
		}
		if *out == nil {
			*out = make(map[string]*api.Context)
		}
		if (*out)[curr.Name] == nil {
			(*out)[curr.Name] = newContext
		} else {
			return fmt.Errorf("error converting *[]NamedContext into *map[string]*api.Context: duplicate name \"%v\" in list: %v", curr.Name, *in)
		}
	}
	return nil
}

// Convert_Map_string_To_Pointer_api_Context_To_Slice_v1_NamedContext 按 key 排序后输出。
func Convert_Map_string_To_Pointer_api_Context_To_Slice_v1_NamedContext(in *map[string]*api.Context, out *[]NamedContext, s conversion.Scope) error {
	allKeys := make([]string, 0, len(*in))
	for key := range *in {
		allKeys = append(allKeys, key)
	}
	sort.Strings(allKeys)

	for _, key := range allKeys {
		newContext := (*in)[key]
		oldContext := Context{}
		if err := Convert_api_Context_To_v1_Context(newContext, &oldContext, s); err != nil {
			return err
		}
		namedContext := NamedContext{key, oldContext}
		*out = append(*out, namedContext)
	}
	return nil
}

// Convert_Slice_v1_NamedExtension_To_Map_string_To_runtime_Object 把 v1 的 extension 列表
// 转换为内部 map。extension 的值是任意 runtime.Object，通过 RawExtension 转换。
func Convert_Slice_v1_NamedExtension_To_Map_string_To_runtime_Object(in *[]NamedExtension, out *map[string]runtime.Object, s conversion.Scope) error {
	for _, curr := range *in {
		var newExtension runtime.Object
		if err := runtime.Convert_runtime_RawExtension_To_runtime_Object(&curr.Extension, &newExtension, s); err != nil {
			return err
		}
		if *out == nil {
			*out = make(map[string]runtime.Object)
		}
		if (*out)[curr.Name] == nil {
			(*out)[curr.Name] = newExtension
		} else {
			return fmt.Errorf("error converting *[]NamedExtension into *map[string]runtime.Object: duplicate name \"%v\" in list: %v", curr.Name, *in)
		}
	}
	return nil
}

// Convert_Map_string_To_runtime_Object_To_Slice_v1_NamedExtension 按 key 排序后输出，
// 把任意对象编码回 RawExtension。
func Convert_Map_string_To_runtime_Object_To_Slice_v1_NamedExtension(in *map[string]runtime.Object, out *[]NamedExtension, s conversion.Scope) error {
	allKeys := make([]string, 0, len(*in))
	for key := range *in {
		allKeys = append(allKeys, key)
	}
	sort.Strings(allKeys)

	for _, key := range allKeys {
		newExtension := (*in)[key]
		oldExtension := runtime.RawExtension{}
		if err := runtime.Convert_runtime_Object_To_runtime_RawExtension(&newExtension, &oldExtension, s); err != nil {
			return err
		}
		namedExtension := NamedExtension{key, oldExtension}
		*out = append(*out, namedExtension)
	}
	return nil
}
