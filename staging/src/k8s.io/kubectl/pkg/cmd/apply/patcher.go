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

package apply

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/jonboulle/clockwork"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/jsonmergepatch"
	"k8s.io/apimachinery/pkg/util/mergepatch"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/openapi3"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/validation/spec"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"
	"k8s.io/kubectl/pkg/util/openapi"
)

const (
	// maxPatchRetry 是 patch 操作在返回失败之前因冲突而重试的最大次数
	maxPatchRetry = 5
	// 退避之前可以重试的次数
	triesBeforeBackOff = 1
	// groupVersionKindExtensionKey 是用于从对象定义的
	// "extensions" map 中查找 GroupVersionKind 值的
	// 键。
	groupVersionKindExtensionKey = "x-kubernetes-group-version-kind"
)

// patchRetryBackOffPeriod 是 apply patch 出错时的退避时长。
var patchRetryBackOffPeriod = 1 * time.Second

var createPatchErrFormat = "creating patch with:\noriginal:\n%s\nmodified:\n%s\ncurrent:\n%s\nfor: %w"

// Patcher 定义用于 patch OpenAPI 对象的选项。
type Patcher struct {
	Mapping *meta.RESTMapping
	Helper  *resource.Helper

	Overwrite bool
	BackOff   clockwork.Clock

	Force             bool
	CascadingStrategy metav1.DeletionPropagation
	Timeout           time.Duration
	GracePeriod       int

	// 如果设置，则针对特定的 resourceVersion 强制应用 patch
	ResourceVersion *string

	// patch 因冲突失败时的重试次数
	Retries int

	OpenAPIGetter openapi.OpenAPIResourcesGetter
	OpenAPIV3Root openapi3.Root
}

func newPatcher(o *ApplyOptions, info *resource.Info, helper *resource.Helper) (*Patcher, error) {
	var openAPIGetter openapi.OpenAPIResourcesGetter
	var openAPIV3Root openapi3.Root

	if o.OpenAPIPatch {
		openAPIGetter = o.OpenAPIGetter
		openAPIV3Root = o.OpenAPIV3Root
	}

	return &Patcher{
		Mapping:           info.Mapping,
		Helper:            helper,
		Overwrite:         o.Overwrite,
		BackOff:           clockwork.NewRealClock(),
		Force:             o.DeleteOptions.ForceDeletion,
		CascadingStrategy: o.DeleteOptions.CascadingStrategy,
		Timeout:           o.DeleteOptions.Timeout,
		GracePeriod:       o.DeleteOptions.GracePeriod,
		OpenAPIGetter:     openAPIGetter,
		OpenAPIV3Root:     openAPIV3Root,
		Retries:           maxPatchRetry,
	}, nil
}

func (p *Patcher) delete(namespace, name string) error {
	options := asDeleteOptions(p.CascadingStrategy, p.GracePeriod)
	_, err := p.Helper.DeleteWithOptions(namespace, name, &options)
	return err
}

func (p *Patcher) patchSimple(obj runtime.Object, modified []byte, namespace, name string, errOut io.Writer, localApply bool) ([]byte, runtime.Object, error) {
	// 序列化来自服务端的对象当前配置。
	current, err := runtime.Encode(unstructured.UnstructuredJSONScheme, obj)
	if err != nil {
		return nil, nil, fmt.Errorf("serializing current configuration from:\n%v\nfor: %w", obj, err)
	}

	// 从注解中获取对象的原始配置。
	original, err := util.GetOriginalConfiguration(obj)
	if err != nil {
		return nil, nil, fmt.Errorf("retrieving original configuration from:\n%v\nfor: %w", obj, err)
	}

	var patchType types.PatchType
	var patch []byte

	if p.OpenAPIV3Root != nil {
		gvkSupported, err := p.gvkSupportsPatchOpenAPIV3(p.Mapping.GroupVersionKind)
		if err != nil {
			// 实际上这条错误日志并非必需（V2 中没有），
			// 但如果用户遇到问题，它有助于我们调试
			// OpenAPI V3 中不存在 V2 内容的情形。
			klog.V(5).Infof("warning: OpenAPI V3 path does not exist - group: %s, version %s, kind %s\n",
				p.Mapping.GroupVersionKind.Group, p.Mapping.GroupVersionKind.Version, p.Mapping.GroupVersionKind.Kind)
		} else if gvkSupported {
			patch, err = p.buildStrategicMergePatchFromOpenAPIV3(original, modified, current)
			if err != nil {
				// 出现问题时回退到 OpenAPI V2
				// 将来应移除此回退逻辑，
				// 但在首个版本中，回退到 OpenAPI V2
				// 同时记录错误可能更有帮助，
				// 以便查看是否收到错误报告。
				fmt.Fprintf(errOut, "warning: error calculating patch from openapi v3 spec: %v\n", err)
			} else {
				patchType = types.StrategicMergePatchType
			}
		} else {
			klog.V(5).Infof("warning: OpenAPI V3 path does not support strategic merge patch - group: %s, version %s, kind %s\n",
				p.Mapping.GroupVersionKind.Group, p.Mapping.GroupVersionKind.Version, p.Mapping.GroupVersionKind.Kind)
		}
	}

	if patch == nil && p.OpenAPIGetter != nil {
		if openAPISchema, err := p.OpenAPIGetter.OpenAPISchema(); err == nil && openAPISchema != nil {
			// 如果使用 openapischema，我们会尝试从 Open API 获取此 GVK 所需的 patch 类型。
			// 如果失败或找不到任何 patch 类型，则回退到内置的 patch 类型判定。
			if patchType, err = p.getPatchTypeFromOpenAPI(openAPISchema, p.Mapping.GroupVersionKind); err == nil && patchType == types.StrategicMergePatchType {
				patch, err = p.buildStrategicMergeFromOpenAPI(openAPISchema, original, modified, current)
				if err != nil {
					// 向用户警告该问题，并继续使用内置类型执行 strategic merge patch。
					fmt.Fprintf(errOut, "warning: error calculating patch from openapi spec: %v\n", err)
				}
			}
		}
	}

	if patch == nil {
		versionedObj, err := scheme.Scheme.New(p.Mapping.GroupVersionKind)
		if err == nil {
			patchType = types.StrategicMergePatchType
			patch, err = p.buildStrategicMergeFromBuiltins(versionedObj, original, modified, current)
			if err != nil {
				return nil, nil, fmt.Errorf(createPatchErrFormat, original, modified, current, err)
			}
		} else {
			if !runtime.IsNotRegisteredError(err) {
				return nil, nil, fmt.Errorf("getting instance of versioned object for %v: %w", p.Mapping.GroupVersionKind, err)
			}

			patchType = types.MergePatchType
			patch, err = p.buildMergePatch(original, modified, current)
			if err != nil {
				return nil, nil, fmt.Errorf(createPatchErrFormat, original, modified, current, err)
			}
		}
	}

	if string(patch) == "{}" {
		return patch, obj, nil
	}

	if localApply {
		var patchedBytes []byte
		if patchType == types.StrategicMergePatchType {
			versionedObj, _ := scheme.Scheme.New(p.Mapping.GroupVersionKind)
			patchedBytes, err = strategicpatch.StrategicMergePatch(current, patch, versionedObj)
		} else {
			patchedBytes, err = jsonpatch.MergePatch(current, patch)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("applying patch locally: %w", err)
		}
		patchedObj, _, err := unstructured.UnstructuredJSONScheme.Decode(patchedBytes, nil, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("decoding locally patched object: %w", err)
		}
		return patch, patchedObj, nil
	}

	if p.ResourceVersion != nil {
		patch, err = addResourceVersion(patch, *p.ResourceVersion)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to insert resourceVersion in patch: %w", err)
		}
	}

	patchedObj, err := p.Helper.Patch(namespace, name, patchType, patch, nil)
	return patch, patchedObj, err
}

// buildMergePatch 根据用于自定义资源定义的 JSONMergePatch
// 构建 patch。
func (p *Patcher) buildMergePatch(original, modified, current []byte) ([]byte, error) {
	preconditions := []mergepatch.PreconditionFunc{mergepatch.RequireKeyUnchanged("apiVersion"),
		mergepatch.RequireKeyUnchanged("kind"), mergepatch.RequireMetadataKeyUnchanged("name")}
	patch, err := jsonmergepatch.CreateThreeWayJSONMergePatch(original, modified, current, preconditions...)
	if err != nil {
		if mergepatch.IsPreconditionFailed(err) {
			return nil, fmt.Errorf("%s", "At least one of apiVersion, kind and name was changed")
		}
		return nil, err
	}

	return patch, nil
}

// gvkSupportsPatchOpenAPIV3 检查特定 GVK 是否支持 patch 操作。
// 如果无法下载 OpenAPI V3，则返回错误。
func (p *Patcher) gvkSupportsPatchOpenAPIV3(gvk schema.GroupVersionKind) (bool, error) {
	gvSpec, err := p.OpenAPIV3Root.GVSpec(schema.GroupVersion{
		Group:   p.Mapping.GroupVersionKind.Group,
		Version: p.Mapping.GroupVersionKind.Version,
	})
	if err != nil {
		return false, err
	}
	if gvSpec == nil || gvSpec.Paths == nil || gvSpec.Paths.Paths == nil {
		return false, fmt.Errorf("gvk group: %s, version: %s, kind: %s does not exist for OpenAPI V3", gvk.Group, gvk.Version, gvk.Kind)
	}
	for _, path := range gvSpec.Paths.Paths {
		if path.Patch != nil {
			if gvkMatchesSingle(p.Mapping.GroupVersionKind, path.Patch.Extensions) {
				if path.Patch.RequestBody == nil || path.Patch.RequestBody.Content == nil {
					// GVK 存在但不支持 requestBody，表明 OpenAPI 格式有误。
					return false, nil
				}
				if _, ok := path.Patch.RequestBody.Content["application/strategic-merge-patch+json"]; ok {
					return true, nil
				}
				// GVK 存在但不支持 strategic-merge-patch，可能是 CRD 或聚合资源。
				return false, nil
			}
		}
	}
	return false, nil
}

func gvkMatchesArray(targetGVK schema.GroupVersionKind, ext spec.Extensions) bool {
	var gvkList []map[string]string
	err := ext.GetObject(groupVersionKindExtensionKey, &gvkList)
	if err != nil {
		return false
	}
	for _, gvkMap := range gvkList {
		if gvkMap["group"] == targetGVK.Group &&
			gvkMap["version"] == targetGVK.Version &&
			gvkMap["kind"] == targetGVK.Kind {
			return true
		}
	}
	return false
}

func gvkMatchesSingle(targetGVK schema.GroupVersionKind, ext spec.Extensions) bool {
	var gvkMap map[string]string
	err := ext.GetObject(groupVersionKindExtensionKey, &gvkMap)
	if err != nil {
		return false
	}
	return gvkMap["group"] == targetGVK.Group &&
		gvkMap["version"] == targetGVK.Version &&
		gvkMap["kind"] == targetGVK.Kind
}

func (p *Patcher) buildStrategicMergePatchFromOpenAPIV3(original, modified, current []byte) ([]byte, error) {
	gvSpec, err := p.OpenAPIV3Root.GVSpec(schema.GroupVersion{
		Group:   p.Mapping.GroupVersionKind.Group,
		Version: p.Mapping.GroupVersionKind.Version,
	})
	if err != nil {
		return nil, err
	}
	if gvSpec == nil || gvSpec.Components == nil {
		return nil, fmt.Errorf("OpenAPI V3 Components is nil")
	}
	for _, c := range gvSpec.Components.Schemas {
		if !gvkMatchesArray(p.Mapping.GroupVersionKind, c.Extensions) {
			continue
		}
		lookupPatchMeta := strategicpatch.PatchMetaFromOpenAPIV3{Schema: c, SchemaList: gvSpec.Components.Schemas}
		if openapiv3Patch, err := strategicpatch.CreateThreeWayMergePatch(original, modified, current, lookupPatchMeta, p.Overwrite); err != nil {
			return nil, err
		} else {
			return openapiv3Patch, nil
		}

	}
	return nil, nil
}

// 如果启用了 OpenAPI，buildStrategicMergeFromOpenAPI 会从 OpenAPI 构建 patch。
// 这用于发布在 openapi 中的核心类型。
func (p *Patcher) buildStrategicMergeFromOpenAPI(openAPISchema openapi.Resources, original, modified, current []byte) ([]byte, error) {
	schema := openAPISchema.LookupResource(p.Mapping.GroupVersionKind)
	if schema == nil {
		// 缺少 schema 时返回 nil patch，且不返回错误。
		return nil, nil
	}
	lookupPatchMeta := strategicpatch.PatchMetaFromOpenAPI{Schema: schema}
	if openapiPatch, err := strategicpatch.CreateThreeWayMergePatch(original, modified, current, lookupPatchMeta, p.Overwrite); err != nil {
		return nil, err
	} else {
		return openapiPatch, nil
	}
}

// getPatchTypeFromOpenAPI 在 Open API 中查找给定 GroupVersionKind 支持的 patch 类型。
func (p *Patcher) getPatchTypeFromOpenAPI(openAPISchema openapi.Resources, gvk schema.GroupVersionKind) (types.PatchType, error) {
	if pc := openAPISchema.GetConsumes(p.Mapping.GroupVersionKind, "PATCH"); pc != nil {
		for _, c := range pc {
			if c == string(types.StrategicMergePatchType) {
				return types.StrategicMergePatchType, nil
			}
		}

		return types.MergePatchType, nil
	}

	return types.MergePatchType, fmt.Errorf("unable to find any patch type for %s in Open API", gvk)
}

// buildStrategicMergeFromStruct 从 struct 构建 patch。当
// openapi endpoint 无法工作，或用户通过设置 openapi-patch flag
// 为 false 禁用它时使用。
func (p *Patcher) buildStrategicMergeFromBuiltins(versionedObj runtime.Object, original, modified, current []byte) ([]byte, error) {
	lookupPatchMeta, err := strategicpatch.NewPatchMetaFromStruct(versionedObj)
	if err != nil {
		return nil, err
	}
	patch, err := strategicpatch.CreateThreeWayMergePatch(original, modified, current, lookupPatchMeta, p.Overwrite)
	if err != nil {
		return nil, err
	}

	return patch, nil
}

// Patch 尝试 patch 一个 OpenAPI 资源。成功时返回 merge patch 以及
// 最终修补后的对象。失败时返回错误。
func (p *Patcher) Patch(current runtime.Object, modified []byte, source, namespace, name string, errOut io.Writer) ([]byte, runtime.Object, error) {
	var getErr error
	patchBytes, patchObject, err := p.patchSimple(current, modified, namespace, name, errOut, false)
	if p.Retries == 0 {
		p.Retries = maxPatchRetry
	}
	for i := 1; i <= p.Retries && apierrors.IsConflict(err); i++ {
		if i > triesBeforeBackOff {
			p.BackOff.Sleep(patchRetryBackOffPeriod)
		}
		current, getErr = p.Helper.Get(namespace, name)
		if getErr != nil {
			return nil, nil, getErr
		}
		patchBytes, patchObject, err = p.patchSimple(current, modified, namespace, name, errOut, false)
	}
	if err != nil {
		if (apierrors.IsConflict(err) || apierrors.IsInvalid(err)) && p.Force {
			patchBytes, patchObject, err = p.deleteAndCreate(current, modified, namespace, name)
		} else {
			err = cmdutil.AddSourceToErr("patching", source, err)
		}
	}
	return patchBytes, patchObject, err
}

// PatchLocal 在本地计算并应用 patch，不发送到服务端。
// 用于 --dry-run=client。
func (p *Patcher) PatchLocal(current runtime.Object, modified []byte, errOut io.Writer) ([]byte, runtime.Object, error) {
	return p.patchSimple(current, modified, "", "", errOut, true)
}

func (p *Patcher) deleteAndCreate(original runtime.Object, modified []byte, namespace, name string) ([]byte, runtime.Object, error) {
	if err := p.delete(namespace, name); err != nil {
		return modified, nil, err
	}
	// TODO: 使用 wait
	if err := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, p.Timeout, true, func(ctx context.Context) (bool, error) {
		if _, err := p.Helper.Get(namespace, name); !apierrors.IsNotFound(err) {
			return false, err
		}
		return true, nil
	}); err != nil {
		return modified, nil, err
	}
	versionedObject, _, err := unstructured.UnstructuredJSONScheme.Decode(modified, nil, nil)
	if err != nil {
		return modified, nil, err
	}
	createdObject, err := p.Helper.Create(namespace, true, versionedObject)
	if err != nil {
		// 如果创建新对象失败，则恢复原始对象，
		// 但仍要向用户传播并提示错误
		recreated, recreateErr := p.Helper.Create(namespace, true, original)
		if recreateErr != nil {
			err = fmt.Errorf("An error occurred force-replacing the existing object with the newly provided one:\n\n%v.\n\nAdditionally, an error occurred attempting to restore the original object:\n\n%v", err, recreateErr)
		} else {
			createdObject = recreated
		}
	}
	return modified, createdObject, err
}

func addResourceVersion(patch []byte, rv string) ([]byte, error) {
	var patchMap map[string]interface{}
	err := json.Unmarshal(patch, &patchMap)
	if err != nil {
		return nil, err
	}
	u := unstructured.Unstructured{Object: patchMap}
	a, err := meta.Accessor(&u)
	if err != nil {
		return nil, err
	}
	a.SetResourceVersion(rv)

	return json.Marshal(patchMap)
}
