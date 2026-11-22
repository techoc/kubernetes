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

package sharding

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

// ResolveFieldValue extracts a metadata field value from a runtime.Object
// based on the given field path.
//
// Field paths use CEL-style object-rooted syntax ("object.metadata.<field>"),
// which differs from the fieldSelector format ("metadata.<field>"). The
// "object." prefix anchors the path to the resource being filtered.
//
// Supported field paths:
//   - "object.metadata.uid"
//   - "object.metadata.namespace"
func ResolveFieldValue(obj runtime.Object, fieldPath string) (string, error) {
	// All supported paths live under metadata, so the accessor is obtained
	// once up front.
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return "", fmt.Errorf("failed to access object metadata: %w", err)
	}

	// Only fields whose values are stable and shardable are accepted; unknown
	// paths are an error so typos do not silently produce a wrong partition.
	switch fieldPath {
	case "object.metadata.uid":
		return string(accessor.GetUID()), nil
	case "object.metadata.namespace":
		return accessor.GetNamespace(), nil
	default:
		return "", fmt.Errorf("unsupported field path: %q", fieldPath)
	}
}
