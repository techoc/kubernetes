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

package validating

import (
	"context"

	"k8s.io/apiserver/pkg/admission"
)

// CELPolicyEvaluator is the interface the validating policy plugin exposes to
// the admission chain. It embeds InitializationValidator so the plugin can be
// initialized once (type-checking, informer sync) before serving traffic, and
// Run/HasSynced let the apiserver lifecycle manage its background loops.
type CELPolicyEvaluator interface {
	admission.InitializationValidator

	// Validate evaluates all matching policies for the given attributes and
	// returns an admission error if any policy denies the request.
	Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error
	// HasSynced reports whether policy and param caches have synced.
	HasSynced() bool
	// Run starts background work and returns when stopCh is closed.
	Run(stopCh <-chan struct{})
}
