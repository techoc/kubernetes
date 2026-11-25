/*
Copyright 2018 The Kubernetes Authors.

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

package options

import (
	"github.com/spf13/pflag"

	garbagecollectorconfig "k8s.io/kubernetes/pkg/controller/garbagecollector/config"
)

// GarbageCollectorControllerOptions 保存垃圾回收控制器的选项。
type GarbageCollectorControllerOptions struct {
	*garbagecollectorconfig.GarbageCollectorControllerConfiguration
}

// AddFlags 将垃圾回收控制器相关参数注册到指定 FlagSet。
func (o *GarbageCollectorControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.Int32Var(&o.ConcurrentGCSyncs, "concurrent-gc-syncs", o.ConcurrentGCSyncs, "The number of garbage collector workers that are allowed to sync concurrently.")
	fs.BoolVar(&o.EnableGarbageCollector, "enable-garbage-collector", o.EnableGarbageCollector, "Enables the generic garbage collector. MUST be synced with the corresponding flag of the kube-apiserver.")
}

// ApplyTo 将选项写入垃圾回收控制器配置。
func (o *GarbageCollectorControllerOptions) ApplyTo(cfg *garbagecollectorconfig.GarbageCollectorControllerConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.ConcurrentGCSyncs = o.ConcurrentGCSyncs
	cfg.GCIgnoredResources = o.GCIgnoredResources
	cfg.EnableGarbageCollector = o.EnableGarbageCollector

	return nil
}

// Validate 校验垃圾回收控制器选项。
func (o *GarbageCollectorControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	return errs
}
