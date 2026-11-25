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

	podgcconfig "k8s.io/kubernetes/pkg/controller/podgc/config"
)

// PodGCControllerOptions 保存 Pod 垃圾回收控制器的选项。
type PodGCControllerOptions struct {
	*podgcconfig.PodGCControllerConfiguration
}

// AddFlags 将 Pod 垃圾回收控制器相关参数注册到指定 FlagSet。
func (o *PodGCControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.Int32Var(&o.TerminatedPodGCThreshold, "terminated-pod-gc-threshold", o.TerminatedPodGCThreshold, "Number of terminated pods that can exist before the terminated pod garbage collector starts deleting terminated pods. If <= 0, the terminated pod garbage collector is disabled.")
}

// ApplyTo 将选项写入 Pod 垃圾回收控制器配置。
func (o *PodGCControllerOptions) ApplyTo(cfg *podgcconfig.PodGCControllerConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.TerminatedPodGCThreshold = o.TerminatedPodGCThreshold

	return nil
}

// Validate 校验 Pod 垃圾回收控制器选项。
func (o *PodGCControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	return errs
}
