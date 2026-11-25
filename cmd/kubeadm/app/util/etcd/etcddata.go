/*
Copyright 2020 The Kubernetes Authors.

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

package etcd

import (
	"os"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// CreateDataDirectory 创建 etcd 数据目录（通常为 /var/lib/etcd）并设置正确权限。
func CreateDataDirectory(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return errors.Wrapf(err, "failed to create the etcd data directory: %q", dir)
	}
	return nil
}
