//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

/*
Copyright 2016 The Kubernetes Authors.

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

package flock

import "golang.org/x/sys/unix"

// Acquire 获取文件锁，锁的持有周期为整个进程生命周期。该方法可重入。
func Acquire(path string) error {
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}

	// 不关闭 fd：需要一直持有它直到进程退出。

	return unix.Flock(fd, unix.LOCK_EX)
}
