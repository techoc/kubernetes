/*
Copyright 2017 The Kubernetes Authors.

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

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	// 挂载命令所使用的程序路径
	chrootCmd        = "chroot"
	mountCmd         = "mount"
	mountBin         = "/bin/mount"
	rootfs           = "rootfs"
	nfsRPCBindErrMsg = "mount.nfs: rpc.statd is not running but is required for remote locking.\nmount.nfs: Either use '-o nolock' to keep locks local, or start statd.\nmount.nfs: an incorrect mount option was specified\n"
	rpcBindCmd       = "/sbin/rpcbind"
	defaultRootfs    = "/home/kubernetes/containerized_mounter/rootfs"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Command failed: must provide a command to run.\n")
		return
	}
	path, _ := filepath.Split(os.Args[0])
	rootfsPath := filepath.Join(path, rootfs)
	if _, err := os.Stat(rootfsPath); os.IsNotExist(err) {
		rootfsPath = defaultRootfs
	}
	command := os.Args[1]
	switch command {
	case mountCmd:
		mountErr := mountInChroot(rootfsPath, os.Args[2:])
		if mountErr != nil {
			fmt.Fprintf(os.Stderr, "Mount failed: %v", mountErr)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown command, must be %s", mountCmd)
		os.Exit(1)

	}
}

// mountInChroot 在指定的根目录下通过 chroot 执行挂载
func mountInChroot(rootfsPath string, args []string) error {
	if _, err := os.Stat(rootfsPath); os.IsNotExist(err) {
		return fmt.Errorf("path <%s> does not exist", rootfsPath)
	}
	args = append([]string{rootfsPath, mountBin}, args...)
	output, err := exec.Command(chrootCmd, args...).CombinedOutput()
	if err == nil {
		return nil
	}

	if !strings.EqualFold(string(output), nfsRPCBindErrMsg) {
		// 挂载失败，但不是 RPC 绑定错误导致的
		return fmt.Errorf("mount failed: %v\nMounting command: %s\nMounting arguments: %v\nOutput: %s", err, chrootCmd, args, string(output))
	}

	// 挂载失败是因为 NFS V3 需要先启动 rpcbind
	output, err = exec.Command(chrootCmd, rootfsPath, rpcBindCmd, "-w").CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount issued for NFS V3 but unable to run rpcbind:\n Output: %s\n Error: %v", string(output), err)
	}

	// rpcbind 已启动，重新尝试挂载
	output, err = exec.Command(chrootCmd, args...).CombinedOutput()

	if err != nil {
		return fmt.Errorf("mount failed for NFS V3 even after running rpcBind %s, %v", string(output), err)
	}

	return nil
}
