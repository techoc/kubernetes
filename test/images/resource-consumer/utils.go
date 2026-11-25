//go:build !windows

/*
Copyright 2021 The Kubernetes Authors.

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
	"log"
	"os/exec"
	"strconv"
)

var (
	consumeCPUBinary = "./consume-cpu/consume-cpu"
	consumeMemBinary = "stress"
)

// ConsumeMem 在指定时长内消耗给定数量的内存（以兆字节计）。
func ConsumeMem(megabytes int, durationSec int) {
	log.Printf("ConsumeMem megabytes: %v, durationSec: %v", megabytes, durationSec)
	megabytesString := strconv.Itoa(megabytes) + "M"
	durationSecString := strconv.Itoa(durationSec)
	// 创建新的内存消耗进程
	consumeMem := exec.Command(consumeMemBinary, "-m", "1", "--vm-bytes", megabytesString, "--vm-hang", "0", "-t", durationSecString)
	err := consumeMem.Run()
	if err != nil {
		log.Printf("Error while consuming memory: %v", err)
	}
}
