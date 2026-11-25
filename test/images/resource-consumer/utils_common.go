/*
Copyright 2015 The Kubernetes Authors.

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
	"log"
	"os/exec"
)

// ConsumeCPU 在指定时长内消耗给定数量的毫核（millicore）。
func ConsumeCPU(millicores int, durationSec int) {
	log.Printf("ConsumeCPU millicores: %v, durationSec: %v", millicores, durationSec)
	// 创建新的 CPU 消耗进程
	arg1 := fmt.Sprintf("-millicores=%d", millicores)
	arg2 := fmt.Sprintf("-duration-sec=%d", durationSec)
	consumeCPU := exec.Command(consumeCPUBinary, arg1, arg2)
	err := consumeCPU.Run()
	if err != nil {
		log.Printf("Error while consuming CPU: %v", err)
	}
}

// GetCurrentStatus 输出一个空操作。
func GetCurrentStatus() {
	log.Printf("GetCurrentStatus")
	// 尚未实现
}
