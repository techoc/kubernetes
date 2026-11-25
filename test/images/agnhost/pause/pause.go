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

package pause

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// CmdPause 由 agnhost Cobra 使用。
var CmdPause = &cobra.Command{
	Use:   "pause",
	Short: "Pauses the execution",
	Long:  `Pauses the execution. Useful for keeping the containers running, so other commands can be executed.`,
	Args:  cobra.MaximumNArgs(0),
	Run:   pause,
}

func pause(cmd *cobra.Command, args []string) {
	fmt.Println("Paused")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("Signals registered")
	sig := <-sigCh

	// 使用 agnhost 镜像的 Pod 可能会覆盖默认的
	// 终止消息路径。此时它们必须使用 TERMINATION_MESSAGE_PATH
	// 环境变量。
	//
	// `terminationMessagePolicy: FallbackToLogsOnError` 同样有效，因为
	// 同一条消息也是输出的最后一行。
	terminationMessagePath := os.Getenv("TERMINATION_MESSAGE_PATH")
	if terminationMessagePath == "" {
		terminationMessagePath = "/dev/termination-log"
	}
	exitMsg := []byte(fmt.Sprintf("exiting because of signal %q\n", sig))
	_ = os.WriteFile(terminationMessagePath, exitMsg, 0644)
	_, _ = os.Stdout.Write(exitMsg)

	var result int
	switch sig {
	case syscall.SIGINT:
		result = 1
	case syscall.SIGTERM:
		result = 2
	}
	os.Exit(result)
}
