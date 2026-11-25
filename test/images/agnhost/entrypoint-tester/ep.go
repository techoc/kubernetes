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

package entrypoint

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// CmdEntrypointTester 由 agnhost Cobra 使用。
var CmdEntrypointTester = &cobra.Command{
	Use:   "entrypoint-tester",
	Short: "Prints the args it's passed and exits",
	Long:  "Prints the args it's passed and exits.",
	Run:   main,
}

// 本程序打印可执行文件的所有参数后退出。
func main(cmd *cobra.Command, args []string) {
	// 某些与 entrypoint-tester 相关的测试会用 agnhost-2 覆盖 agnhost 的默认入口点，
	// 此时本函数的 args 只包含子命令的参数
	// （./agnhost entrypoint-tester these args），但我们需要打印*所有*
	// 参数，因此改为打印 os.Args。
	fmt.Printf("%v\n", os.Args)
	os.Exit(0)
}
