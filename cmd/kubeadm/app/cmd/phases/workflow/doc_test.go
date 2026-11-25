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

package workflow

import (
	"fmt"

	"github.com/spf13/cobra"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

var myWorkflowRunner = NewRunner()

type myWorkflowData struct {
	data string
}

func (c *myWorkflowData) Data() string {
	return c.data
}

type myPhaseData interface {
	Data() string
}

func ExamplePhase() {
	// 创建阶段。
	var myPhase1 = Phase{
		Name:  "myPhase1",
		Short: "A phase of a kubeadm composable workflow...",
		Run: func(data RunData) error {
			// 将数据转换为带类型的数据结构。
			d, ok := data.(myPhaseData)
			if !ok {
				return errors.New("invalid RunData type")
			}

			// 实现阶段逻辑。
			fmt.Printf("%v\n", d.Data())
			return nil
		},
	}

	// 创建另一个阶段。
	var myPhase2 = Phase{
		Name:  "myPhase2",
		Short: "Another phase of a kubeadm composable workflow...",
		Run: func(data RunData) error {
			// 将数据转换为带类型的数据结构。
			d, ok := data.(myPhaseData)
			if !ok {
				return errors.New("invalid RunData type")
			}

			// 实现阶段逻辑。
			fmt.Printf("%v\n", d.Data())
			return nil
		},
	}

	// 将新阶段添加到工作流。
	// 阶段按添加到工作流的顺序执行。
	myWorkflowRunner.AppendPhase(myPhase1)
	myWorkflowRunner.AppendPhase(myPhase2)
}

func ExampleRunner_Run() {
	// 创建阶段。
	var myPhase = Phase{
		Name:  "myPhase",
		Short: "A phase of a kubeadm composable workflow...",
		Run: func(data RunData) error {
			// 将数据转换为带类型的数据结构。
			d, ok := data.(myPhaseData)
			if !ok {
				return errors.New("invalid RunData type")
			}

			// 实现阶段逻辑。
			fmt.Printf("%v\n", d.Data())
			return nil
		},
	}

	// 将新阶段添加到工作流。
	var myWorkflowRunner = NewRunner()
	myWorkflowRunner.AppendPhase(myPhase)

	// 定义创建运行时数据的方法，数据由工作流包含的所有阶段共享。
	myWorkflowRunner.SetDataInitializer(func(cmd *cobra.Command, args []string) (RunData, error) {
		return myWorkflowData{data: "some data"}, nil
	})

	// 传入参数列表以运行工作流。
	myWorkflowRunner.Run([]string{})
}
