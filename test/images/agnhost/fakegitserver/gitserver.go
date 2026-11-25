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

package fakegitserver

import (
	"io"
	"net/http"

	"github.com/spf13/cobra"
)

// CmdFakeGitServer 由 agnhost 的 Cobra 命令使用。
var CmdFakeGitServer = &cobra.Command{
	Use:   "fake-gitserver",
	Short: "Fakes a git server",
	Long: `When doing "git clone http://localhost:8000", you will clone an empty git repo named "localhost" on local.
You can also use "git clone http://localhost:8000 my-repo-name" to rename that repo.`,
	Args: cobra.MaximumNArgs(0),
	Run:  main,
}

func hello(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "I am a fake git server")

}

// 执行 `git clone localhost:8000` 时，会在本地克隆一个名为 "8000" 的空 git 仓库。
// 也可以使用 `git clone localhost:8000 my-repo-name` 为该仓库重命名。
func main(cmd *cobra.Command, args []string) {
	http.HandleFunc("/", hello)
	http.ListenAndServe(":8000", nil)
}
