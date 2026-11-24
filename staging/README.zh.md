# 外部仓库暂存区

本目录用于暂存已拆分到独立仓库的软件包。这里的内容会定期发布到各自的顶级
`k8s.io` 仓库。

目前在此暂存的仓库：

- [`k8s.io/api`](https://github.com/kubernetes/api)
- [`k8s.io/apiextensions-apiserver`](https://github.com/kubernetes/apiextensions-apiserver)
- [`k8s.io/apimachinery`](https://github.com/kubernetes/apimachinery)
- [`k8s.io/apiserver`](https://github.com/kubernetes/apiserver)
- [`k8s.io/cli-runtime`](https://github.com/kubernetes/cli-runtime)
- [`k8s.io/client-go`](https://github.com/kubernetes/client-go)
- [`k8s.io/cloud-provider`](https://github.com/kubernetes/cloud-provider)
- [`k8s.io/cluster-bootstrap`](https://github.com/kubernetes/cluster-bootstrap)
- [`k8s.io/code-generator`](https://github.com/kubernetes/code-generator)
- [`k8s.io/component-base`](https://github.com/kubernetes/component-base)
- [`k8s.io/component-helpers`](https://github.com/kubernetes/component-helpers)
- [`k8s.io/controller-manager`](https://github.com/kubernetes/controller-manager)
- [`k8s.io/cri-api`](https://github.com/kubernetes/cri-api)
- [`k8s.io/cri-client`](https://github.com/kubernetes/cri-client)
- [`k8s.io/cri-streaming`](https://github.com/kubernetes/cri-streaming)
- [`k8s.io/csi-translation-lib`](https://github.com/kubernetes/csi-translation-lib)
- [`k8s.io/dynamic-resource-allocation`](https://github.com/kubernetes/dynamic-resource-allocation)
- [`k8s.io/endpointslice`](https://github.com/kubernetes/endpointslice)
- [`k8s.io/externaljwt`](https://github.com/kubernetes/externaljwt)
- [`k8s.io/kms`](https://github.com/kubernetes/kms)
- [`k8s.io/kube-aggregator`](https://github.com/kubernetes/kube-aggregator)
- [`k8s.io/kube-controller-manager`](https://github.com/kubernetes/kube-controller-manager)
- [`k8s.io/kube-proxy`](https://github.com/kubernetes/kube-proxy)
- [`k8s.io/kube-scheduler`](https://github.com/kubernetes/kube-scheduler)
- [`k8s.io/kubectl`](https://github.com/kubernetes/kubectl)
- [`k8s.io/kubelet`](https://github.com/kubernetes/kubelet)
- [`k8s.io/metrics`](https://github.com/kubernetes/metrics)
- [`k8s.io/mount-utils`](https://github.com/kubernetes/mount-utils)
- [`k8s.io/pod-security-admission`](https://github.com/kubernetes/pod-security-admission)
- [`k8s.io/sample-apiserver`](https://github.com/kubernetes/sample-apiserver)
- [`k8s.io/sample-cli-plugin`](https://github.com/kubernetes/sample-cli-plugin)
- [`k8s.io/sample-controller`](https://github.com/kubernetes/sample-controller)

`staging/` 目录中的代码是权威版本，即代码的唯一副本。你可以直接修改这些代码。

## 在 Kubernetes 代码中使用暂存仓库

Kubernetes 通过 Go workspace 和模块 `replace` 指令使用本目录中的仓库。例如，当
Kubernetes 代码导入 `k8s.io/client-go` 仓库中的软件包时，该导入会解析到项目根目录下
的 `staging/src/k8s.io/client-go`：

```go
// pkg/example/some_code.go
package example

import (
	"k8s.io/client-go/dynamic" // 解析到 staging/src/k8s.io/client-go/dynamic
)
```

## 在暂存区中创建新仓库

### 在 `kubernetes/kubernetes` 中添加暂存仓库：

1. 向 SIG Architecture[邮件列表](https://groups.google.com/a/kubernetes.io/g/sig-architecture)
   和将负责该仓库的 SIG 邮件列表发送邮件，申请批准创建暂存仓库。

2. 获得批准后，创建新的暂存仓库。

3. 更新
   [`import-restrictions.yaml`](/staging/publishing/import-restrictions.yaml)，
   添加该新仓库可以导入的其他暂存仓库列表。

4. 按照
   https://github.com/kubernetes/kubernetes-template-project
   中的说明，向暂存仓库添加所有必需的模板文件。

5. 确保 `.github/PULL_REQUEST_TEMPLATE.md` 和 `CONTRIBUTING.md` 文件注明，
   该仓库不直接接受 PR。

6. 确保添加 `docs.go` 文件。请参阅
   [#kubernetes/kubernetes#91354](https://github.com/kubernetes/kubernetes/blob/release-1.24/staging/src/k8s.io/client-go/doc.go)
   作为参考。

7. 注意：不要手动编辑新仓库（`staging/src/k8s.io/<newrepo>/`）中的 `go.mod`
   或 `go.sum`。请改为运行以下命令：

```
  ./hack/update-vendor.sh
```

### 创建发布仓库

1. 在 `kubernetes/org` 仓库中创建一个
   [issue](https://github.com/kubernetes/org/issues/new?assignees=&labels=area%2Fgithub-repo&projects=&template=repo-create.yml&title=REQUEST%3A+%3CCreate+or+Migrate%3E+%3Cgithub+repo%3E)，
   申请在 Kubernetes 组织中创建相应的发布仓库。发布仓库**必须**具有一个初始空提交，
   还需要设置特定的访问规则和分支设置。请参阅
   [#kubernetes/org#58](https://github.com/kubernetes/org/issues/58)
   中的示例。

2. 设置分支保护，并通过在
   [`prow/config.yaml`](https://github.com/kubernetes/test-infra/blob/master/config/prow/config.yaml)
   中添加该仓库，允许 `stage-bots` 团队访问。请参阅
   [#kubernetes/test-infra#9292](https://github.com/kubernetes/test-infra/pull/9292)
   中的示例。

3. 在 Kubernetes 组织中创建仓库后，通过更新以下内容来配置 publishing-bot，
   使其发布暂存仓库：

    - [`rules.yaml`](/staging/publishing/rules.yaml)：
      确保依赖列表与 `Godeps.json` 文件中的暂存仓库一致。

    - [`repos.sh`](https://github.com/kubernetes/publishing-bot/blob/master/hack/repos.sh)：
      将暂存仓库添加到待发布仓库列表中。

4. 在负责这些仓库的 SIG 的
   [`sigs.yaml`](https://github.com/kubernetes/community/blob/master/sigs.yaml)
   中，将暂存仓库和发布仓库添加为子项目。

5. 将仓库添加到本 `README.md` 文件的暂存仓库列表中。
