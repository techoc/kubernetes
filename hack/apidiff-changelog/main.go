/*
Copyright The Kubernetes Authors.

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

// Package main 提供跨模块比较 API 变更的命令行工具。它运行 apidiff 检测两个 git 版本之间的 API 变更，
// 并为不兼容变更插入或校验 changelog 条目。
//
// 返回值：
// - 0：未发现不兼容变更
// - 1：发生意外错误
// - 2：发现不兼容变更，且至少有一项未记录
// - 3：发现不兼容变更，且全部已记录
package main

import (
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

//go:embed api-changes-allowlist
var apiChangesAllowlist string

// changelogFilename 是 changelog 文件的默认名称。
const changelogFilename = "CHANGELOG.md"

// errVerificationFail 表示校验失败：在 changelog 中找不到预期的变更。
var errVerificationFail = errors.New("changes not found in code blocks of first heading")

// exitError 由 main() 用来决定退出码。
type exitError struct {
	exitCode int
}

func (e exitError) Error() string {
	return ""
}

// runDiffOptions 配置 API diff 的行为。
type runDiffOptions struct {
	repoRoot        string // 仓库根目录，默认为当前目录
	base            string // 基准 git 版本（必填）
	target          string // 目标 git 版本，为空时表示工作区
	updateChangelog bool   // 直接更新 changelog 文件，而不是输出补丁
	mergeCommit     string // 非空时使用指定的合并提交来填充新的 changelog 小节
}

func main() {
	repoRoot := flag.String("repo-root", ".", "root of the repository; defaults to the current directory")
	base := flag.String("base", "", "base git revision (required)")
	target := flag.String("target", "", "target git revision; default is the working tree")
	updateChangelog := flag.Bool("update-changelog", false, "update CHANGELOG.md files in place when incompatible changes are found")
	mergeCommit := flag.String("merge-commit", "", "when set, populates new changelog sections with PR information from this merge commit")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <module-dir>...\n\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "This tool analyzes API changes between two git revisions for one or more Go modules.")
		fmt.Fprintln(os.Stderr, "Module directories must be provided as positional arguments.")
		fmt.Fprintln(os.Stderr, "\nOptions:")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		fmt.Fprintln(os.Stderr, "\nError: at least one module directory must be specified")
		os.Exit(1)
	}

	if *base == "" {
		flag.Usage()
		fmt.Fprintln(os.Stderr, "\nError: -base is required")
		os.Exit(1)
	}
	opts := runDiffOptions{
		repoRoot:        *repoRoot,
		base:            *base,
		target:          *target,
		updateChangelog: *updateChangelog,
		mergeCommit:     *mergeCommit,
	}
	if err := runDiff(opts, flag.Args()); err != nil {
		if errExit, ok := errors.AsType[exitError](err); ok {
			// 不输出 "Error"，语义过硬。
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(errExit.exitCode)
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// runDiff 执行完整的 API diff 流程：导出基准版本和目标版本的 API 状态，
// 逐模块比较、报告不兼容变更，并按需更新或修补 CHANGELOG.md。
// 只要有模块存在未处理的不兼容变更，就返回非 nil 错误。
func runDiff(opts runDiffOptions, dirs []string) error {
	if _, err := exec.LookPath("apidiff"); err != nil {
		return fmt.Errorf("apidiff binary not found in PATH; please install it with 'go install golang.org/x/exp/cmd/apidiff@latest'")
	}

	tempDir, err := os.MkdirTemp("", "apidiff-")
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	repoRoot, err := filepath.Abs(opts.repoRoot)
	if err != nil {
		return fmt.Errorf("resolving repo root %q: %w", opts.repoRoot, err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getting working directory: %w", err)
	}
	dirs, err = normalizeDirs(cwd, repoRoot, dirs)
	if err != nil {
		return fmt.Errorf("normalizing directories: %w", err)
	}

	afterDir := filepath.Join(tempDir, "after")
	beforeDir := filepath.Join(tempDir, "before")

	// 导出目标版本（或工作区）的 API 状态。
	if opts.target == "" {
		if err := runApidiff(repoRoot, dirs, afterDir); err != nil {
			return fmt.Errorf("running apidiff on working tree: %w", err)
		}
	} else {
		targetWorktree := filepath.Join(tempDir, "target")
		cleanup, err := setupWorktree(targetWorktree, opts.target)
		if err != nil {
			return err
		}
		defer cleanup()
		if err := runApidiff(targetWorktree, dirs, afterDir); err != nil {
			return fmt.Errorf("running apidiff on target %s: %w", opts.target, err)
		}
	}

	// 导出基准版本的 API 状态。
	baseWorktree := filepath.Join(tempDir, "base")
	cleanup, err := setupWorktree(baseWorktree, opts.base)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := runApidiff(baseWorktree, dirs, beforeDir); err != nil {
		return fmt.Errorf("running apidiff on base %s: %w", opts.base, err)
	}

	// 从 PR 合并提交中填充 changelog 时，先提取提交信息。
	var mcTitle, mcDescription string
	if opts.mergeCommit != "" {
		mcTitle, mcDescription, err = getMergeCommitInfo(opts.mergeCommit)
		if err != nil {
			return err
		}
	}

	const defaultTitle = "Replace with a short title"
	const defaultDescription = `Replace this text with a short summary of the change
and how users of the package can deal with this breaking
change. If users are not expected to be affected, then
instead explain why. If the changes are too long,
you may shorten them by replacing multiple lines
with three dots (...).`

	type changelogEntry struct {
		original string
		updated  string
	}
	var dirsWithChanges []string
	dirLabel := make(map[string]string) // 逐目录标注："已记录"、已补充占位文档，或"未记录"
	var changelogEntries []changelogEntry
	foundDirs := 0
	addedDirs := 0

	fmt.Println()

	// 遍历各模块目录，检测并处理不兼容的 API 变更：
	// 1. 比较基准版本与目标版本的 apidiff 输出。
	// 2. 若存在不兼容变更，检查模块是否维护 CHANGELOG.md。
	// 3. 校验变更是否已记录在 changelog 中。
	// 4. 若未记录，根据参数决定直接更新 changelog，还是生成补丁。
	for _, dir := range dirs {
		beforeFile := filepath.Join(beforeDir, outputName(dir))
		afterFile := filepath.Join(afterDir, outputName(dir))

		incompatible, err := compareApidiff(dir, beforeFile, afterFile)
		if err != nil {
			return fmt.Errorf("comparing %s: %w", dir, err)
		}
		if incompatible == "" {
			continue
		}
		dirsWithChanges = append(dirsWithChanges, dir)

		// 检查该目录下是否有 CHANGELOG.md。
		changelog := filepath.Join(repoRoot, dir, changelogFilename)
		if _, err := os.Stat(changelog); os.IsNotExist(err) {
			// 没有 changelog 文件，无需进一步处理。
			continue
		}

		// 校验不兼容变更是否已记录。
		verifyErr := verifyChangelog(changelog, incompatible)
		if verifyErr == nil {
			// 已记录，不算失败。
			dirLabel[dir] = "documented"
			foundDirs++
			continue
		}
		if !errors.Is(verifyErr, errVerificationFail) {
			return fmt.Errorf("verifying changelog for %s: %w", dir, verifyErr)
		}

		// 确定新 changelog 小节的标题和描述。
		title, description := defaultTitle, defaultDescription
		if opts.mergeCommit != "" {
			title, description = mcTitle, mcDescription
		}

		if opts.updateChangelog {
			// 直接把条目插入原始 changelog。
			if err := insertChangelog(changelog, incompatible, title, description); err != nil {
				return fmt.Errorf("inserting changelog entry for %s: %w", dir, err)
			}
			// 现已记录，不视为失败。
			dirLabel[dir] = "added placeholder documentation"
			addedDirs++
		} else {
			// 把条目写入临时副本，稍后据此生成补丁。
			tempChangelog := filepath.Join(tempDir, dir, changelogFilename)
			if err := os.MkdirAll(filepath.Dir(tempChangelog), 0755); err != nil {
				return fmt.Errorf("creating temp changelog directory: %w", err)
			}
			content, err := os.ReadFile(changelog)
			if err != nil {
				return fmt.Errorf("reading changelog %s: %w", changelog, err)
			}
			if err := os.WriteFile(tempChangelog, content, 0644); err != nil {
				return fmt.Errorf("writing temp changelog: %w", err)
			}
			if err := insertChangelog(tempChangelog, incompatible, title, description); err != nil {
				return fmt.Errorf("inserting changelog entry for %s: %w", dir, err)
			}
			changelogEntries = append(changelogEntries, changelogEntry{original: changelog, updated: tempChangelog})
		}
	}

	if len(dirsWithChanges) == 0 {
		return nil
	}

	fmt.Print(`
Detected incompatible changes on modules:
`)
	for _, f := range dirsWithChanges {
		label := dirLabel[f]
		if label == "" {
			label = "undocumented"
		}
		fmt.Printf("%s (%s)\n", f, label)
	}
	fmt.Print(`
For more information about incompatible changes, see
https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/go_api_changes.md

`)

	if len(changelogEntries) > 0 {
		dirArgs := strings.Join(dirs, " ")
		targetFlag := ""
		if opts.target != "" {
			targetFlag = fmt.Sprintf("-t %s ", opts.target)
		}
		fmt.Printf(`
Run the following command to add the incompatible changes to
the %s file(s), edit the modified file(s) to
replace the template text in the new section at the top
with an explanation of the changes, then include the result
in the pull request for review:

    hack/update-go-apidocs.sh

Under the hood this will run:

    hack/apidiff.sh -u -r %s %s%s

If running these commands is impossible, then you can
apply the following diff instead by piping it into "patch -p0":

vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
`, changelogFilename, opts.base, targetFlag, dirArgs)
		for _, entry := range changelogEntries {
			// diff 发现差异时会返回 1，忽略退出码。
			diffCmd := exec.Command("diff", "-c", entry.original, entry.updated)
			out, _ := diffCmd.CombinedOutput()
			fmt.Print(string(out))
		}
		fmt.Print(`^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`)
	}

	// 3 告诉 apidiff.sh：即使存在不兼容变更，也应视为检查通过。
	if len(dirsWithChanges) == foundDirs+addedDirs {
		var parts []string
		if foundDirs > 0 {
			//nolint:staticcheck // ST1005: error strings should not be capitalized - ignored here because exit errors get dumped to stderr as-is and this is more readable.
			parts = append(parts, fmt.Sprintf("Found documentation of changes in %d module(s)", foundDirs))
		}
		if addedDirs > 0 {
			//nolint:staticcheck // ST1005: error strings should not be capitalized - ignored here because exit errors get dumped to stderr as-is and this is more readable.
			parts = append(parts, fmt.Sprintf("Added documentation of changes in %d module(s)", addedDirs))
		}
		return fmt.Errorf("%s%w", strings.Join(parts, "\n"), exitError{3})
	}

	// 2 表示缺少文档，或需要人工处理（例如编辑并提交更新后的 CHANGELOG.md）。
	//
	//nolint:staticcheck // ST1005: error strings should not be capitalized
	return fmt.Errorf("Error: incompatible changes in %d module(s)%w", len(dirsWithChanges), exitError{2})
}

// setupWorktree 为指定版本在 worktreePath 创建 git worktree，
// 返回一个用于删除该 worktree 的清理函数。
func setupWorktree(worktreePath, rev string) (func(), error) {
	if _, err := os.Stat(worktreePath); os.IsNotExist(err) {
		cmd := exec.Command("git", "worktree", "add", "-f", "-d", worktreePath, rev)
		if out, err := cmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("git worktree add %s: %w\n%s", rev, err, out)
		}
	}
	return func() {
		_ = exec.Command("git", "worktree", "remove", "-f", worktreePath).Run()
	}, nil
}

// normalizeDirs 将每个目录转换为相对 repoRoot 的路径。
// 相对路径先基于 cwd 转为绝对路径；若有路径不在 repoRoot 下则返回错误。
func normalizeDirs(cwd, repoRoot string, dirs []string) ([]string, error) {
	var result []string
	seen := make(map[string]bool)
	for _, dir := range dirs {
		if !filepath.IsAbs(dir) {
			dir = filepath.Join(cwd, dir)
		}
		rel, err := filepath.Rel(repoRoot, dir)
		if err != nil {
			return nil, fmt.Errorf("computing relative path for %q: %w", dir, err)
		}
		if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("%q is not under repo root %q", dir, repoRoot)
		}
		// 去重
		if !seen[rel] {
			seen[rel] = true
			result = append(result, rel)
		}
	}
	return result, nil
}

// runApidiff 对每个目标目录并行运行 apidiff，把模块 API 状态写入 outDir。
func runApidiff(repoRoot string, dirs []string, outDir string) error {
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for _, dir := range dirs {
		absDir := filepath.Join(repoRoot, dir)
		if _, err := os.Stat(absDir); os.IsNotExist(err) {
			fmt.Printf("module %s does not exist, skipping ...\n", dir)
			continue
		}
		wg.Add(1)
		go func(d, absD string) {
			defer wg.Done()
			outFile := filepath.Join(outDir, outputName(d))
			cmd := exec.Command("apidiff", "-m", "-w", outFile, ".")
			cmd.Dir = absD
			if out, err := cmd.CombinedOutput(); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("apidiff in %s: %w\n%s", d, err, out))
				mu.Unlock()
			}
		}(dir, absDir)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// outputName 将目录路径转换为可安全用作 apidiff 输出文件名的形式。
func outputName(path string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	return re.ReplaceAllString(path, "_") + ".out"
}

// getMergeCommitInfo 从 GitHub 合并提交中提取 PR 标题和描述。
// 提交信息应为 GitHub 生成的格式：首行是 "Merge pull request #<number> from <branch>"，
// 末行是 PR 标题。
func getMergeCommitInfo(commit string) (title, description string, err error) {
	cmd := exec.Command("git", "show", "--no-patch", "--format=%B", commit)
	out, err := cmd.Output()
	if err != nil {
		return "", "", fmt.Errorf("git show %s: %w", commit, err)
	}
	lines := trimTrailingEmpty(strings.Split(strings.TrimRight(string(out), "\n"), "\n"))
	// 首行格式："Merge pull request #<number> from <branch>"。
	words := strings.Fields(lines[0])
	if len(words) < 4 {
		return "", "", fmt.Errorf("unexpected merge commit format: %q", lines[0])
	}
	prNum := strings.TrimPrefix(words[3], "#")
	title = lines[len(lines)-1]
	description = fmt.Sprintf("See [PR #%s](https://github.com/kubernetes/kubernetes/pull/%s).", prNum, prNum)
	return title, description, nil
}

// trimTrailingEmpty 去掉切片末尾的空字符串。
func trimTrailingEmpty(lines []string) []string {
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// compareApidiff 对两个模块状态文件运行 apidiff，向 stdout 输出格式化报告，
// 并返回需要写入 changelog 的不兼容变更（不含容忍项）；没有则返回空字符串。
func compareApidiff(what, beforeFile, afterFile string) (string, error) {
	if _, err := os.Stat(beforeFile); os.IsNotExist(err) {
		fmt.Printf("## %s\ncan not compare changes, module didn't exist before or after\n\n", what)
		return "", nil
	}
	if _, err := os.Stat(afterFile); os.IsNotExist(err) {
		fmt.Printf("## %s\ncan not compare changes, module didn't exist before or after\n\n", what)
		return "", nil
	}

	// apidiff 发现不兼容变更时会以非零状态退出，忽略退出码。
	cmd := exec.Command("apidiff", "-m", beforeFile, afterFile)
	out, _ := cmd.CombinedOutput()

	// 过滤掉 "Ignoring internal package" 行。
	var filtered []string
	for line := range strings.SplitSeq(string(out), "\n") {
		if !strings.HasPrefix(line, "Ignoring internal package") {
			filtered = append(filtered, line)
		}
	}
	filtered = trimTrailingEmpty(filtered)

	fmt.Printf("## %s\n", what)
	if len(filtered) == 0 {
		fmt.Println("no changes")
		return "", nil
	}

	preamble, incompatibleLines, compatibleLines := splitApidiffSections(filtered)

	// 用允许列表过滤不兼容变更。
	incompatibleStr := strings.Join(incompatibleLines, "\n")
	tolerated, err := filterChanges(incompatibleStr, false)
	if err != nil {
		return "", fmt.Errorf("filtering tolerated changes: %w", err)
	}
	incompatibleStr, err = filterChanges(incompatibleStr, true)
	if err != nil {
		return "", fmt.Errorf("filtering incompatible changes: %w", err)
	}

	// 打印前导部分，与 shell 的 `echo "$changes"` 保持一致；即使为空也会输出空行。
	fmt.Println(preamble)
	if incompatibleStr != "" {
		fmt.Println("Incompatible changes:")
		fmt.Print(incompatibleStr)
	}
	if tolerated != "" {
		fmt.Println("Acceptable incompatible changes:")
		fmt.Print(tolerated)
	}
	if len(compatibleLines) > 0 {
		fmt.Println("Compatible changes:")
		fmt.Println(strings.Join(compatibleLines, "\n"))
	}
	fmt.Println()

	return incompatibleStr, nil
}

// splitApidiffSections 将 apidiff 输出按前导、不兼容、兼容三部分拆分，
// 并对各部分内的行排序。
func splitApidiffSections(lines []string) (preamble string, incompatible, compatible []string) {
	// 先找出并提取 "Compatible changes:" 小节。
	for i, line := range lines {
		if line == "Compatible changes:" {
			compatible = append([]string{}, lines[i+1:]...)
			sort.Strings(compatible)
			lines = trimTrailingEmpty(lines[:i])
			break
		}
	}
	// 找出并提取 "Incompatible changes:" 小节。
	for i, line := range lines {
		if line == "Incompatible changes:" {
			incompatible = append([]string{}, lines[i+1:]...)
			sort.Strings(incompatible)
			lines = trimTrailingEmpty(lines[:i])
			break
		}
	}
	preamble = strings.Join(lines, "\n")
	return
}

// filterChanges 按允许列表过滤 API 变更行。
// exclude 为 true 时返回不匹配任何模式的行（真正的不兼容变更）；
// 为 false 时返回至少匹配一个模式的行（可容忍的变更）。
func filterChanges(changes string, exclude bool) (string, error) {
	patterns, err := compileAllowlistPatterns()
	if err != nil {
		return "", err
	}
	var result strings.Builder
	for line := range strings.SplitSeq(changes, "\n") {
		if line == "" {
			continue
		}
		matched := false
		for _, p := range patterns {
			if p.MatchString(line) {
				matched = true
				break
			}
		}
		if matched != exclude {
			result.WriteString(line)
			result.WriteByte('\n')
		}
	}
	return result.String(), nil
}

// compileAllowlistPatterns 编译内嵌允许列表中的正则表达式。
func compileAllowlistPatterns() ([]*regexp.Regexp, error) {
	var patterns []*regexp.Regexp
	for line := range strings.SplitSeq(apiChangesAllowlist, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		re, err := regexp.Compile(line)
		if err != nil {
			return nil, fmt.Errorf("failed to compile regex %q: %w", line, err)
		}
		patterns = append(patterns, re)
	}
	return patterns, nil
}

// verifyChangelog 检查 file 的第一个标题下是否有包含变更的代码块。
// 找不到变更时返回 errVerificationFail。
func verifyChangelog(file, changes string) error {
	if changes == "" {
		return errors.New("no changes specified")
	}
	if changes[len(changes)-1] != '\n' {
		changes += "\n"
	}
	content, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	found, codeBlocks := extractInitialCodeBlocks(string(content))
	if !found {
		return fmt.Errorf("no heading found in changelog")
	}
	expectedLines := strings.Split(changes, "\n")
	for _, block := range codeBlocks {
		if matchesWithWildcard(strings.Split(block, "\n"), expectedLines) {
			return nil
		}
	}
	return errVerificationFail
}

// extractInitialCodeBlocks 解析 Markdown，提取第一个小节（以 # 开头的行标识）中的代码块。
func extractInitialCodeBlocks(content string) (bool, []string) {
	lines := strings.Split(content, "\n")
	var codeBlocks []string
	var haveFirstHeading bool
	var inFirstSection bool
	var firstHeadingLevel int
	var inCodeBlock bool
	var currentCodeBlock strings.Builder

	for _, line := range lines {
		if strings.HasPrefix(line, "#") {
			level := 0
			for i := 0; i < len(line) && line[i] == '#'; i++ {
				level++
			}
			if !haveFirstHeading {
				haveFirstHeading = true
				firstHeadingLevel = level
				inFirstSection = true
				continue
			}
			if level <= firstHeadingLevel {
				break
			}
		}
		if inFirstSection {
			if strings.HasPrefix(line, "```") {
				if inCodeBlock {
					codeBlocks = append(codeBlocks, currentCodeBlock.String())
					currentCodeBlock.Reset()
					inCodeBlock = false
				} else {
					inCodeBlock = true
				}
			} else if inCodeBlock {
				currentCodeBlock.WriteString(line)
				currentCodeBlock.WriteString("\n")
			}
		}
	}
	return haveFirstHeading, codeBlocks
}

// matchesWithWildcard 检查 text 是否匹配 pattern；pattern 中单独一行 "..." 可匹配任意行数。
func matchesWithWildcard(pattern, text []string) bool {
	pi, ti := 0, 0
	for pi < len(pattern) && ti < len(text) {
		if strings.TrimSpace(pattern[pi]) == "..." {
			pi++
			if pi >= len(pattern) {
				return true
			}
			for ti < len(text) {
				if matchesWithWildcard(pattern[pi:], text[ti:]) {
					return true
				}
				ti++
			}
			return false
		}
		if pattern[pi] != text[ti] {
			return false
		}
		pi++
		ti++
	}
	for pi < len(pattern) && strings.TrimSpace(pattern[pi]) == "..." {
		pi++
	}
	return pi == len(pattern) && ti == len(text)
}

// insertChangelog 向 file 插入一个新标题，其中包含记录变更的代码块。
func insertChangelog(file, changes, title, description string) error {
	if changes == "" {
		return errors.New("no changes specified")
	}
	if changes[len(changes)-1] != '\n' {
		changes += "\n"
	}
	content, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	newContent, err := insertHeading(content, changes, title, description)
	if err != nil {
		return err
	}
	if err := os.WriteFile(file, []byte(newContent), 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return nil
}

// insertHeading 创建三级标题，附带模板描述和记录变更的代码块。
// 文件没有标题时追加到末尾，否则插入第一个标题之前。
func insertHeading(content []byte, changes, title, description string) (string, error) {
	lines := string(content)
	if !strings.HasSuffix(changes, "\n") {
		changes += "\n"
	}
	newHeading := `### ` + title + `

` + description + `

` + "```" + `
` + changes +
		"```" + `
`
	firstHeading := headingRE.FindIndex(content)
	if firstHeading == nil {
		return lines + "\n" + newHeading, nil
	}
	firstHeadingPos := firstHeading[0]
	return lines[:firstHeadingPos] + newHeading + "\n" + lines[firstHeadingPos:], nil
}

var headingRE = regexp.MustCompile(`(?m)^#`)
