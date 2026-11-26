package utils

import (
	"os"
	"path/filepath"
)

// CleanPath 让路径可以安全地用于 filepath.Join：除常规清理外，还会给相对路径临时加上
// "/" 再清理并去掉该前缀。这样拼接到另一路径之后，从词法上必定仍是该路径的子目录。
// 整个过程只做词法处理，因此含符号链接的路径不能因此获得安全性。
func CleanPath(path string) string {
	// 空字符串直接返回。
	if path == "" {
		return ""
	}

	// 先统一清理路径，避免 "/../../../../../" 这类形式引发问题。
	path = filepath.Clean(path)

	// 相对路径（如 "../../../../<etc>/some/path"）需额外处理；绝对路径不能转成相对路径。
	if !filepath.IsAbs(path) {
		path = filepath.Clean(string(os.PathSeparator) + path)
		// 按构造这些路径都相对根目录，此处不会失败。
		path, _ = filepath.Rel(string(os.PathSeparator), path)
	}

	// 再做一次清理，确保结果规范。
	return filepath.Clean(path)
}
