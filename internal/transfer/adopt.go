package transfer

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
)

// VerifiedArchiveReuse permits reuse of a file already registered by the
// centre's *verified* archive index, without staging a second download.
// The local file is hashed again: filename, catalogue size or ETag alone
// never constitute integrity evidence.
func VerifiedArchiveReuse(spec model.TaskSpec, archiveRoot, indexedPath, digest string) (string, int64, error) {
	name, err := safeName(spec.Name)
	if err != nil {
		return "", 0, err
	}
	rel, err := safeRelativeDir(spec.RelativeDir)
	if err != nil {
		return "", 0, err
	}
	if len(digest) != 64 {
		return "", 0, errors.New("indexed SHA-256 is missing or invalid")
	}
	for _, r := range digest {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return "", 0, errors.New("invalid indexed SHA-256 encoding")
		}
	}
	root, err := filepath.Abs(archiveRoot)
	if err != nil {
		return "", 0, err
	}
	expected := filepath.Join(root, rel, name)
	actual, err := filepath.Abs(indexedPath)
	if err != nil || filepath.Clean(actual) != expected {
		return "", 0, errors.New("indexed archive path does not match target product directory")
	}
	info, err := os.Lstat(expected)
	if err != nil {
		return "", 0, err
	}
	if !info.Mode().IsRegular() {
		return "", 0, errors.New("indexed archive is not a regular file")
	}
	if spec.Size > 0 && info.Size() != spec.Size {
		// CMR/LAADS catalogue sizes have sometimes been inaccurate. Do not
		// trust them as proof of corruption; the indexed SHA-256 is mandatory.
		// Nonetheless reject changed metadata, which requires a new archive
		// scan before the centre can vouch for the new bytes.
		return "", 0, fmt.Errorf("indexed archive size %d disagrees with official metadata %d", info.Size(), spec.Size)
	}
	actualDigest, err := fileSHA256(expected)
	if err != nil {
		return "", 0, err
	}
	if actualDigest != digest {
		return "", 0, &IntegrityError{Detail: "local archive SHA-256 does not match verified index"}
	}
	return expected, info.Size(), nil
}
