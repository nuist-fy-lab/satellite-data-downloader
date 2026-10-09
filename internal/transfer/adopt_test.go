package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
)

func TestVerifiedArchiveReuseChecksPathAndActualSHA256(t *testing.T) {
	root := t.TempDir()
	spec := model.TaskSpec{Name: "cloud.hdf", SourceID: "modis",
		CollectionID: "modis:C123", GranuleID: "modis:G123", AssetID: "asset",
		RelativeDir: "modis/Aqua/MYD06/2023/05/03"}
	archive := filepath.Join(root, filepath.FromSlash(spec.RelativeDir), spec.Name)
	if err := os.MkdirAll(filepath.Dir(archive), 0755); err != nil {
		t.Fatal(err)
	}
	content := []byte("a verified archive that must not be re-downloaded")
	if err := os.WriteFile(archive, content, 0600); err != nil {
		t.Fatal(err)
	}
	sha := sha256.Sum256(content)
	digest := hex.EncodeToString(sha[:])
	spec.Size = int64(len(content))
	path, n, err := VerifiedArchiveReuse(spec, root, archive, digest)
	if err != nil || n != spec.Size || path != archive {
		t.Fatalf("valid archive rejected: %s %d %v", path, n, err)
	}
	if _, _, err := VerifiedArchiveReuse(spec, root, archive, hex.EncodeToString(make([]byte, 32))); err == nil {
		t.Fatal("mismatching sha256 wrongly adopted")
	}
	if _, _, err := VerifiedArchiveReuse(spec, root, filepath.Join(root, "elsewhere.hdf"), digest); err == nil {
		t.Fatal("wrong archive path wrongly adopted")
	}
	if err := os.Remove(archive); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "somewhere"), archive); err != nil {
		t.Fatal(err)
	}
	if _, _, err := VerifiedArchiveReuse(spec, root, archive, digest); err == nil {
		t.Fatal("symlink must never be adopted")
	}
}
