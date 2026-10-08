package transfer

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
)

const checksumFixture = "science granule content from trusted dataset"

func officialTestResolver(t *testing.T, handler http.HandlerFunc) *OfficialResolver {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	return &OfficialResolver{client: server.Client(), cmr: server.URL, gcs: server.URL, browse: server.URL}
}

func TestOfficialChecksumAPIsWithRealMetadataShapes(t *testing.T) {
	md5sum := md5.Sum([]byte(checksumFixture))
	sha := sha256.Sum256([]byte(checksumFixture))
	crc := crc32.Checksum([]byte(checksumFixture), crc32.MakeTable(crc32.Castagnoli))
	crcBytes := []byte{byte(crc >> 24), byte(crc >> 16), byte(crc >> 8), byte(crc)}
	resolver := officialTestResolver(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/search/granules.umm_json"):
			if r.URL.Query().Get("concept_id") != "G123-LAADS" && r.URL.Query().Get("concept_id") != "G456-LARC_CLOUD" {
				t.Error("unrecognized CMR granule id", r.URL.String())
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"umm": map[string]any{"DataGranule": map[string]any{"ArchiveAndDistributionInformation": []any{
				map[string]any{"Name": "cloud.hdf", "Checksum": map[string]string{"Algorithm": "MD5", "Value": hex.EncodeToString(md5sum[:])}},
				map[string]any{"Name": "cloud.hdf.met", "Checksum": map[string]string{"Algorithm": "MD5", "Value": "22222222222222222222222222222222"}},
			}}}}}})
		case strings.HasPrefix(r.URL.Path, "/storage/v1/b/"):
			_ = json.NewEncoder(w).Encode(map[string]string{"md5Hash": base64.StdEncoding.EncodeToString(md5sum[:]), "crc32c": base64.StdEncoding.EncodeToString(crcBytes)})
		case strings.HasPrefix(r.URL.Path, "/data/browse/collections/"):
			_ = json.NewEncoder(w).Encode(map[string]any{"properties": map[string]any{"extraInformation": map[string]string{"md5": hex.EncodeToString(md5sum[:])}}})
		case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/AHI-L1b"):
			if r.Header.Get("x-amz-checksum-mode") != "ENABLED" {
				t.Error("missing S3 checksum-mode header")
			}
			w.Header().Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(sha[:]))
			w.Header().Set("x-amz-checksum-type", "FULL_OBJECT")
		case r.Method == http.MethodHead && r.URL.Path == "/composite":
			w.Header().Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(sha[:]))
			w.Header().Set("x-amz-checksum-type", "COMPOSITE")
		default:
			t.Errorf("unexpected metadata request %s %s", r.Method, r.URL)
		}
	})
	for _, tc := range []struct {
		name string
		task model.Task
		want int
	}{
		{"MODIS_CMR", model.Task{TaskSpec: model.TaskSpec{SourceID: "modis", GranuleID: "modis:G123-LAADS", Name: "cloud.hdf"}}, 1},
		{"VIIRS_CMR", model.Task{TaskSpec: model.TaskSpec{SourceID: "viirs", GranuleID: "viirs:G123-LAADS", Name: "cloud.hdf"}}, 1},
		{"CALIPSO_file_specific", model.Task{TaskSpec: model.TaskSpec{SourceID: "calipso", GranuleID: "calipso:G456-LARC_CLOUD", Name: "cloud.hdf"}}, 1},
		{"GOES_MD5_CRC32C", model.Task{TaskSpec: model.TaskSpec{SourceID: "goes", URL: "https://storage.googleapis.com/gcp-public-data-goes-18/ABI-L2-CMIPF/2023/file.nc"}}, 2},
		{"MTG_Browse_MD5", model.Task{TaskSpec: model.TaskSpec{SourceID: "mtg", GranuleID: "mtg:FCI-product", CollectionID: "mtg:EO:EUM:DAT:0678"}}, 1},
		{"Himawari9_FULL_OBJECT", model.Task{TaskSpec: model.TaskSpec{SourceID: "himawari9", URL: "https://noaa-himawari9.s3.amazonaws.com/AHI-L1b-FLDK/file.dat"}}, 1},
		{"Himawari8_ETag_not_MD5", model.Task{TaskSpec: model.TaskSpec{SourceID: "himawari8", URL: "https://noaa-himawari8.s3.amazonaws.com/file.dat"}}, 0},
		{"EarthCARE_no_checksum", model.Task{TaskSpec: model.TaskSpec{SourceID: "earthcare"}}, 0},
		{"CloudSat_no_checksum", model.Task{TaskSpec: model.TaskSpec{SourceID: "cloudsat"}}, 0},
		{"ERA5_no_checksum", model.Task{TaskSpec: model.TaskSpec{SourceID: "era5"}}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := *resolver
			if tc.task.SourceID == "himawari9" {
				// Substitute a local TLS test target while keeping the source's
				// original hostname in the validation path.
				r.client = &http.Client{Transport: &replaceHostTransport{target: resolver.client.Transport, host: strings.TrimPrefix(resolver.cmr, "https://")}}
			}
			checks, err := r.Resolve(t.Context(), tc.task)
			if err != nil {
				t.Fatal(err)
			}
			if len(checks) != tc.want {
				t.Fatalf("checksum count=%d expected=%d: %+v", len(checks), tc.want, checks)
			}
			if len(checks) > 0 {
				p := filepath.Join(t.TempDir(), "file")
				if err := os.WriteFile(p, []byte(checksumFixture), 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := sha256AndVerifyOfficial(p, checks); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	// The .met checksum is never accidentally matched to the HDF itself.
	met, err := resolver.Resolve(t.Context(), model.Task{TaskSpec: model.TaskSpec{SourceID: "calipso", GranuleID: "calipso:G456-LARC_CLOUD", Name: "cloud.hdf.met"}})
	if err != nil || len(met) != 1 || met[0].Value != "22222222222222222222222222222222" {
		t.Fatalf("bad CALIPSO filename match: %v %v", met, err)
	}
}

type replaceHostTransport struct {
	target http.RoundTripper
	host   string
}

func (r *replaceHostTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	copy.URL.Host = r.host
	return r.target.RoundTrip(copy)
}

func TestOfficialMismatchBlocksArchiveAndUsesTrueFullObjectHashes(t *testing.T) {
	data := []byte(checksumFixture)
	md5sum := md5.Sum(data)
	sha1sum := sha1.Sum(data)
	sha256sum := sha256.Sum256(data)
	crc := crc32.ChecksumIEEE(data)
	crcBytes := []byte{byte(crc >> 24), byte(crc >> 16), byte(crc >> 8), byte(crc)}
	checks := []OfficialChecksum{
		{Algorithm: "MD5", Value: hex.EncodeToString(md5sum[:])},
		{Algorithm: "SHA1", Value: hex.EncodeToString(sha1sum[:])},
		{Algorithm: "SHA256", Value: hex.EncodeToString(sha256sum[:])},
		{Algorithm: "CRC32", Value: hex.EncodeToString(crcBytes)},
	}
	root := t.TempDir()
	stage := filepath.Join(root, "staging")
	if err := os.WriteFile(stage, data, 0600); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(root, "archive")
	if err := os.MkdirAll(archive, 0750); err != nil {
		t.Fatal(err)
	}
	task := model.Task{ID: 1, TaskSpec: model.TaskSpec{Name: "file.hdf", RelativeDir: "modis/Aqua/2023"}, StagingPath: stage}
	wrong := append([]OfficialChecksum(nil), checks...)
	wrong[0].Value = "00000000000000000000000000000000"
	_, err := PromoteWithOfficial(task, archive, wrong)
	var integrity *IntegrityError
	if !errors.As(err, &integrity) || !strings.Contains(err.Error(), "official MD5") {
		t.Fatalf("wrong official checksum accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(archive, "modis/Aqua/2023/file.hdf")); !os.IsNotExist(err) {
		t.Fatal("corrupt content promoted before official verification")
	}
	result, err := PromoteWithOfficial(task, archive, checks)
	if err != nil || result.OfficialStatus != "verified" || result.OfficialAlgorithm != "MD5,SHA1,SHA256,CRC32" {
		t.Fatalf("verified promotion not recorded: %+v err=%v", result, err)
	}
	if len(result.SHA256) != 64 {
		t.Fatalf("missing mandatory local SHA256: %+v", result)
	}
}

func TestCompositeChecksumsAndUnknownAlgorithmsNeverClaimSuccess(t *testing.T) {
	resolver := officialTestResolver(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-amz-checksum-type", "COMPOSITE")
		w.Header().Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	})
	resolver.client = &http.Client{Transport: &replaceHostTransport{target: resolver.client.Transport, host: strings.TrimPrefix(resolver.cmr, "https://")}}
	checks, err := resolver.Resolve(context.Background(), model.Task{TaskSpec: model.TaskSpec{SourceID: "himawari9", URL: "https://noaa-himawari9.s3.amazonaws.com/composite"}})
	if err != nil || len(checks) != 0 {
		t.Fatalf("S3 composite digest treated as full-file checksum: %v %v", checks, err)
	}
	if _, err := parseOfficialHex("CUSTOM", "012345"); err == nil {
		t.Fatal("unsupported algorithm accepted")
	}
	if _, err := parseOfficialBase64("MD5", "not-base64"); err == nil {
		t.Fatal("invalid checksum encoding accepted")
	}
}
