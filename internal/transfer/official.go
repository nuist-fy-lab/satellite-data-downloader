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
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
)

// OfficialChecksum is a file-level hash supplied by an authoritative
// data-provider API. Values are normalized to lowercase hex; in particular,
// an HTTP ETag is never silently interpreted as an MD5 digest.
type OfficialChecksum struct {
	Algorithm string
	Value     string
}

type OfficialResolver struct {
	client *http.Client
	cmr    string
	gcs    string
	browse string
}

func NewOfficialResolver() *OfficialResolver {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Public metadata lookups do not use the production data access proxy.
	transport.Proxy = nil
	transport.MaxIdleConnsPerHost = 8
	return &OfficialResolver{
		client: &http.Client{Timeout: 8 * time.Second, Transport: transport},
		cmr:    "https://cmr.earthdata.nasa.gov",
		gcs:    "https://storage.googleapis.com",
		browse: "https://api.eumetsat.int",
	}
}

var cmrConcept = regexp.MustCompile(`^G[0-9]+-[A-Z0-9_]+$`)

// Resolve only queries known, fixed public metadata endpoints. This allows
// thousands of already-queued tasks to acquire checksums without re-discovery
// or changing their authentication credentials and download concurrency.
// A nil result means the authoritative response contained no checksum.
func (r *OfficialResolver) Resolve(ctx context.Context, task model.Task) ([]OfficialChecksum, error) {
	switch task.SourceID {
	case "modis", "viirs", "calipso":
		id, ok := strings.CutPrefix(task.GranuleID, task.SourceID+":")
		if !ok || !cmrConcept.MatchString(id) {
			return nil, fmt.Errorf("invalid CMR granule id for official checksum")
		}
		return r.cmrChecksums(ctx, id, task.Name)
	case "goes":
		return r.goesChecksums(ctx, task.URL)
	case "mtg":
		return r.mtgChecksums(ctx, task.CollectionID, task.GranuleID)
	case "himawari9":
		return r.s3Checksums(ctx, task.URL)
	default:
		return nil, nil
	}
}

func (r *OfficialResolver) loadJSON(ctx context.Context, endpoint string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	response, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("official checksum metadata returned HTTP %d", response.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(target)
}

func (r *OfficialResolver) cmrChecksums(ctx context.Context, concept, filename string) ([]OfficialChecksum, error) {
	var response struct {
		Items []struct {
			UMM struct {
				DataGranule struct {
					Archives []struct {
						Name     string `json:"Name"`
						Checksum struct {
							Algorithm string `json:"Algorithm"`
							Value     string `json:"Value"`
						} `json:"Checksum"`
					} `json:"ArchiveAndDistributionInformation"`
				} `json:"DataGranule"`
			} `json:"umm"`
		} `json:"items"`
	}
	endpoint := r.cmr + "/search/granules.umm_json?concept_id=" + url.QueryEscape(concept)
	if err := r.loadJSON(ctx, endpoint, &response); err != nil {
		return nil, err
	}
	if len(response.Items) != 1 {
		return nil, fmt.Errorf("CMR expected one granule %s, got %d", concept, len(response.Items))
	}
	archives := response.Items[0].UMM.DataGranule.Archives
	var matches []OfficialChecksum
	for _, record := range archives {
		if record.Name != filename && !(len(archives) == 1 && (record.Name == "" || record.Name == "Not provided")) {
			continue
		}
		if record.Checksum.Value == "" {
			continue
		}
		check, err := parseOfficialHex(record.Checksum.Algorithm, record.Checksum.Value)
		if err != nil {
			return nil, err
		}
		matches = append(matches, check)
	}
	return matches, nil
}

func (r *OfficialResolver) goesChecksums(ctx context.Context, raw string) ([]OfficialChecksum, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() != "storage.googleapis.com" || u.User != nil {
		return nil, errors.New("invalid GOES checksum object URL")
	}
	parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	if len(parts) != 2 || !strings.HasPrefix(parts[0], "gcp-public-data-goes-") || parts[1] == "" || u.RawQuery != "" {
		return nil, errors.New("invalid GOES object bucket or key")
	}
	var metadata struct {
		MD5    string `json:"md5Hash"`
		CRC32C string `json:"crc32c"`
	}
	endpoint := r.gcs + "/storage/v1/b/" + url.PathEscape(parts[0]) + "/o/" + url.PathEscape(parts[1])
	if err := r.loadJSON(ctx, endpoint, &metadata); err != nil {
		return nil, err
	}
	var checks []OfficialChecksum
	for _, item := range []struct{ algorithm, value string }{{"MD5", metadata.MD5}, {"CRC32C", metadata.CRC32C}} {
		if item.value == "" {
			continue
		}
		check, err := parseOfficialBase64(item.algorithm, item.value)
		if err != nil {
			return nil, err
		}
		checks = append(checks, check)
	}
	return checks, nil
}

func (r *OfficialResolver) mtgChecksums(ctx context.Context, collectionID, granuleID string) ([]OfficialChecksum, error) {
	collection, ok := strings.CutPrefix(collectionID, "mtg:")
	if !ok || !regexp.MustCompile(`^EO:EUM:DAT:[0-9]+$`).MatchString(collection) {
		return nil, errors.New("invalid MTG product collection for checksum")
	}
	product, ok := strings.CutPrefix(granuleID, "mtg:")
	if !ok || product == "" || strings.ContainsAny(product, "/\\") {
		return nil, errors.New("invalid MTG product id for checksum")
	}
	var metadata struct {
		Properties struct {
			Extra struct {
				MD5 string `json:"md5"`
			} `json:"extraInformation"`
		} `json:"properties"`
	}
	endpoint := r.browse + "/data/browse/collections/" + url.PathEscape(collection) + "/products/" + url.PathEscape(product) + "?format=json"
	if err := r.loadJSON(ctx, endpoint, &metadata); err != nil {
		return nil, err
	}
	if metadata.Properties.Extra.MD5 == "" {
		return nil, nil
	}
	check, err := parseOfficialHex("MD5", metadata.Properties.Extra.MD5)
	if err != nil {
		return nil, err
	}
	return []OfficialChecksum{check}, nil
}

func (r *OfficialResolver) s3Checksums(ctx context.Context, raw string) ([]OfficialChecksum, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() != "noaa-himawari9.s3.amazonaws.com" || u.User != nil || u.RawQuery != "" || path.Clean(u.Path) == "/" {
		return nil, errors.New("invalid Himawari-9 checksum object URL")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, raw, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-amz-checksum-mode", "ENABLED")
	response, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("S3 checksum HEAD returned HTTP %d", response.StatusCode)
	}
	var found []OfficialChecksum
	for _, entry := range []struct{ algorithm, header string }{
		{"CRC32", "x-amz-checksum-crc32"}, {"CRC32C", "x-amz-checksum-crc32c"},
		{"SHA1", "x-amz-checksum-sha1"}, {"SHA256", "x-amz-checksum-sha256"},
	} {
		value := response.Header.Get(entry.header)
		if value == "" {
			continue
		}
		if response.Header.Get("x-amz-checksum-type") != "FULL_OBJECT" {
			// A composite digest cannot be compared against a local full-file hash.
			continue
		}
		check, err := parseOfficialBase64(entry.algorithm, value)
		if err != nil {
			return nil, err
		}
		found = append(found, check)
	}
	return found, nil
}

func checksumHash(algorithm string) (hash.Hash, int, error) {
	switch strings.ToUpper(strings.ReplaceAll(algorithm, "-", "")) {
	case "MD5":
		return md5.New(), md5.Size, nil
	case "SHA1":
		return sha1.New(), sha1.Size, nil
	case "SHA256":
		return sha256.New(), sha256.Size, nil
	case "CRC32":
		return crc32.NewIEEE(), 4, nil
	case "CRC32C":
		return crc32.New(crc32.MakeTable(crc32.Castagnoli)), 4, nil
	default:
		return nil, 0, fmt.Errorf("unsupported official checksum algorithm %q", algorithm)
	}
}

func parseOfficialHex(algorithm, value string) (OfficialChecksum, error) {
	_, length, err := checksumHash(algorithm)
	if err != nil {
		return OfficialChecksum{}, err
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(value))
	if err != nil || len(decoded) != length {
		return OfficialChecksum{}, fmt.Errorf("invalid official %s checksum", algorithm)
	}
	return OfficialChecksum{Algorithm: strings.ToUpper(strings.ReplaceAll(algorithm, "-", "")), Value: hex.EncodeToString(decoded)}, nil
}

func parseOfficialBase64(algorithm, value string) (OfficialChecksum, error) {
	_, length, err := checksumHash(algorithm)
	if err != nil {
		return OfficialChecksum{}, err
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil || len(decoded) != length {
		return OfficialChecksum{}, fmt.Errorf("invalid official %s checksum encoding", algorithm)
	}
	return OfficialChecksum{Algorithm: algorithm, Value: hex.EncodeToString(decoded)}, nil
}

// sha256AndVerifyOfficial hashes the file once for both local SHA-256 and all
// official checksums. A mismatch blocks archival promotion.
func sha256AndVerifyOfficial(filename string, checks []OfficialChecksum) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()
	sha := sha256.New()
	writers := []io.Writer{sha}
	verification := make([]hash.Hash, 0, len(checks))
	for _, check := range checks {
		h, _, err := checksumHash(check.Algorithm)
		if err != nil {
			return "", err
		}
		verification = append(verification, h)
		writers = append(writers, h)
	}
	if _, err := io.CopyBuffer(io.MultiWriter(writers...), file, make([]byte, 1<<20)); err != nil {
		return "", err
	}
	for i, check := range checks {
		if hex.EncodeToString(verification[i].Sum(nil)) != strings.ToLower(check.Value) {
			return "", &IntegrityError{Detail: fmt.Sprintf("official %s checksum mismatch for %s", check.Algorithm, filepath.Base(filename))}
		}
	}
	return hex.EncodeToString(sha.Sum(nil)), nil
}
