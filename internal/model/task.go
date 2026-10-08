package model

import "time"

type TaskSpec struct {
	Key          string `json:"key"`
	SourceID     string `json:"source_id"`
	CollectionID string `json:"collection_id"`
	GranuleID    string `json:"granule_id"`
	AssetID      string `json:"asset_id"`
	Name         string `json:"name"`
	URL          string `json:"url"`
	AuthRef      string `json:"auth_ref,omitempty"`
	RelativeDir  string `json:"relative_dir"`
	Size         int64  `json:"size,omitempty"`
	ETag         string `json:"etag,omitempty"`
	MediaType    string `json:"media_type,omitempty"`
}

type Task struct {
	ID int64 `json:"id"`
	TaskSpec
	State                     string    `json:"state"`
	Attempts                  int       `json:"attempts"`
	NextAttemptAt             time.Time `json:"next_attempt_at,omitempty"`
	ObservedSize              int64     `json:"observed_size,omitempty"`
	SHA256                    string    `json:"sha256,omitempty"`
	OfficialChecksumStatus    string    `json:"official_checksum_status,omitempty"`
	OfficialChecksumAlgorithm string    `json:"official_checksum_algorithm,omitempty"`
	OfficialChecksumValue     string    `json:"official_checksum_value,omitempty"`
	StagingPath               string    `json:"staging_path,omitempty"`
	ArchivePath               string    `json:"archive_path,omitempty"`
	LastError                 string    `json:"last_error,omitempty"`
	RequestedAt               time.Time `json:"requested_at"`
	StartedAt                 time.Time `json:"started_at,omitempty"`
	CompletedAt               time.Time `json:"completed_at,omitempty"`
	UpdatedAt                 time.Time `json:"updated_at"`
}

type ProviderStatus struct {
	SourceID            string         `json:"source_id"`
	Enabled             bool           `json:"enabled"`
	TransferConcurrency int            `json:"transfer_concurrency"`
	VerifyConcurrency   int            `json:"verify_concurrency"`
	AdaptiveConcurrency int            `json:"adaptive_concurrency"`
	ActiveTransfers     int            `json:"active_transfers"`
	BytesPerSecond      float64        `json:"bytes_per_second"`
	AuthRef             string         `json:"auth_ref,omitempty"`
	AuthConfigured      bool           `json:"auth_configured"`
	States              map[string]int `json:"states"`
}
