package server

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/pipeline"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/store"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/transfer"
)

type Server struct {
	store    *store.Store
	pipeline *pipeline.Runtime
}

func New(state *store.Store, runtime *pipeline.Runtime) http.Handler {
	s := &Server{store: state, pipeline: runtime}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /v1/providers", s.providers)
	mux.HandleFunc("POST /v1/tasks", s.enqueue)
	mux.HandleFunc("POST /v1/tasks/adopt-verified", s.adoptVerified)
	mux.HandleFunc("GET /v1/tasks", s.tasks)
	mux.HandleFunc("POST /v1/tasks/status", s.taskStatuses)
	mux.HandleFunc("GET /v1/tasks/{id}", s.task)
	mux.HandleFunc("POST /v1/tasks/{id}/retry", s.retry)
	mux.HandleFunc("POST /v1/tasks/{id}/archive-audit", s.archiveAudit)
	for _, path := range []string{"/v1/access", "/v1/credentials", "/v1/credentials/{id}", "/v1/proxies", "/v1/proxies/{id}", "/v1/accounts", "/v1/accounts/{id}"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			writeError(w, http.StatusGone, errors.New("access management moved to satellite-data centre"))
		})
	}
	return withHeaders(mux)
}

// Only the centre's local Unix socket can request this operation. SHA-256
// is independently recomputed from disk before any download task is marked
// complete. A conflicting in-flight task is never cancelled or overwritten.
func (s *Server) adoptVerified(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Task        model.TaskSpec `json:"task"`
		ArchivePath string         `json:"archive_path"`
		SHA256      string         `json:"sha256"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(input.Task.Key) == "" {
		input.Task.Key = taskKey(input.Task)
	}
	if err := s.pipeline.ValidateTask(input.Task); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if existing, err := s.store.TaskByKey(r.Context(), input.Task.Key); err == nil {
		// Overlapping orders do not repeatedly hash an already registered
		// file or replace a currently downloading shared task.
		writeJSON(w, http.StatusOK, map[string]any{"task": existing, "created": false, "reused_local": false})
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	root, err := s.pipeline.ArchiveRootFor(input.Task.SourceID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	path, size, err := transfer.VerifiedArchiveReuse(input.Task, root, input.ArchivePath, input.SHA256)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	task, created, err := s.store.AdoptVerified(r.Context(), input.Task, path, input.SHA256, size)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": task, "created": created, "reused_local": created})
}

// archiveAudit is available only via the downloader's protected local Unix
// socket. Historic checksum Sidecar results are persisted without changing
// any task scheduling or joining the live transfer/verification worker pools.
func (s *Server) archiveAudit(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var payload struct {
		ArchivePath string `json:"archive_path"`
		SHA256      string `json:"sha256"`
		Status      string `json:"status"`
		Algorithm   string `json:"algorithm"`
		Value       string `json:"value"`
	}
	if err := decodeJSON(w, r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.store.RecordArchiveAudit(r.Context(), id, payload.ArchivePath, payload.SHA256,
		payload.Status, payload.Algorithm, payload.Value); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
}

func (s *Server) taskStatuses(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var payload struct {
		IDs []int64 `json:"ids"`
	}
	if err := decoder.Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("invalid task status JSON"))
		return
	}
	items, err := s.store.TasksByIDs(r.Context(), payload.IDs)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": items})
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) providers(w http.ResponseWriter, r *http.Request) {
	items, err := s.pipeline.ProviderStatuses(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"providers": items})
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var spec model.TaskSpec
	if err := decoder.Decode(&spec); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("invalid task JSON"))
		return
	}
	if strings.TrimSpace(spec.Key) == "" {
		spec.Key = taskKey(spec)
	}
	if err := s.pipeline.ValidateTask(spec); err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "disabled") {
			status = http.StatusConflict
		}
		writeError(w, status, err)
		return
	}
	task, created, err := s.store.Enqueue(r.Context(), spec)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"created": created, "task": task})
}

func (s *Server) tasks(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := s.store.Tasks(r.Context(), r.URL.Query().Get("source"), r.URL.Query().Get("state"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": items})
}

func (s *Server) task(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid task id"))
		return
	}
	item, err := s.store.Task(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, errors.New("task not found"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": item})
}

func (s *Server) retry(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid task id"))
		return
	}
	if err := s.store.RetryTask(r.Context(), id); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	item, err := s.store.Task(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": item})
}

func taskKey(spec model.TaskSpec) string {
	hash := sha256.New()
	for _, value := range []string{spec.SourceID, spec.CollectionID, spec.GranuleID, spec.AssetID, spec.Name} {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("invalid request JSON")
	}
	return nil
}

func pathID(value string) (int64, error) {
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("invalid id")
	}
	return id, nil
}

func friendlyPoolError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	switch {
	case strings.Contains(message, "credentials.alias"):
		return errors.New("凭据别名已存在，请编辑已有凭据或使用其他别名")
	case strings.Contains(message, "proxies.alias"):
		return errors.New("代理别名已存在，请编辑已有代理或使用其他别名")
	case strings.Contains(message, "accounts.alias"):
		return errors.New("账号别名已存在，请编辑已有账号或使用其他别名")
	default:
		return err
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func withHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
