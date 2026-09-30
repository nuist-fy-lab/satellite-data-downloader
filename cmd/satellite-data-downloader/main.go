package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/access"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/config"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/pipeline"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/server"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/store"
)

func main() {
	cfg := config.Load()
	for _, path := range []string{filepath.Dir(cfg.SocketPath), filepath.Dir(cfg.DBPath), cfg.DownloadRoot} {
		if err := os.MkdirAll(path, 0o750); err != nil {
			slog.Error("create runtime directory", "path", path, "error", err)
			os.Exit(1)
		}
	}
	archiveMode := os.FileMode(0o750)
	if cfg.ArchiveGroupWritable {
		archiveMode = 0o777
	}
	if err := os.MkdirAll(cfg.ArchiveRoot, archiveMode); err != nil {
		slog.Error("create archive directory", "path", cfg.ArchiveRoot, "error", err)
		os.Exit(1)
	}
	state, err := store.Open(cfg.DBPath)
	if err != nil {
		slog.Error("open downloader state", "error", err)
		os.Exit(1)
	}
	defer state.Close()
	if err := state.Recover(context.Background()); err != nil {
		slog.Error("recover downloader queue", "error", err)
		os.Exit(1)
	}
	accessManager, err := access.NewManager(cfg.CentralSocket)
	if err != nil {
		slog.Error("initialize access manager", "error", err)
		os.Exit(1)
	}
	runtime, err := pipeline.New(cfg, state, accessManager)
	if err != nil {
		slog.Error("initialize provider pipelines", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	runtime.Run(ctx)

	listener, err := listenUnix(cfg.SocketPath)
	if err != nil {
		slog.Error("listen on downloader socket", "path", cfg.SocketPath, "error", err)
		os.Exit(1)
	}
	defer listener.Close()
	defer os.Remove(cfg.SocketPath)
	server := &http.Server{
		Handler:           server.New(state, runtime),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	slog.Info("satellite downloader listening", "socket", cfg.SocketPath)
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("downloader server stopped", "error", err)
		os.Exit(1)
	}
}

func listenUnix(path string) (net.Listener, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("downloader socket path exists and is not a Unix socket")
		}
		conn, dialErr := net.DialTimeout("unix", path, 300*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			return nil, errors.New("another downloader process is already listening")
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o660); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}
