package postgres

import (
	"errors"
	"io"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
)

// IsWalPartitioningEnabled reports whether WALG_WAL_PARTITIONING is set.
// When enabled, WAL/delta/metadata files are stored under a 16-char
// partition subdirectory (timeline + log segment high bits) — same convention
// as pgbackrest — to avoid hot-prefix throttling on object stores. Readers
// fall back to the flat layout for files written before the flag was flipped.
func IsWalPartitioningEnabled() bool {
	return viper.GetBool(conf.PgWalPartitioning)
}

// walPartitionSubdirectory returns the partition subdirectory for filename.
// Caller must ensure len(filename) >= 16 and that the prefix is hex-only
// (true for WAL files and delta files; not for *.history).
func walPartitionSubdirectory(filename string) string {
	return filename[:16]
}

// partitionedUploaderFor clones u and points it at the partition subdirectory
// derived from filename. Returns u unchanged when partitioning is disabled.
// Caller is responsible for verifying filename is partitionable (typically
// isWalFilename for WAL paths or len >= 16 for delta paths).
func partitionedUploaderFor(u internal.Uploader, filename string) internal.Uploader {
	if !IsWalPartitioningEnabled() {
		return u
	}
	cloned := u.Clone()
	cloned.ChangeDirectory(walPartitionSubdirectory(filename))
	return cloned
}

// isNotFound reports whether err signals the requested object simply wasn't
// there — as opposed to a transient backend error. We only fall back to the
// flat layout on this; other errors propagate so a bad bucket doesn't get
// papered over with a second failing request.
func isNotFound(err error) bool {
	var nf internal.ArchiveNonExistenceError
	return errors.As(err, &nf)
}

// downloadWalFileWithPartitionFallback downloads a WAL file to location.
// When partitioning is enabled it tries the partition subdirectory first
// and falls back to the flat layout on a not-found result; when disabled
// it goes straight to flat, avoiding a useless extra round trip on every fetch.
func downloadWalFileWithPartitionFallback(reader internal.StorageFolderReader, walFileName, location string) error {
	if IsWalPartitioningEnabled() && isWalFilename(walFileName) {
		partitioned := reader.SubFolder(walPartitionSubdirectory(walFileName))
		err := internal.DownloadFileTo(partitioned, walFileName, location)
		if err == nil {
			return nil
		}
		if !isNotFound(err) {
			return err
		}
		tracelog.DebugLogger.Printf("WAL file %s not in partition, trying flat layout", walFileName)
	}
	return internal.DownloadFileTo(reader, walFileName, location)
}

// downloadStorageFileWithPartitionFallback is the decompressing counterpart
// of downloadWalFileWithPartitionFallback for delta files and other inputs
// to the WAL delta reader.
func downloadStorageFileWithPartitionFallback(reader internal.StorageFolderReader, filename string) (io.ReadCloser, error) {
	if IsWalPartitioningEnabled() && len(filename) >= 16 {
		partitioned := reader.SubFolder(walPartitionSubdirectory(filename))
		rc, err := internal.DownloadAndDecompressStorageFile(partitioned, filename)
		if err == nil {
			return rc, nil
		}
		if !isNotFound(err) {
			return nil, err
		}
		tracelog.DebugLogger.Printf("File %s not in partition, trying flat layout", filename)
	}
	return internal.DownloadAndDecompressStorageFile(reader, filename)
}
