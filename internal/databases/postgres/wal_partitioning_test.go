package postgres

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

const (
	partitioningTestWal       = "00000001000000000000007C"
	partitioningTestPartition = "0000000100000000"
)

func setPartitioning(t *testing.T, on bool) {
	t.Helper()
	prev := viper.GetBool(conf.PgWalPartitioning)
	viper.Set(conf.PgWalPartitioning, on)
	t.Cleanup(func() { viper.Set(conf.PgWalPartitioning, prev) })
}

func TestWalPartitionSubdirectory(t *testing.T) {
	assert.Equal(t, partitioningTestPartition, walPartitionSubdirectory(partitioningTestWal))
	// Delta filenames share the same partition as their underlying WAL segment.
	assert.Equal(t, partitioningTestPartition, walPartitionSubdirectory(partitioningTestWal+"_delta"))
}

func TestPartitionedUploaderFor_Disabled(t *testing.T) {
	setPartitioning(t, false)
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	u := internal.NewRegularUploader(nil, folder)

	got := partitionedUploaderFor(u, partitioningTestWal)
	assert.Same(t, internal.Uploader(u), got, "uploader should be returned unchanged when flag is off")
}

func TestPartitionedUploaderFor_Enabled(t *testing.T) {
	setPartitioning(t, true)
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	u := internal.NewRegularUploader(nil, folder)

	got := partitionedUploaderFor(u, partitioningTestWal)
	assert.NotSame(t, internal.Uploader(u), got, "uploader should be cloned when flag is on")
	assert.Contains(t, got.Folder().GetPath(), partitioningTestPartition,
		"cloned uploader's folder path should include the partition subdir")
	assert.NotContains(t, u.Folder().GetPath(), partitioningTestPartition,
		"original uploader's folder must not be mutated")
}

// downloadWalFileWithPartitionFallback tests use bare-named objects (no
// compression suffix); findDecompressorAndDownload's final fall-through
// returns them as-is, which is enough to verify partition-vs-flat routing.

func TestDownloadWalFileWithPartitionFallback_DisabledOnlyConsultsFlat(t *testing.T) {
	setPartitioning(t, false)
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	expected := []byte("flat-payload")
	assert.NoError(t, folder.PutObject(partitioningTestWal, bytes.NewReader(expected)))
	// A different blob in the partition subdir would be wrongly preferred if
	// the reader probed there despite the flag being off.
	partitionFolder := folder.GetSubFolder(partitioningTestPartition)
	assert.NoError(t, partitionFolder.PutObject(partitioningTestWal, bytes.NewReader([]byte("partition-payload"))))

	dst := filepath.Join(t.TempDir(), "out")
	err := downloadWalFileWithPartitionFallback(internal.NewFolderReader(folder), partitioningTestWal, dst)
	assert.NoError(t, err)
	got, err := os.ReadFile(dst)
	assert.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestDownloadWalFileWithPartitionFallback_PartitionHit(t *testing.T) {
	setPartitioning(t, true)
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	expected := []byte("partition-payload")
	partitionFolder := folder.GetSubFolder(partitioningTestPartition)
	assert.NoError(t, partitionFolder.PutObject(partitioningTestWal, bytes.NewReader(expected)))

	dst := filepath.Join(t.TempDir(), "out")
	err := downloadWalFileWithPartitionFallback(internal.NewFolderReader(folder), partitioningTestWal, dst)
	assert.NoError(t, err)
	got, err := os.ReadFile(dst)
	assert.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestDownloadWalFileWithPartitionFallback_PartitionMissFallsBackToFlat(t *testing.T) {
	setPartitioning(t, true)
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	expected := []byte("flat-payload")
	assert.NoError(t, folder.PutObject(partitioningTestWal, bytes.NewReader(expected)))
	// Nothing in the partition subdir — fallback should kick in.

	dst := filepath.Join(t.TempDir(), "out")
	err := downloadWalFileWithPartitionFallback(internal.NewFolderReader(folder), partitioningTestWal, dst)
	assert.NoError(t, err)
	got, err := os.ReadFile(dst)
	assert.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestDownloadWalFileWithPartitionFallback_NotFoundEverywhere(t *testing.T) {
	setPartitioning(t, true)
	folder := memory.NewFolder("in_memory/", memory.NewKVS())

	dst := filepath.Join(t.TempDir(), "out")
	err := downloadWalFileWithPartitionFallback(internal.NewFolderReader(folder), partitioningTestWal, dst)
	assert.Error(t, err)
}

func TestDownloadStorageFileWithPartitionFallback_PartitionHit(t *testing.T) {
	setPartitioning(t, true)
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	deltaName := partitioningTestWal + "_delta"
	expected := []byte("delta-payload")
	partitionFolder := folder.GetSubFolder(partitioningTestPartition)
	assert.NoError(t, partitionFolder.PutObject(deltaName, bytes.NewReader(expected)))

	rc, err := downloadStorageFileWithPartitionFallback(internal.NewFolderReader(folder), deltaName)
	assert.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	assert.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestDownloadStorageFileWithPartitionFallback_PartitionMissFallsBackToFlat(t *testing.T) {
	setPartitioning(t, true)
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	deltaName := partitioningTestWal + "_delta"
	expected := []byte("delta-payload")
	assert.NoError(t, folder.PutObject(deltaName, bytes.NewReader(expected)))

	rc, err := downloadStorageFileWithPartitionFallback(internal.NewFolderReader(folder), deltaName)
	assert.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	assert.NoError(t, err)
	assert.Equal(t, expected, got)
}
