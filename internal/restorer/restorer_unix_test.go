//+build !windows

package restorer

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

func TestRestorerRestoreEmptyHardlinkedFileds(t *testing.T) {
	repo, cleanup := repository.TestRepository(t)
	defer cleanup()

	_, id := saveSnapshot(t, repo, Snapshot{
		Nodes: map[string]Node{
			"dirtest": Dir{
				Nodes: map[string]Node{
					"file1": File{Links: 2, Inode: 1},
					"file2": File{Links: 2, Inode: 1},
				},
			},
		},
	})

	res, err := NewRestorer(repo, id)
	rtest.OK(t, err)

	res.SelectFilter = func(item string, dstpath string, node *restic.Node) (selectedForRestore bool, childMayBeSelected bool) {
		return true, true
	}

	tempdir, cleanup := rtest.TempDir(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)

	f1, err := os.Stat(filepath.Join(tempdir, "dirtest/file1"))
	rtest.OK(t, err)
	rtest.Equals(t, int64(0), f1.Size())
	s1, ok1 := f1.Sys().(*syscall.Stat_t)

	f2, err := os.Stat(filepath.Join(tempdir, "dirtest/file2"))
	rtest.OK(t, err)
	rtest.Equals(t, int64(0), f2.Size())
	s2, ok2 := f2.Sys().(*syscall.Stat_t)

	if ok1 && ok2 {
		rtest.Equals(t, s1.Ino, s2.Ino)
	}
}

// Test restoring of sparse files.
//
// This test does not check whether Truncate was ever used, let alone if it
// was successful. Check the coverage report for that information.
func TestRestorerSparseFiles(t *testing.T) {
	repo, cleanup := repository.TestRepository(t)
	defer cleanup()

	var zeros [1 << 20]byte // 1MB

	target := &fs.Reader{
		Mode:       0600,
		Name:       "/zeros",
		ReadCloser: ioutil.NopCloser(bytes.NewReader(zeros[:])),
	}
	sc := archiver.NewScanner(target)
	err := sc.Scan(context.TODO(), []string{"/zeros"})
	rtest.OK(t, err)

	arch := archiver.New(repo, target, archiver.Options{})
	_, id, err := arch.Snapshot(context.Background(), []string{"/zeros"},
		archiver.SnapshotOptions{})

	res, err := NewRestorer(repo, id)
	rtest.OK(t, err)

	tempdir, cleanup := rtest.TempDir(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)

	filename := filepath.Join(tempdir, "zeros")
	content, err := ioutil.ReadFile(filename)
	rtest.OK(t, err)

	rtest.Equals(t, zeros[:], content)

	fi, err := os.Stat(filename)
	rtest.OK(t, err)
	st := fi.Sys().(*syscall.Stat_t)
	if st == nil {
		return
	}

	t.Logf("wrote %d bytes file with %d blocks of size %d",
		len(zeros), st.Blocks, st.Blksize)
}
