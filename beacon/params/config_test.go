package params

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestChainConfig_LoadForks(t *testing.T) {
	const config = `
GENESIS_FORK_VERSION: 0x00000000

ALTAIR_FORK_VERSION: 0x00000001
ALTAIR_FORK_EPOCH: 1

EIP7928_FORK_VERSION: 0xb0000038
EIP7928_FORK_EPOCH: 18446744073709551615

EIP7XXX_FORK_VERSION: 
EIP7XXX_FORK_EPOCH: 

BLOB_SCHEDULE: []
`
	c := &ChainConfig{}
	err := c.LoadForks([]byte(config))
	if err != nil {
		t.Fatal(err)
	}

	for _, fork := range c.Forks {
		if fork.Name == "GENESIS" && (fork.Epoch != 0) {
			t.Errorf("unexpected genesis fork epoch %d", fork.Epoch)
		}
		if fork.Name == "ALTAIR" && (fork.Epoch != 1 || !bytes.Equal(fork.Version, []byte{0, 0, 0, 1})) {
			t.Errorf("unexpected altair fork epoch %d version %x", fork.Epoch, fork.Version)
		}
	}
}

func TestSaveCheckpointToFile(t *testing.T) {
	var (
		dir  = t.TempDir()
		file = filepath.Join(dir, "checkpoint")
		link = filepath.Join(dir, "link")
		cp1  = common.Hash{1}
		cp2  = common.Hash{2}
	)
	c := &ChainConfig{CheckpointFile: file}
	if saved, err := c.SaveCheckpointToFile(cp1); !saved || err != nil {
		t.Fatalf("first save: saved %v, error %v", saved, err)
	}
	// A hard link keeps the file the first save wrote. It sees the second
	// checkpoint only if the second save rewrites that file in place.
	linked := os.Link(file, link) == nil
	if !linked {
		t.Log("No hard links on this file system: not checking that the file is replaced")
	}
	if saved, err := c.SaveCheckpointToFile(cp2); !saved || err != nil {
		t.Fatalf("second save: saved %v, error %v", saved, err)
	}
	if linked {
		if old, err := os.ReadFile(link); err != nil || string(old) != cp1.Hex() {
			t.Errorf("the second save rewrote the file in place: %q, %v", old, err)
		}
	}
	// The second checkpoint loads, and no temporary file is left behind.
	checkLoad(t, file, cp2)
	if tmp, _ := filepath.Glob(filepath.Join(dir, ".checkpoint.tmp*")); len(tmp) != 0 {
		t.Errorf("temporary files left behind: %v", tmp)
	}
}

func TestSaveCheckpointToFileSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symbolic links need privileges on Windows")
	}
	dir := t.TempDir()

	// A link to an existing file: the file gets the checkpoint and keeps its
	// permissions.
	var (
		target = filepath.Join(dir, "target")
		link   = filepath.Join(dir, "link")
	)
	if err := os.WriteFile(target, []byte(common.Hash{1}.Hex()), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(target, 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	c := &ChainConfig{CheckpointFile: link}
	if saved, err := c.SaveCheckpointToFile(common.Hash{2}); !saved || err != nil {
		t.Fatalf("save: saved %v, error %v", saved, err)
	}
	checkLink(t, link)
	checkLoad(t, target, common.Hash{2})
	if info, err := os.Stat(target); err != nil || info.Mode().Perm() != 0640 {
		t.Errorf("the save changed the file's permissions (%v)", err)
	}

	// A link to a file that doesn't exist yet, by absolute and by relative path:
	// the save creates the file.
	for _, dest := range []string{filepath.Join(dir, "later"), "later-relative"} {
		link := filepath.Join(dir, "link-"+filepath.Base(dest))
		if err := os.Symlink(dest, link); err != nil {
			t.Fatal(err)
		}
		c := &ChainConfig{CheckpointFile: link}
		if saved, err := c.SaveCheckpointToFile(common.Hash{3}); !saved || err != nil {
			t.Fatalf("save through a link to %s: saved %v, error %v", dest, saved, err)
		}
		checkLink(t, link)
		checkLoad(t, filepath.Join(dir, filepath.Base(dest)), common.Hash{3})
	}
}

// checkLink checks that the save left the symbolic link a link.
func checkLink(t *testing.T, link string) {
	t.Helper()
	if info, err := os.Lstat(link); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Errorf("the save replaced the symbolic link %s (%v)", link, err)
	}
}

func TestSaveCheckpointToFileInPlace(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs a directory the test can't create files in")
	}
	var (
		dir  = t.TempDir()
		file = filepath.Join(dir, "checkpoint")
	)
	if err := os.WriteFile(file, bytes.Repeat([]byte{'x'}, 100), 0600); err != nil {
		t.Fatal(err)
	}
	// The file is writable but its directory isn't: the save writes over it in
	// place, and cuts it to the checkpoint's length.
	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0700)
	c := &ChainConfig{CheckpointFile: file}
	if saved, err := c.SaveCheckpointToFile(common.Hash{1}); !saved || err != nil {
		t.Fatalf("save: saved %v, error %v", saved, err)
	}
	checkLoad(t, file, common.Hash{1})
}

func TestSetCheckpointFileEmpty(t *testing.T) {
	file := filepath.Join(t.TempDir(), "checkpoint")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	// An empty file holds no checkpoint: the configured one is kept, and the
	// next save writes one into the file.
	c := &ChainConfig{Checkpoint: common.Hash{1}}
	if loaded, err := c.SetCheckpointFile(file); loaded || err != nil {
		t.Fatalf("empty file: loaded %v, error %v", loaded, err)
	}
	if c.Checkpoint != (common.Hash{1}) {
		t.Errorf("empty file changed the checkpoint to %v", c.Checkpoint)
	}
	if saved, err := c.SaveCheckpointToFile(common.Hash{2}); !saved || err != nil {
		t.Fatalf("save: saved %v, error %v", saved, err)
	}
	checkLoad(t, file, common.Hash{2})

	// Other content that isn't a checkpoint is still an error.
	if err := os.WriteFile(file, []byte("not a checkpoint"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := new(ChainConfig).SetCheckpointFile(file); err == nil {
		t.Error("no error for a file that isn't a checkpoint")
	}
}

// checkLoad checks that the checkpoint file loads, and holds want.
func checkLoad(t *testing.T, file string, want common.Hash) {
	t.Helper()
	c := new(ChainConfig)
	if loaded, err := c.SetCheckpointFile(file); !loaded || err != nil {
		t.Fatalf("load: loaded %v, error %v", loaded, err)
	}
	if c.Checkpoint != want {
		t.Errorf("loaded checkpoint %v, want %v", c.Checkpoint, want)
	}
}
