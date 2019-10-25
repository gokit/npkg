package nfs_test

import (
	"testing"

	filesystem "github.com/influx6/npkg/nfs"
	tests "github.com/influx6/npkg/ntests"
)

func TestFileSystemGroup(t *testing.T) {
	g := filesystem.VirtualFileSystem{
		GetFileFunc: getFileFunc,
	}
	b := filesystem.VirtualFileSystem{
		GetFileFunc: getFileFunc,
	}

	gs := filesystem.NewSystemGroup()
	if err := gs.Register("/static/", g); err != nil {
		tests.Failed("Should have succesffully registered nfs for /static/")
	}
	tests.Passed("Should have succesffully registered nfs for /static/")

	if err := gs.Register("/thunder", b); err != nil {
		tests.Failed("Should have succesffully registered nfs for /thunder")
	}
	tests.Passed("Should have succesffully registered nfs for /thunder")

	if err := gs.Register("/static/css", b); err != nil {
		tests.Failed("Should have succesffully registered nfs for /static/css")
	}
	tests.Passed("Should have succesffully registered nfs for /static/css")

	if _, err := gs.Open("/static/"); err != nil {
		tests.Failed("Should have succeeded in retrieving dir")
	}
	tests.Passed("Should have succeeded in retrieving dir")

	if _, err := gs.Open("/static/css"); err != nil {
		tests.Failed("Should have succeeded in retrieving dir")
	}
	tests.Passed("Should have succeeded in retrieving dir")

	if _, err := gs.Open("/thunder"); err != nil {
		tests.Failed("Should have succeeded in retrieving dir")
	}
	tests.Passed("Should have succeeded in retrieving dir")

	if _, err := gs.Open("/static/wombat.css"); err != nil {
		tests.Failed("Should have succeeded to retrieve any file: %+q", err)
	}
	tests.Passed("Should have succeeded to retrieve any fileq")

	if _, err := gs.Open("/thunder/wombat.css"); err != nil {
		tests.Failed("Should have succeeded to retrieve any file: %+q", err)
	}
	tests.Passed("Should have succeeded to retrieve any fileq")

	if _, err := gs.Open("/static/css/wombat.css"); err != nil {
		tests.Failed("Should have succeeded to retrieve any file: %+q", err)
	}
	tests.Passed("Should have succeeded to retrieve any fileq")

}

func getFileFunc(path string) (filesystem.File, error) {
	return &filesystem.VirtualFile{}, nil
}
