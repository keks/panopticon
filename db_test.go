package panopticon

import (
	"context"
	"io/ioutil"
	"testing"

	"cryptoscope.co/go/voyeur"

	"github.com/dgraph-io/badger"
)

func TestDB(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp/", "go_test-ssb_nt")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("temp dir:", tmpdir)
	// defer os.RemoveAll(tmpdir)

	opts := badger.DefaultOptions
	opts.Dir = tmpdir
	opts.ValueDir = tmpdir

	bdg, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	
	spec := FilterSpec{
		"index": func(ctx context.Context, s *Store, em voyeur.Emitter, e voyeur.Event) {
		},
	}
	
	_ = bdg
	_ = spec
}