package panopticon

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"cryptoscope.co/go/voyeur"
	"github.com/dgraph-io/badger"
)

func TestStore(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp/", "go_test-ssb_nt")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(tmpdir)
	opts := badger.DefaultOptions
	opts.Dir = tmpdir
	opts.ValueDir = tmpdir

	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	s := NewStore(
		db,
		voyeur.Map(func(ctx context.Context, em voyeur.Emitter, e voyeur.Event) {}))

	filterStore := NewStore(
		db,
		voyeur.Map(func(ctx context.Context, em voyeur.Emitter, e voyeur.Event) {
			em.Emit(ctx, e)
		}))

	s.PutStore(
		"msg",
		NewStore(
			db,
			voyeur.Map(func(ctx context.Context, em voyeur.Emitter, e voyeur.Event) {
				filterStore.OnEvent(ctx, e)
			})))

	s.PutStore("filters", filterStore)

	filterStore.PutStore(
		"startsWithZero",
		NewStore(
			db,
			voyeur.Map(func(ctx context.Context, em voyeur.Emitter, e voyeur.Event) {
				pe, ok := e.(PutEvent)
				if !ok {
					return
				}

				if len(pe.Value) > 0 && pe.Value[0] == 0 {
					em.Emit(ctx, e)
				}
			})))
}
