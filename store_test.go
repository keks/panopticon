package panopticon

import (
	"context"
	"io/ioutil"
	"testing"

	"cryptoscope.co/go/binpath"
	"cryptoscope.co/go/voyeur"

	"github.com/dgraph-io/badger"
)

type TestEvent struct {
	NewMessageEvent
	
	// whether or not message should pass filter
	Pass bool
}

func TestStore(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp/", "go_test-ssb_nt")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("temp dir:", tmpdir)
	//defer os.RemoveAll(tmpdir)

	opts := badger.DefaultOptions
	opts.Dir = tmpdir
	opts.ValueDir = tmpdir

	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	root := NewStore(db, voyeur.Fwd)

	_, err = root.MkSubStore("filter", func(ctx context.Context, s *Store, em voyeur.Emitter, e voyeur.Event) {
		t.Logf("filterStore got event of type %T", e)
		if ev, ok := e.(TestEvent); ok && ev.Pass {
			t.Logf("filterStore accepted event %#v", e)

			ev.Path = binpath.FromBytes(ev.Key)
			
			err := s.Put(ev.Path, ev.Value)
			if err != nil {
				t.Log(err)
				t.Fail()
				return
			}
			
			em.Emit(ctx, ev)
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	root.OnEvent(context.Background(), TestEvent{
		NewMessageEvent: NewMessageEvent{
			Key:[]byte("foo"),
			Value:[]byte("bar"),
		},
		Pass: false,
	})
	
	root.OnEvent(context.Background(), TestEvent{
		NewMessageEvent: NewMessageEvent{
			Key:[]byte("testkey"),
			Value:[]byte("testvalue"),
		},
		Pass: true,
	})
	
	var n int
	
	err = root.db.View(func (txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{})
		for it.Rewind(); it.Valid(); it.Next() {
			n++
		}
		return nil
	})
	
	if err != nil {
		t.Fatal(err)
	}
	
	if n != 1 {
		t.Fatalf("expected 1 record, got %d", n)
	}
}
