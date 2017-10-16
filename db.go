package panopticon

import (
	"context"
	"encoding/binary"
	"sort"
	"sync"

	"cryptoscope.co/go/binpath"
	"cryptoscope.co/go/voyeur"

	"github.com/dgraph-io/badger"
)

func Fwd(ctx context.Context, s *Store, em voyeur.Emitter, e voyeur.Event) {
	em.Emit(ctx, e)
}

type NewMessageEvent struct {
	Key   []byte
	Path  binpath.Path
	Seq   uint64
	Value []byte
}

func (e NewMessageEvent) EventType() string {
	return "NewMessageEvent"
}

func BuildFilter(s *Store, f func(context.Context, *Store, voyeur.Emitter, voyeur.Event)) voyeur.Filter {
	return voyeur.Map(func (ctx context.Context, em voyeur.Emitter, e voyeur.Event) {
		f(ctx, s, em, e)
	})
}

type FilterSpec map[string]func(context.Context, *Store, voyeur.Emitter, voyeur.Event)

func (s FilterSpec) Sorted() []string {
	paths := make([]string, 0, len(s))

	for p := range s {
		paths = append(paths, p)
	}

	sort.Strings(paths)

	return paths
}

type DB struct {
	s    *Store
	spec FilterSpec
	l    sync.Mutex
}

func NewDB(bdg *badger.DB, spec FilterSpec) (*DB, error) {
	db := &DB{
		s:    NewStore(bdg, voyeur.Noop),
		spec: spec,
	}

	filters, err := db.s.MkSubStore("filters", Fwd)
	if err != nil {
		return nil, err
	}

	for _, path := range spec.Sorted() {
		_, err := filters.MkSubStore(path, spec[path])
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func incBytes(buf []byte) uint64 {
	n := binary.BigEndian.Uint64(buf)
	binary.BigEndian.PutUint64(buf, n+1)
	return n
}

func (db *DB) Put(ctx context.Context, key, value []byte) error {
	orderPrefix := binpath.Must(binpath.FromString("/order/"))
	msgsPrefix := binpath.Must(binpath.FromString("/msgs/"))

	msgsPath := binpath.Join(msgsPrefix, binpath.FromBytes(key))
	orderStatus := binpath.Must(binpath.FromString("/order/cur"))

	db.l.Lock()
	defer db.l.Unlock()

	var seq uint64

	err := db.s.db.Update(func(txn *badger.Txn) error {
		curItem, err := txn.Get(orderStatus)
		if err != nil {
			return err
		}

		nextBs, err := curItem.Value()
		if err != nil {
			return err
		}
		seq = incBytes(nextBs)

		txn.Set(msgsPath, value, 0)
		if err != nil {
			return err
		}

		// TODO possible optimization: don't use msgsKey here but key to save space
		err = txn.Set(binpath.Join(orderPrefix, binpath.FromBytes(nextBs)), msgsPath, 0)
		if err != nil {
			return err
		}

		return txn.Set(orderStatus, nextBs, 0)
	})
	if err != nil {
		return err
	}

	go db.s.OnEvent(ctx, NewMessageEvent{
		Key:   key,
		Value: value,
		Path:  msgsPath,
		Seq:   seq,
	})

	return nil
}
