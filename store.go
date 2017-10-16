package panopticon

import (
	"context"
	"sync"

	"cryptoscope.co/go/binpath"
	"cryptoscope.co/go/voyeur"

	"github.com/dgraph-io/badger"
)

type PutEvent struct {
	Path  binpath.Path
	Value []byte
}

func (e PutEvent) EventType() string {
	return "PutEvent"
}

type Store struct {
	voyeur.Filter

	db     *badger.DB
	prefix binpath.Path

	subs map[string]*Store
	lock sync.Mutex
}

func NewStore(db *badger.DB, f voyeur.Filter) *Store {
	s := &Store{
		Filter: f,
		db:     db,
		subs:   make(map[string]*Store),
	}

	return s
}

func (s *Store) GetStore(path binpath.Path) *Store {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.subs == nil {
		s.subs = make(map[string]*Store)
	}

	var (
		cur  = s
		head binpath.Path
	)

	for {
		head, path = path.Pop()
		if next, ok := cur.subs[string(head)]; ok {
			cur = next
		} else {
			return nil
		}
	}

	return cur
}

func (s *Store) MkSubStore(rel string, filterFunc func(context.Context, *Store, voyeur.Emitter, voyeur.Event)) (*Store, error) {
	relPath, err := binpath.FromString(rel)
	if err != nil {
		return nil, err
	}
	
	sub := &Store{
		prefix: binpath.Join(s.prefix, relPath),
		db: s.db,
		subs: make(map[string]*Store),
	}
	sub.Filter = BuildFilter(sub, filterFunc)

	s.Register(context.Background(), sub)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.subs[rel] = sub

	return sub, nil
}

func (s *Store) Get(path binpath.Path) (data []byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(binpath.Join(s.prefix, path))
		if err != nil {
			return err
		}

		data, err = item.Value()
		return err
	})

	return data, err
}

func (s *Store) Put(path binpath.Path, data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(binpath.Join(s.prefix, path), data, 0)
	})
}
