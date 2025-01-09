package internal

type LoopingList[T any] struct {
	items []T
	idx   int
}

func NewLoopingList[T any](items []T) LoopingList[T] {
	return LoopingList[T]{
		items: items,
	}
}

func (s *LoopingList[T]) Next() T {
	defer func() {
		s.idx++
	}()
	return s.items[s.idx]
}
