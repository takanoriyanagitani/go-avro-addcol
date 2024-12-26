package util

import "context"

type IO[T any] func(context.Context) (T, error)

func (i IO[T]) Or(alt IO[T]) IO[T] {
	return func(ctx context.Context) (T, error) {
		t, e := i(ctx)
		switch e {
		case nil:
			return t, nil
		default:
			return alt(ctx)
		}
	}
}

func Err[T any](err error) IO[T] {
	return func(_ context.Context) (t T, e error) {
		return t, err
	}
}

func Of[T any](t T) IO[T] {
	return func(_ context.Context) (T, error) {
		return t, nil
	}
}

func OfFn[T any](f func() T) IO[T] {
	return func(_ context.Context) (T, error) {
		return f(), nil
	}
}

func Bind[T, U any](
	i IO[T],
	f func(T) IO[U],
) IO[U] {
	return func(ctx context.Context) (u U, e error) {
		t, e := i(ctx)
		if nil != e {
			return u, e
		}
		return f(t)(ctx)
	}
}

func Lift[T, U any](
	pure func(T) (U, error),
) func(T) IO[U] {
	return func(t T) IO[U] {
		return func(_ context.Context) (U, error) {
			return pure(t)
		}
	}
}

type Void struct{}

var Empty Void = struct{}{}

func All[T any](i ...IO[T]) IO[[]T] {
	return func(ctx context.Context) ([]T, error) {
		var ret []T = make([]T, 0, len(i))
		for _, iot := range i {
			t, e := iot(ctx)
			if nil != e {
				return nil, e
			}
			ret = append(ret, t)
		}
		return ret, nil
	}
}
