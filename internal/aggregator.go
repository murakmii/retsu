package internal

type (
	Aggregator[T any] interface {
		Aggregate(T, uint64)
	}

	IntSumAggregator[T int32 | int64] struct {
		sum T
		n   uint64
	}
)

func NewIntSumAggregator[T int32 | int64]() *IntSumAggregator[T] {
	return &IntSumAggregator[T]{}
}

func (agg *IntSumAggregator[T]) Aggregate(n T, repeated uint64) {
	agg.sum += n * T(repeated)
}

func (agg *IntSumAggregator[T]) Result() T {
	return agg.sum
}
