package main

import (
	"github.com/emirpasic/gods/maps/treemap"
	"time"
)

type booksSate map[string]*book

type book struct {
	symbol string
	secN   int
	time   time.Time
	bids   *treemap.Map
	asks   *treemap.Map
}

func treeComparator(a, b interface{}) int {
	af, _ := a.(float64)
	bf, _ := b.(float64)
	if af < bf {
		return -1
	}
	if af > bf {
		return 1
	}
	return 0
}

func (sq *book) topBids(n int) [][2]float64 {
	top := make([][2]float64, 0, n)
	it := sq.bids.Iterator()
	it.End()
	i := 0
	for it.Prev() {
		top = append(top, [2]float64{it.Key().(float64), it.Value().(float64)})
		if i++; i == n {
			return top
		}
	}
	return top
}

func (sq *book) topAsks(n int) [][2]float64 {
	top := make([][2]float64, 0, n)
	it := sq.asks.Iterator()
	i := 0
	for it.Next() {
		top = append(top, [2]float64{it.Key().(float64), it.Value().(float64)})
		if i++; i == n {
			return top
		}
	}
	return top
}

func (sq *book) updatePrices(bids binanceQuotes, asks binanceQuotes) {
	for _, q := range bids {
		if q[1] == 0 {
			sq.bids.Remove(q[0])
			continue
		}
		sq.bids.Put(q[0], q[1])
	}
	for _, q := range asks {
		if q[1] == 0 {
			sq.asks.Remove(q[0])
			continue
		}
		sq.asks.Put(q[0], q[1])
	}
}
