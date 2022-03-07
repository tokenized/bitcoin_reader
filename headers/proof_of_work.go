package headers

import (
	"context"
	"math/big"
	"sort"

	"github.com/tokenized/pkg/bitcoin"

	"github.com/pkg/errors"
)

func (b Branch) Target(ctx context.Context, height int) (*big.Int, error) {

	// NOTE: Assume 2017 difficulty adjustment is active --ce

	// Get median time and work for 3 blocks at current height
	lastTime, lastWork, err := b.MedianTimeAndWork(ctx, height-1, 3)
	if err != nil {
		return nil, errors.Wrap(err, "last header stats")
	}

	// Get median time and work for 3 blocks for 144 blocks below current height
	firstTime, firstWork, err := b.MedianTimeAndWork(ctx, height-144-1, 3)
	if err != nil {
		return nil, errors.Wrap(err, "first header stats")
	}

	timeSpan := lastTime - firstTime

	// Apply time span limits
	if timeSpan < 72*600 {
		timeSpan = 72 * 600
	}
	if timeSpan > 288*600 {
		timeSpan = 288 * 600
	}

	// Work (W) is difference in median accumulated work
	work := &big.Int{}
	work.Sub(lastWork, firstWork)

	// Projected Work (PW) = (W * 600) / TS.
	projected := &big.Int{}
	projected.Mul(work, big.NewInt(600))
	projected.Div(projected, big.NewInt(int64(timeSpan)))

	target := bitcoin.ConvertToWork(projected)

	if target.Cmp(bitcoin.MaxWork) > 0 {
		target.Set(bitcoin.MaxWork)
	}

	return target, nil
}

func (b Branch) TimeAndWork(ctx context.Context, height int) (uint32, *big.Int, error) {
	data := b.AtHeight(height)
	if data == nil {
		return 0, nil, ErrHeaderDataNotFound
	}

	return data.Header.Timestamp, data.AccumulatedWork, nil
}

func (b Branch) MedianTimeAndWork(ctx context.Context,
	height, count int) (uint32, *big.Int, error) {

	// Get time and accumulated work for 3 blocks ending at height
	list := make(timeAndWorkList, count)
	for i := 0; i < count; i++ {
		time, work, err := b.TimeAndWork(ctx, height)
		if err != nil {
			return 0, nil, errors.Wrapf(err, "get header stats : %d", height)
		}

		list[count-i-1] = &timeAndWork{
			time: time,
			work: work,
		}

		height--
	}

	// Sort by time
	sort.Sort(list)

	// Get values from the middle item in the list.
	result := list[count/2]
	return result.time, result.work, nil
}

type timeAndWork struct {
	time uint32
	work *big.Int
}

type timeAndWorkList []*timeAndWork

// Len is part of sort.Interface.
func (l timeAndWorkList) Len() int {
	return len(l)
}

// Swap is part of sort.Interface.
func (l timeAndWorkList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// Less is part of sort.Interface.
func (l timeAndWorkList) Less(i, j int) bool {
	return l[i].time < l[j].time
}
