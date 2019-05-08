package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coraxster/binanceMiner/clickhouseStore"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/gorilla/websocket"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const BinanceSymbolsUrl = "https://www.binance.com/api/v3/ticker/price"
const BinanceUpdatesHost = "stream.binance.com:9443"
const StreamSuffix = "@depth"
const StreamTimelimit = 23 * time.Hour
const SourceKey = "binance"
const BinanceBooksUrlTmpl = "https://www.binance.com/api/v1/depth?symbol=%s&limit=500"

type BinanceMiner struct {
	connN   int
	topSize int
}

func NewBinanceMiner(connN int, topSize int) *BinanceMiner {
	return &BinanceMiner{connN: connN, topSize: topSize}
}

func (s *BinanceMiner) SeedBooks(booksCh chan *clickhouseStore.Book, symbols []string) error {
	ctx, cancelSeed := context.WithCancel(context.Background())
	defer cancelSeed()
	updatesCh := make(chan *bookUpdate, 1000)
	go func() {
		s.multiSeedUpdates(ctx, updatesCh, symbols)
		close(updatesCh)
	}()
	time.Sleep(10 * time.Second)
	bState, err := s.getFullBooksState(symbols)
	if err != nil {
		return err
	}
	for bUpdate := range updatesCh {
		updatingBook, ok := bState[bUpdate.Symbol]
		if !ok {
			fmt.Println("symbol not found in booksState: ", bUpdate.Symbol)
			continue
		}
		if bUpdate.FirstSecN > updatingBook.secN+1 {
			return errors.New(fmt.Sprintf("got %d > %d", bUpdate.FirstSecN, updatingBook.secN+1))
		}
		if bUpdate.LastSecN < updatingBook.secN+1 {
			continue
		}
		updatingBook.secN = bUpdate.LastSecN
		updatingBook.time = time.Unix(int64(bUpdate.Ts)/1000, int64(bUpdate.Ts)%1000*1000000)
		updatingBook.updatePrices(bUpdate.Bids, bUpdate.Asks)
		booksCh <- convertToClickhouse(updatingBook, s.topSize)
	}
	return errors.New("unexpected updatesCh close")
}

func (s *BinanceMiner) multiSeedUpdates(ctx context.Context, updatesCh chan *bookUpdate, symbols []string) {
	wg := sync.WaitGroup{}
	worker := func(workerId int) {
		defer wg.Done()
		for {
			if err := s.seedUpdatesTimingOut(ctx, updatesCh, symbols); err != nil {
				log.Warn(workerId, ": ", err)
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
	for i := 0; i < s.connN; i++ {
		wg.Add(1)
		go worker(i)
	}
	wg.Wait()
}

func (s *BinanceMiner) seedUpdatesTimingOut(ctx context.Context, updatesCh chan *bookUpdate, symbols []string) error {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancelSeed := context.WithCancel(ctx)
	defer cancelSeed()
	for {
		workerErrCh := make(chan error)
		workerCtx, _ := context.WithTimeout(ctx, StreamTimelimit)
		go func() {
			wg.Add(1)
			workerErrCh <- s.seedUpdates(workerCtx, updatesCh, symbols)
			wg.Done()
			close(workerErrCh)
		}()
		select {
		case err := <-workerErrCh:
			return err
		case <-time.After(StreamTimelimit - 10*time.Minute):
			go func() {
				<-workerErrCh
			}()
		}
	}
}

type streamResponse struct {
	Data bookUpdate
}
type bookUpdate struct {
	Event     string        `json:"e"`
	Ts        int           `json:"E"`
	Symbol    string        `json:"s"`
	FirstSecN int           `json:"U"`
	LastSecN  int           `json:"u"`
	Bids      binanceQuotes `json:"b"`
	Asks      binanceQuotes `json:"a"`
}
type binanceQuotes [][2]float64

func (c *binanceQuotes) UnmarshalJSON(b []byte) error {
	tmp := make([][2]json.Number, 0)
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	vals := make([][2]float64, len(tmp))
	for i, a := range tmp {
		var err error
		vals[i][0], err = a[0].Float64()
		if err != nil {
			return err
		}
		vals[i][1], err = a[1].Float64()
		if err != nil {
			return err
		}
	}
	*c = vals
	return nil
}

func (s *BinanceMiner) seedUpdates(ctx context.Context, ch chan *bookUpdate, symbols []string) error {
	query := "streams="
	for _, s := range symbols {
		query = query + strings.ToLower(s) + StreamSuffix + "/"
	}
	u := url.URL{Scheme: "wss", Host: BinanceUpdatesHost, Path: "/stream", RawQuery: query}
	c, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return err
	}
	time.Sleep(5 * time.Second)
	defer c.Close()
	for {
		var r streamResponse
		if err := c.ReadJSON(&r); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case ch <- &r.Data:
		}

	}
}

type fullBookResponse struct {
	symbol string
	SecN   int           `json:"lastUpdateId"`
	Bids   binanceQuotes `json:"bids"`
	Asks   binanceQuotes `json:"asks"`
}

func (s *BinanceMiner) getFullBooksState(symbols []string) (booksSate, error) {
	bState := booksSate(make(map[string]*book))
	for i, s := range symbols {
		var br fullBookResponse
		msg, err := http.Get(fmt.Sprintf(BinanceBooksUrlTmpl, s))
		if err != nil {
			return nil, err
		}
		err = json.NewDecoder(msg.Body).Decode(&br)
		if err != nil {
			return nil, err
		}
		//todo: добавить обработку сообщений-ошибок
		b := &book{
			secN: br.SecN,
			bids: treemap.NewWith(treeComparator),
			asks: treemap.NewWith(treeComparator),
		}
		b.updatePrices(br.Bids, br.Asks)
		bState[s] = b
		log.Println("got full book", i+1, "/", len(symbols))
	}
	return bState, nil
}

type symbolInfo struct {
	Symbol string
}

func (s *BinanceMiner) GetAllSymbols() ([]string, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", BinanceSymbolsUrl, nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	respBody, _ := ioutil.ReadAll(resp.Body)
	parsed := make([]symbolInfo, 0)
	err = json.Unmarshal(respBody, &parsed)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(parsed))
	for i, s := range parsed {
		result[i] = s.Symbol
	}
	return result, nil
}

func convertToClickhouse(b *book, topSize int) *clickhouseStore.Book {
	askPrices := make([]float64, 0, topSize)
	askQuantities := make([]float64, 0, topSize)
	for _, q := range b.topAsks(topSize) {
		askPrices = append(askPrices, q[0])
		askQuantities = append(askQuantities, q[1])
	}
	bidPrices := make([]float64, 0, topSize)
	bidQuantities := make([]float64, 0, topSize)
	for _, q := range b.topBids(topSize) {
		bidPrices = append(bidPrices, q[0])
		bidQuantities = append(bidQuantities, q[1])
	}
	return &clickhouseStore.Book{
		Source:        SourceKey,
		Time:          b.time,
		Symbol:        b.symbol,
		SecN:          b.secN,
		BidPrices:     bidPrices,
		AskPrices:     askPrices,
		BidQuantities: bidQuantities,
		AskQuantities: askQuantities,
	}
}
