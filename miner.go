package main

import (
	"context"
	"encoding/json"
	"github.com/coraxster/binanceMiner/clickhouseStore"
	"github.com/gorilla/websocket"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

const BinanceSymbolsUrl = "https://www.binance.com/api/v3/ticker/price"
const BinanceBooksHost = "stream.binance.com:9443"
const StreamSuffix = "@depth10"
const StreamTimelimit = 23 * time.Hour
const SourceKey = "binance"

type BinanceMiner struct {
	aliveCount int32
}

func NewBinanceMiner() *BinanceMiner {
	return &BinanceMiner{}
}

func (s *BinanceMiner) AliveCount() int {
	return int(atomic.LoadInt32(&s.aliveCount))
}

func (s *BinanceMiner) SeedBooks(ch chan *clickhouseStore.Book, symbols []string) error {
	query := "streams="
	for _, s := range symbols {
		query = query + strings.ToLower(s) + StreamSuffix + "/"
	}
	for {
		ctx, _ := context.WithTimeout(context.Background(), StreamTimelimit)
		errCh := make(chan error)
		go func() {
			errCh <- s.seed(ctx, ch, query)
			close(errCh)
		}()
		select {
		case err := <-errCh:
			return err
		case <-time.After(StreamTimelimit - 10*time.Minute):
			go func() {
				<-errCh
			}()
		}
	}
}

type streamResponse struct {
	Stream string
	Data   streamResponseData
}
type streamResponseData struct {
	LastUpdateId int
	Bids         streamQuotes
	Asks         streamQuotes
}
type streamQuotes struct {
	Prices     []float64
	Quantities []float64
}

func (c *streamQuotes) UnmarshalJSON(b []byte) error {
	tmp := make([][2]json.Number, 0)
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	c.Prices = make([]float64, len(tmp))
	c.Quantities = make([]float64, len(tmp))
	for i, a := range tmp {
		var err error
		c.Prices[i], err = a[0].Float64()
		if err != nil {
			return err
		}
		c.Quantities[i], err = a[1].Float64()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *BinanceMiner) seed(ctx context.Context, ch chan *clickhouseStore.Book, query string) error {
	u := url.URL{Scheme: "wss", Host: BinanceBooksHost, Path: "/stream", RawQuery: query}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	atomic.AddInt32(&s.aliveCount, 1)
	defer c.Close()
	defer atomic.AddInt32(&s.aliveCount, -1)

	var r streamResponse
	for {
		if err = c.ReadJSON(&r); err != nil {
			return err
		}
		t := time.Now()
		symbol := r.Stream[0 : len(r.Stream)-len(StreamSuffix)]
		ch <- &clickhouseStore.Book{
			Source:        SourceKey,
			Time:          t,
			Symbol:        symbol,
			SecN:          r.Data.LastUpdateId,
			BidPrices:     r.Data.Bids.Prices,
			AskPrices:     r.Data.Asks.Prices,
			BidQuantities: r.Data.Bids.Quantities,
			AskQuantities: r.Data.Asks.Quantities,
		}
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
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
