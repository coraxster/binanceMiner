package binanceScrubber

import (
	"context"
	"encoding/json"
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

type BinanceScrubber struct {
	aliveCount int32
}

func NewBinanceScrubber() *BinanceScrubber {
	return &BinanceScrubber{}
}

type Book struct {
	Source string
	Time   time.Time
	Symbol string
	SecN   int
	Bids   [][2]float64
	Asks   [][2]float64
}

func (s *BinanceScrubber) AliveCount() int {
	return int(atomic.LoadInt32(&s.aliveCount))
}

func (s *BinanceScrubber) SeedBooks(ch chan *Book, symbols []string) error {
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
type streamQuotes [][2]float64

func (c *streamQuotes) UnmarshalJSON(b []byte) error {
	tmp := make([][2]json.Number, 0)
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	*c = make(streamQuotes, len(tmp))
	for i, a := range tmp {
		var pair [2]float64
		var err error
		pair[0], err = a[0].Float64()
		if err != nil {
			return err
		}
		pair[1], err = a[1].Float64()
		if err != nil {
			return err
		}
		(*c)[i] = pair
	}
	return nil
}

func (s *BinanceScrubber) seed(ctx context.Context, ch chan *Book, query string) error {
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
		_, message, err := c.ReadMessage()
		if err != nil {
			return err
		}
		t := time.Now()
		err = json.Unmarshal(message, &r)
		if err != nil {
			return err
		}
		symbol := r.Stream[0 : len(r.Stream)-len(StreamSuffix)]
		ch <- &Book{"binance", t, symbol, r.Data.LastUpdateId, r.Data.Bids, r.Data.Asks}
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

func (s *BinanceScrubber) GetAllSymbols() ([]string, error) {
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
