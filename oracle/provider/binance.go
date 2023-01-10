package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"
	"time"
	"strconv"

	"github.com/gorilla/websocket"
	"price-feeder/oracle/types"
	"github.com/rs/zerolog"
)

const (
	binanceWSPath     = "/ws/umeestream"
	binanceRestPath   = "/api/v3/ticker/price"
	binanceRestPathTickers = "/api/v3/ticker"
)

var (
	_ Provider = (*BinanceProvider)(nil)
	binanceDefaultEndpoints = Endpoint{
		Name: ProviderBinance,
		Rest: "https://api1.binance.com",
		Websocket: "stream.binance.com:9443",
		PollInterval: 6 * time.Second,
		PingDuration: disabledPingDuration,
		PingType: websocket.PingMessage,
	}
	binanceUSDefaultEndpoints = Endpoint{
		Name: ProviderBinanceUS,
		Rest: "https://api.binance.us",
		Websocket: "stream.binance.us:9443",
		PollInterval: 6 * time.Second,
		PingDuration: disabledPingDuration,
		PingType: websocket.PingMessage,
	}
)

type (
	// BinanceProvider defines an Oracle provider implemented by the Binance public
	// API.
	//
	// REF: https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-mini-ticker-stream
	// REF: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams
	BinanceProvider struct {
		provider
	}

	BinanceTicker struct {
		Symbol    string `json:"symbol"` // Symbol ex.: BTCUSDT
		LastPrice string `json:"lastPrice"` // Last price ex.: 0.0025
		Volume    string `json:"volume"` // Total traded base asset volume ex.: 1000
	}

	BinanceCandleMetadata struct {
		Close     string `json:"c"` // Price at close
		TimeStamp int64  `json:"T"` // Close time in unix epoch ex.: 1645756200000
		Volume    string `json:"v"` // Volume during period
	}

	BinanceCandle struct {
		Symbol   string                `json:"s"` // Symbol ex.: BTCUSDT
		Metadata BinanceCandleMetadata `json:"k"` // Metadata for candle
	}

	BinanceSubscriptionMsg struct {
		Method string   `json:"method"` // SUBSCRIBE/UNSUBSCRIBE
		Params []string `json:"params"` // streams to subscribe ex.: usdtatom@ticker
		ID     uint16   `json:"id"`     // identify messages going back and forth
	}

	BinanceSubscriptionResp struct {
		Result string `json:"result"`
		ID     uint16 `json:"id"`
	}

	BinancePairSummary struct {
		Symbol string `json:"symbol"`
	}
)

func NewBinanceProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	binanceUS bool,
	pairs ...types.CurrencyPair,
) (*BinanceProvider, error) {
	websocketUrl := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   binanceWSPath,
	}
	provider := &BinanceProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		websocketUrl,
		provider.messageReceived,
		provider.getSubscriptionMsgs,
	)
	go provider.poll()
	go provider.websocket.Start()
	return provider, nil
}

func (p *BinanceProvider) poll() {
	for {
		err := p.updateTickers()
		if err != nil {
			p.logger.Warn().Err(err).Msg("binance failed to update tickers")
		}
		time.Sleep(p.endpoints.PollInterval)
	}
}

func (p *BinanceProvider) updateTickers() error {
	symbols := make([]string, len(p.pairs))
	i := 0
	for symbol := range p.pairs {
		symbols[i] = symbol
		i++
	}
	url := fmt.Sprintf(
		"%s%s?type=MINI&symbols=[\"%s\"]",
		p.endpoints.Rest,
		binanceRestPathTickers,
		strings.Join(symbols, "\",\""),
	)
	tickersResponse, err := p.rest.Get(url)
	if err != nil {
		p.logger.Warn().Err(err).Msg("binance failed requesting tickers")
		return err
	}
	if tickersResponse.StatusCode != 200 {
		p.logger.Warn().Int("code", tickersResponse.StatusCode).Msg("binance tickers request returned invalid status")
		if tickersResponse.StatusCode == 429 || tickersResponse.StatusCode == 418 {
			backoffSeconds, err := strconv.Atoi(tickersResponse.Header.Get("Retry-After"))
			if err != nil {
				return err
			}
			p.logger.Warn().Int("seconds", backoffSeconds).Msg("binance ratelimit backoff")
			time.Sleep(time.Duration(backoffSeconds) * time.Second)
			return nil
		}
	}
	tickersContent, err := ioutil.ReadAll(tickersResponse.Body)
	if err != nil {
		return err
	}
	var tickers []BinanceTicker
	err = json.Unmarshal(tickersContent, &tickers)
	if err != nil {
		return err
	}
	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()
	for _, ticker := range tickers {
		p.tickers[ticker.Symbol] = types.TickerPrice {
			Price: strToDec(ticker.LastPrice),
			Volume: strToDec(ticker.Volume),
			Time: now,
		}
	}
	return nil
}

func (p *BinanceProvider) getSubscriptionMsgs(pairs ...types.CurrencyPair) []interface{} {
	msg := BinanceSubscriptionMsg{
		Method: "SUBSCRIBE",
		Params: make([]string, len(pairs)),
		ID:     1,
	}
	for i, cp := range pairs {
		msg.Params[i] = strings.ToLower(cp.String()) + "@kline_1m"
	}
	return []interface{}{msg}
}

func (p *BinanceProvider) messageReceived(messageType int, bz []byte) {
	var (
		candleResp       BinanceCandle
		candleErr        error
		subscribeResp    BinanceSubscriptionResp
		subscribeRespErr error
	)

	candleErr = json.Unmarshal(bz, &candleResp)
	if len(candleResp.Metadata.Close) != 0 {
		p.setCandlePair(candleResp)
		telemetryWebsocketMessage(ProviderBinance, MessageTypeCandle)
		return
	}

	subscribeRespErr = json.Unmarshal(bz, &subscribeResp)
	if subscribeResp.ID == 1 {
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("candle", candleErr).
		AnErr("subscribeResp", subscribeRespErr).
		Msg("Error on receive message")
}

func (p *BinanceProvider) setCandlePair(candle BinanceCandle) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	staleTime := PastUnixTime(providerCandlePeriod)
	candleList := []types.CandlePrice{}
	candlePrice, err := candle.toCandlePrice()
	if err != nil {
		p.logger.Warn().Err(err).Str("symbol", candle.Symbol).Msg("failed to convert candle price")
	} else {
		candleList = append(candleList, candlePrice)
	}
	for _, c := range p.candles[candle.Symbol] {
		if staleTime < c.TimeStamp {
			candleList = append(candleList, c)
		}
	}
	p.candles[candle.Symbol] = candleList
}

func (candle BinanceCandle) toCandlePrice() (types.CandlePrice, error) {
	return types.NewCandlePrice(string(ProviderBinance), candle.Symbol, candle.Metadata.Close, candle.Metadata.Volume,
		candle.Metadata.TimeStamp)
}

func (p *BinanceProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := p.rest.Get(p.endpoints.Rest + binanceRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary []BinancePairSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary))
	for _, pairName := range pairsSummary {
		availablePairs[strings.ToUpper(pairName.Symbol)] = struct{}{}
	}

	return availablePairs, nil
}
