package provider

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"price-feeder/oracle/types"
)

const (
	huobiWSPath        = "/ws"
	huobiReconnectTime = time.Minute * 2
	huobiRestPath      = "/market/tickers"
)

var (
	_ Provider = (*HuobiProvider)(nil)
	huobiDefaultEndpoints = Endpoint{
		Name: ProviderHuobi,
		Rest: "https://api.huobi.pro",
		Websocket: "api-aws.huobi.pro",
		PingDuration: disabledPingDuration,
		PingType: websocket.PingMessage,
	}
)

type (
	// HuobiProvider defines an Oracle provider implemented by the Huobi public
	// API.
	//
	// REF: https://huobiapi.github.io/docs/spot/v1/en/#market-ticker
	// REF: https://huobiapi.github.io/docs/spot/v1/en/#get-klines-candles
	HuobiProvider struct {
		provider
	}

	// HuobiTicker defines the response type for the channel and the tick object for a
	// given ticker/symbol.
	HuobiTicker struct {
		CH   string    `json:"ch"` // Channel name. Format：market.$symbol.ticker
		Tick HuobiTick `json:"tick"`
	}

	// HuobiTick defines the response type for the last 24h market summary and the last
	// traded price for a given ticker/symbol.
	HuobiTick struct {
		Vol       float64 `json:"vol"`       // Accumulated trading value of last 24 hours
		LastPrice float64 `json:"lastPrice"` // Last traded price
	}

	// HuobiCandle defines the response type for the channel and the tick object for a
	// given ticker/symbol.
	HuobiCandle struct {
		CH   string          `json:"ch"` // Channel name. Format：market.$symbol.kline.$period
		Tick HuobiCandleTick `json:"tick"`
	}

	// HuobiCandleTick defines the response type for the candle.
	HuobiCandleTick struct {
		Close     float64 `json:"close"` // Closing price during this period
		TimeStamp int64   `json:"id"`    // TimeStamp for this as an ID
		Volume    float64 `json:"vol"`   // Volume during this period
	}

	// HuobiSubscriptionMsg Msg to subscribe to one ticker channel at time.
	HuobiSubscriptionMsg struct {
		Sub string `json:"sub"` // channel to subscribe market.$symbol.ticker
	}

	// HuobiSubscriptionResp the response structure for a Huobi subscription response
	HuobiSubscriptionResp struct {
		Status string `json:"status"`
	}

	// HuobiPairsSummary defines the response structure for an Huobi pairs
	// summary.
	HuobiPairsSummary struct {
		Data []HuobiPairData `json:"data"`
	}

	// HuobiPairData defines the data response structure for an Huobi pair.
	HuobiPairData struct {
		Symbol string `json:"symbol"`
	}
)

// NewHuobiProvider returns a new Huobi provider with the WS connection and msg handler.
func NewHuobiProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*HuobiProvider, error) {
	websocketUrl := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   huobiWSPath,
	}
	provider := &HuobiProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		websocketUrl,
		provider.messageReceived,
		provider.getSubscriptionMsgs,
	)
	go provider.websocket.Start()
	return provider, nil
}

func (p *HuobiProvider) getSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 0, len(cps)*2)
	for _, cp := range cps {
		subscriptionMsgs = append(subscriptionMsgs, newHuobiTickerSubscriptionMsg(cp))
		subscriptionMsgs = append(subscriptionMsgs, newHuobiCandleSubscriptionMsg(cp))
	}
	return subscriptionMsgs
}

// messageReceived handles the received data from the Huobi websocket. All return
// data of websocket Market APIs are compressed with GZIP so they need to be
// decompressed.
func (p *HuobiProvider) messageReceived(messageType int, bz []byte) {
	if messageType != websocket.BinaryMessage {
		return
	}

	bz, err := decompressGzip(bz)
	if err != nil {
		p.logger.Err(err).Msg("failed to decompress gziped message")
		return
	}

	if bytes.Contains(bz, ping) {
		p.pong(bz)
		return
	}

	var (
		tickerResp    HuobiTicker
		tickerErr     error
		candleResp    HuobiCandle
		candleErr     error
		subscribeResp HuobiSubscriptionResp
	)

	// sometimes the message received is not a ticker or a candle response.
	tickerErr = json.Unmarshal(bz, &tickerResp)
	if tickerResp.Tick.LastPrice != 0 {
		p.setTickerPair(tickerResp)
		telemetryWebsocketMessage(ProviderHuobi, MessageTypeTicker)
		return
	}

	candleErr = json.Unmarshal(bz, &candleResp)
	if candleResp.Tick.Close != 0 {
		p.setCandlePair(candleResp)
		telemetryWebsocketMessage(ProviderHuobi, MessageTypeCandle)
		return
	}

	err = json.Unmarshal(bz, &subscribeResp)
	if subscribeResp.Status == "ok" {
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		AnErr("candle", candleErr).
		AnErr("subscribeResp", err).
		Msg("Error on receive message")
}

// pong return a heartbeat message when a "ping" is received and reset the
// reconnect ticker because the connection is alive. After connected to Huobi's
// Websocket server, the server will send heartbeat periodically (5s interval).
// When client receives an heartbeat message, it should respond with a matching
// "pong" message which has the same integer in it, e.g. {"ping": 1492420473027}
// and then the return pong message should be {"pong": 1492420473027}.
func (p *HuobiProvider) pong(bz []byte) {
	var heartbeat struct {
		Ping uint64 `json:"ping"`
	}

	if err := json.Unmarshal(bz, &heartbeat); err != nil {
		p.logger.Err(err).Msg("could not unmarshal heartbeat")
		return
	}

	if err := p.websocket.SendJSON(struct {
		Pong uint64 `json:"pong"`
	}{Pong: heartbeat.Ping}); err != nil {
		p.logger.Err(err).Msg("could not send pong message back")
	}
}

func (p *HuobiProvider) setTickerPair(ticker HuobiTicker) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	symbol, err := huobiChannelToSymbol(ticker.CH)
	if err != nil {
		p.logger.Warn().Err(err).Str("channel", ticker.CH).Msg("failed to convert ticker")
	}
	price, err := ticker.toTickerPrice()
	if err != nil {
		p.logger.Warn().Err(err).Str("symbol", symbol).Msg("failed to convert ticker")
	}
	p.tickers[symbol] = price
}

func (p *HuobiProvider) setCandlePair(candle HuobiCandle) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	// convert huobi timestamp seconds -> milliseconds
	candle.Tick.TimeStamp = SecondsToMilli(candle.Tick.TimeStamp)
	staleTime := PastUnixTime(providerCandlePeriod)
	candleList := []types.CandlePrice{}
	symbol, err := huobiChannelToSymbol(candle.CH)
	if err != nil {
		p.logger.Warn().Err(err).Str("channel", candle.CH).Msg("failed to convert candle")
	}
	price, err := candle.toCandlePrice()
	if err != nil {
		p.logger.Warn().Err(err).Str("symbol", symbol).Msg("failed to convert candle")
	}
	candleList = append(candleList, price)
	for _, c := range p.candles[symbol] {
		if staleTime < c.TimeStamp {
			candleList = append(candleList, c)
		}
	}
	p.candles[symbol] = candleList
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
func (p *HuobiProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + huobiRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary HuobiPairsSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary.Data))
	for _, pair := range pairsSummary.Data {
		availablePairs[strings.ToUpper(pair.Symbol)] = struct{}{}
	}

	return availablePairs, nil
}

// decompressGzip uncompress gzip compressed messages. All data returned from the
// websocket Market APIs is compressed with GZIP, so it needs to be unzipped.
func decompressGzip(bz []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(bz))
	if err != nil {
		return nil, err
	}

	return io.ReadAll(r)
}

// toTickerPrice converts current HuobiTicker to TickerPrice.
func (ticker HuobiTicker) toTickerPrice() (types.TickerPrice, error) {
	return types.NewTickerPrice(
		string(ProviderHuobi),
		ticker.CH,
		strconv.FormatFloat(ticker.Tick.LastPrice, 'f', -1, 64),
		strconv.FormatFloat(ticker.Tick.Vol, 'f', -1, 64),
	)
}

func (candle HuobiCandle) toCandlePrice() (types.CandlePrice, error) {
	return types.NewCandlePrice(
		string(ProviderHuobi),
		candle.CH,
		strconv.FormatFloat(candle.Tick.Close, 'f', -1, 64),
		strconv.FormatFloat(candle.Tick.Volume, 'f', -1, 64),
		candle.Tick.TimeStamp,
	)
}

// newHuobiTickerSubscriptionMsg returns a new ticker subscription Msg.
func newHuobiTickerSubscriptionMsg(cp types.CurrencyPair) HuobiSubscriptionMsg {
	return HuobiSubscriptionMsg{
		Sub: currencyPairToHuobiTickerPair(cp),
	}
}

// currencyPairToHuobiTickerPair returns the channel name in the following format:
// "market.$symbol.ticker".
func currencyPairToHuobiTickerPair(cp types.CurrencyPair) string {
	return strings.ToLower("market." + cp.String() + ".ticker")
}

// newHuobiSubscriptionMsg returns a new candle subscription Msg.
func newHuobiCandleSubscriptionMsg(cp types.CurrencyPair) HuobiSubscriptionMsg {
	return HuobiSubscriptionMsg{
		Sub: currencyPairToHuobiCandlePair(cp),
	}
}

// currencyPairToHuobiCandlePair returns the channel name in the following format:
// "market.$symbol.line.$period".
func currencyPairToHuobiCandlePair(cp types.CurrencyPair) string {
	return strings.ToLower("market." + cp.String() + ".kline.1min")
}

// convert candle or ticker channels from "market.symbol.xxxxx" to "SYMBOL"
func huobiChannelToSymbol(channel string) (string, error) {
	fields := strings.Split(channel, ".")
	if len(fields) < 3 || len(fields[1]) < 1 {
		return "", fmt.Errorf("invalid channel name")
	}
	return strings.ToUpper(fields[1]), nil
}