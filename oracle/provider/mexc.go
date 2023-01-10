package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"
	"github.com/rs/zerolog"
	"github.com/gorilla/websocket"
	"price-feeder/oracle/types"
)

const (
	mexcWSPath   = "/raw/ws"
	mexcRestPath = "/open/api/v2/market/ticker"
)

var (
	_ Provider = (*MexcProvider)(nil)
	mexcDefaultEndpoints = Endpoint{
		Name: ProviderMexc,
		Rest: "https://www.mexc.com",
		Websocket: "wbs.mexc.com",
		PingDuration: defaultPingDuration,
		PingType: websocket.PingMessage,
	}
)

type (
	// MexcProvider defines an Oracle provider implemented by the Mexc public
	// API.
	//
	// REF: https://mxcdevelop.github.io/apidocs/spot_v2_en/#ticker-information
	// REF: https://mxcdevelop.github.io/apidocs/spot_v2_en/#k-line
	// REF: https://mxcdevelop.github.io/apidocs/spot_v2_en/#overview
	MexcProvider struct {
		provider
	}

	// MexcTickerResponse is the ticker price response object.
	MexcTickerResponse struct {
		Symbol map[string]MexcTicker `json:"data"` // e.x. ATOM_USDT
	}
	MexcTicker struct {
		LastPrice float64 `json:"p"` // Last price ex.: 0.0025
		Volume    float64 `json:"v"` // Total traded base asset volume ex.: 1000
	}

	// MexcCandle is the candle websocket response object.
	MexcCandleResponse struct {
		Symbol   string     `json:"symbol"` // Symbol ex.: ATOM_USDT
		Metadata MexcCandle `json:"data"`   // Metadata for candle
	}
	MexcCandle struct {
		Close     float64 `json:"c"` // Price at close
		TimeStamp int64   `json:"t"` // Close time in unix epoch ex.: 1645756200000
		Volume    float64 `json:"v"` // Volume during period
	}

	// MexcCandleSubscription Msg to subscribe all the candle channels.
	MexcCandleSubscription struct {
		OP       string `json:"op"`       // kline
		Symbol   string `json:"symbol"`   // streams to subscribe ex.: atom_usdt
		Interval string `json:"interval"` // Min1、Min5、Min15、Min30
	}

	// MexcTickerSubscription Msg to subscribe all the ticker channels.
	MexcTickerSubscription struct {
		OP string `json:"op"` // kline
	}

	// MexcPairSummary defines the response structure for a Mexc pair
	// summary.
	MexcPairSummary struct {
		Symbol string `json:"symbol"`
	}
)

func NewMexcProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*MexcProvider, error) {
	websocketUrl := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   mexcWSPath,
	}
	provider := &MexcProvider{}
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

func (p *MexcProvider) getSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 0, len(cps)+1)
	for _, cp := range cps {
		mexcPair := currencyPairToMexcPair(cp)
		subscriptionMsgs = append(subscriptionMsgs, newMexcCandleSubscriptionMsg(mexcPair))
	}
	subscriptionMsgs = append(subscriptionMsgs, newMexcTickerSubscriptionMsg())
	return subscriptionMsgs
}

func (p *MexcProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerResp MexcTickerResponse
		tickerErr  error
		candleResp MexcCandleResponse
		candleErr  error
	)

	tickerErr = json.Unmarshal(bz, &tickerResp)
	for _, pair := range p.pairs {
		mexcPair := currencyPairToMexcPair(pair)
		if tickerResp.Symbol[mexcPair].LastPrice != 0 {
			p.setTickerPair(
				pair.String(),
				tickerResp.Symbol[mexcPair],
			)
			telemetryWebsocketMessage(ProviderMexc, MessageTypeTicker)
			return
		}
	}

	candleErr = json.Unmarshal(bz, &candleResp)
	if candleResp.Metadata.Close != 0 {
		p.setCandlePair(candleResp)
		telemetryWebsocketMessage(ProviderMexc, MessageTypeCandle)
		return
	}

	if tickerErr != nil || candleErr != nil {
		p.logger.Error().
			Int("length", len(bz)).
			AnErr("ticker", tickerErr).
			AnErr("candle", candleErr).
			Msg("mexc: Error on receive message")
	}
}

func (p *MexcProvider) setTickerPair(symbol string, ticker MexcTicker) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.tickers[symbol] = types.TickerPrice{
		Price:  floatToDec(ticker.LastPrice),
		Volume: floatToDec(ticker.Volume),
		Time: time.Now(),
	}
}

func (p *MexcProvider) setCandlePair(candleResp MexcCandleResponse) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	candle := types.CandlePrice{
		Price:  floatToDec(candleResp.Metadata.Close),
		Volume: floatToDec(candleResp.Metadata.Volume),
		// convert seconds -> milli
		TimeStamp: SecondsToMilli(candleResp.Metadata.TimeStamp),
	}

	staleTime := PastUnixTime(providerCandlePeriod)
	candleList := []types.CandlePrice{}
	candleList = append(candleList, candle)

	for _, c := range p.candles[candleResp.Symbol] {
		if staleTime < c.TimeStamp {
			candleList = append(candleList, c)
		}
	}

	p.candles[strings.ReplaceAll(candleResp.Symbol, "_", "")] = candleList
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
// ex.: map["ATOMUSDT" => {}, "UMEEUSDC" => {}].
func (p *MexcProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + mexcRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary []MexcPairSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary))
	for _, pairName := range pairsSummary {
		availablePairs[strings.ToUpper(pairName.Symbol)] = struct{}{}
	}

	return availablePairs, nil
}

// currencyPairToMexcPair receives a currency pair and return mexc
// ticker symbol atomusdt@ticker.
func currencyPairToMexcPair(cp types.CurrencyPair) string {
	return strings.ToUpper(cp.Base + "_" + cp.Quote)
}

// newMexcCandleSubscriptionMsg returns a new candle subscription Msg.
func newMexcCandleSubscriptionMsg(param string) MexcCandleSubscription {
	return MexcCandleSubscription{
		OP:       "sub.kline",
		Symbol:   param,
		Interval: "Min1",
	}
}

// newMexcTickerSubscriptionMsg returns a new ticker subscription Msg.
func newMexcTickerSubscriptionMsg() MexcTickerSubscription {
	return MexcTickerSubscription{
		OP: "sub.overview",
	}
}
