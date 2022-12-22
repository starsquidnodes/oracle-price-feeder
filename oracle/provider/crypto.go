package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"price-feeder/oracle/types"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	cryptoWSPath             = "/v2/market"
	cryptoReconnectTime      = time.Second * 30
	cryptoRestPath           = "/v2/public/get-ticker"
	cryptoTickerChannel      = "ticker"
	cryptoCandleChannel      = "candlestick"
	cryptoHeartbeatMethod    = "public/heartbeat"
	cryptoHeartbeatReqMethod = "public/respond-heartbeat"
	cryptoTickerMsgPrefix    = "ticker."
	cryptoCandleMsgPrefix    = "candlestick.5m."
)

var _ Provider = (*CryptoProvider)(nil)

type (
	// CryptoProvider defines an Oracle provider implemented by the Crypto.com public
	// API.
	//
	// REF: https://exchange-docs.crypto.com/spot/index.html#introduction
	CryptoProvider struct {
		provider
	}

	CryptoTickerResponse struct {
		Result CryptoTickerResult `json:"result"`
	}
	CryptoTickerResult struct {
		InstrumentName string         `json:"instrument_name"` // ex.: ATOM_USDT
		Channel        string         `json:"channel"`         // ex.: ticker
		Data           []CryptoTicker `json:"data"`            // ticker data
	}
	CryptoTicker struct {
		InstrumentName string `json:"i"` // Instrument Name, e.g. BTC_USDT, ETH_CRO, etc.
		Volume         string `json:"v"` // The total 24h traded volume
		LatestTrade    string `json:"a"` // The price of the latest trade, null if there weren't any trades
	}

	CryptoCandleResponse struct {
		Result CryptoCandleResult `json:"result"`
	}
	CryptoCandleResult struct {
		InstrumentName string         `json:"instrument_name"` // ex.: ATOM_USDT
		Channel        string         `json:"channel"`         // ex.: candlestick
		Data           []CryptoCandle `json:"data"`            // candlestick data
	}
	CryptoCandle struct {
		Close     string `json:"c"` // Price at close
		Volume    string `json:"v"` // Volume during interval
		Timestamp int64  `json:"t"` // End time of candlestick (Unix timestamp)
	}

	CryptoSubscriptionMsg struct {
		ID     int64                    `json:"id"`
		Method string                   `json:"method"` // subscribe, unsubscribe
		Params CryptoSubscriptionParams `json:"params"`
		Nonce  int64                    `json:"nonce"` // Current timestamp (milliseconds since the Unix epoch)
	}
	CryptoSubscriptionParams struct {
		Channels []string `json:"channels"` // Channels to be subscribed ex. ticker.ATOM_USDT
	}

	CryptoPairsSummary struct {
		Result CryptoInstruments `json:"result"`
	}
	CryptoInstruments struct {
		Data []CryptoTicker `json:"data"`
	}

	CryptoHeartbeatResponse struct {
		ID     int64  `json:"id"`
		Method string `json:"method"` // public/heartbeat
	}
	CryptoHeartbeatRequest struct {
		ID     int64  `json:"id"`
		Method string `json:"method"` // public/respond-heartbeat
	}
)

func NewCryptoProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*CryptoProvider, error) {
	websocketUrl := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   cryptoWSPath,
	}
	provider := &CryptoProvider{}
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

func (p *CryptoProvider) getSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 0, len(cps)*2)
	for _, cp := range cps {
		cryptoPair := currencyPairToCryptoPair(cp)
		channel := cryptoTickerMsgPrefix + cryptoPair
		msg := newCryptoSubscriptionMsg([]string{channel})
		subscriptionMsgs = append(subscriptionMsgs, msg)

		cryptoPair = currencyPairToCryptoPair(cp)
		channel = cryptoCandleMsgPrefix + cryptoPair
		msg = newCryptoSubscriptionMsg([]string{channel})
		subscriptionMsgs = append(subscriptionMsgs, msg)
	}
	return subscriptionMsgs
}

func (p *CryptoProvider) messageReceived(messageType int, bz []byte) {
	if messageType != websocket.TextMessage {
		return
	}

	var (
		heartbeatResp CryptoHeartbeatResponse
		heartbeatErr  error
		tickerResp    CryptoTickerResponse
		tickerErr     error
		candleResp    CryptoCandleResponse
		candleErr     error
	)

	// sometimes the message received is not a ticker or a candle response.
	heartbeatErr = json.Unmarshal(bz, &heartbeatResp)
	if heartbeatResp.Method == cryptoHeartbeatMethod {
		p.pong(heartbeatResp)
		return
	}

	tickerErr = json.Unmarshal(bz, &tickerResp)
	if tickerResp.Result.Channel == cryptoTickerChannel {
		for _, tickerPair := range tickerResp.Result.Data {
			p.setTickerPair(
				strings.ReplaceAll(tickerResp.Result.InstrumentName, "_", ""),
				tickerPair,
			)
			telemetryWebsocketMessage(ProviderCrypto, MessageTypeTicker)
		}
		return
	}

	candleErr = json.Unmarshal(bz, &candleResp)
	if candleResp.Result.Channel == cryptoCandleChannel {
		for _, candlePair := range candleResp.Result.Data {
			p.setCandlePair(
				strings.ReplaceAll(candleResp.Result.InstrumentName, "_", ""),
				candlePair,
			)
			telemetryWebsocketMessage(ProviderCrypto, MessageTypeCandle)
		}
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("heartbeat", heartbeatErr).
		AnErr("ticker", tickerErr).
		AnErr("candle", candleErr).
		Msg("Error on receive message")
}

// pong return a heartbeat message when a "ping" is received and reset the
// recconnect ticker because the connection is alive. After connected to crypto.com's
// Websocket server, the server will send heartbeat periodically (30s interval).
// When client receives an heartbeat message, it must respond back with the
// public/respond-heartbeat method, using the same matching id,
// within 5 seconds, or the connection will break.
func (p *CryptoProvider) pong(heartbeatResp CryptoHeartbeatResponse) {
	heartbeatReq := CryptoHeartbeatRequest{
		ID:     heartbeatResp.ID,
		Method: cryptoHeartbeatReqMethod,
	}

	if err := p.websocket.SendJSON(heartbeatReq); err != nil {
		p.logger.Err(err).Msg("could not send pong message back")
	}
}

func (p *CryptoProvider) setTickerPair(symbol string, tickerPair CryptoTicker) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	tickerPrice, err := types.NewTickerPrice(
		string(ProviderCrypto),
		symbol,
		tickerPair.LatestTrade,
		tickerPair.Volume,
	)
	if err != nil {
		p.logger.Warn().Err(err).Msg("crypto: failed to parse ticker")
		return
	}

	p.tickers[symbol] = tickerPrice
}

func (p *CryptoProvider) setCandlePair(symbol string, candlePair CryptoCandle) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	candle, err := types.NewCandlePrice(
		string(ProviderCrypto),
		symbol,
		candlePair.Close,
		candlePair.Volume,
		SecondsToMilli(candlePair.Timestamp),
	)
	if err != nil {
		p.logger.Warn().Err(err).Msg("crypto: failed to parse candle")
		return
	}

	staleTime := PastUnixTime(providerCandlePeriod)
	candleList := []types.CandlePrice{}
	candleList = append(candleList, candle)

	for _, c := range p.candles[symbol] {
		if staleTime < c.TimeStamp {
			candleList = append(candleList, c)
		}
	}

	p.candles[symbol] = candleList
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
// ex.: map["ATOMUSDT" => {}, "UMEEUSDC" => {}].
func (p *CryptoProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + cryptoRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary CryptoPairsSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary.Result.Data))
	for _, pair := range pairsSummary.Result.Data {
		splitInstName := strings.Split(pair.InstrumentName, "_")
		if len(splitInstName) != 2 {
			continue
		}

		cp := types.CurrencyPair{
			Base:  strings.ToUpper(splitInstName[0]),
			Quote: strings.ToUpper(splitInstName[1]),
		}

		availablePairs[cp.String()] = struct{}{}
	}

	return availablePairs, nil
}

// currencyPairToCryptoPair receives a currency pair and return crypto
// ticker symbol atomusdt@ticker.
func currencyPairToCryptoPair(cp types.CurrencyPair) string {
	return strings.ToUpper(cp.Base + "_" + cp.Quote)
}

// newCryptoSubscriptionMsg returns a new subscription Msg.
func newCryptoSubscriptionMsg(channels []string) CryptoSubscriptionMsg {
	return CryptoSubscriptionMsg{
		ID:     1,
		Method: "subscribe",
		Params: CryptoSubscriptionParams{
			Channels: channels,
		},
		Nonce: time.Now().UnixMilli(),
	}
}
