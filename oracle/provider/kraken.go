package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"price-feeder/oracle/types"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	krakenRestPath = "/0/public/AssetPairs"
	krakenRestPathTickers = "/0/public/Ticker"
	krakenEventSystemStatus = "systemStatus"
	krakenEventSubscriptionStatus = "subscriptionStatus"
)

var (
	_ Provider = (*KrakenProvider)(nil)
	krakenDefaultEndpoints = Endpoint{
		Name: ProviderKraken,
		Rest: "https://api.kraken.com",
		Websocket: "ws.kraken.com",
		PingDuration: disabledPingDuration,
		PingType: websocket.PingMessage,
		PollInterval: 6 * time.Second,
	}
)

type (
	// KrakenProvider defines an Oracle provider implemented by the Kraken public
	// API.
	//
	// REF: https://docs.kraken.com/websockets/#overview
	KrakenProvider struct {
		provider
	}

	KrakenTickers struct {
		Result map[string]KrakenTicker `json:"result"`
	}

	// KrakenTicker ticker price response from Kraken ticker channel.
	// REF: https://docs.kraken.com/websockets/#message-ticker
	KrakenTicker struct {
		C []string `json:"c"` // Close with Price in the first position
		V []string `json:"v"` // Volume with the value over last 24 hours in the second position
	}

	// KrakenCandle candle response from Kraken candle channel.
	// REF: https://docs.kraken.com/websockets/#message-ohlc
	KrakenCandle struct {
		Close     string // Close price during this period
		TimeStamp int64  // Linux epoch timestamp
		Volume    string // Volume during this period
		Symbol    string // Symbol for this candle
	}

	// KrakenSubscriptionMsg Msg to subscribe to all the pairs at once.
	KrakenSubscriptionMsg struct {
		Event        string                    `json:"event"`        // subscribe/unsubscribe
		Pair         []string                  `json:"pair"`         // Array of currency pairs ex.: "BTC/USDT",
		Subscription KrakenSubscriptionChannel `json:"subscription"` // subscription object
	}

	// KrakenSubscriptionChannel Msg with the channel name to be subscribed.
	KrakenSubscriptionChannel struct {
		Name string `json:"name"` // channel to be subscribed ex.: ticker
	}

	// KrakenEvent wraps the possible events from the provider.
	KrakenEvent struct {
		Event string `json:"event"` // events from kraken ex.: systemStatus | subscriptionStatus
	}

	// KrakenEventSubscriptionStatus parse the subscriptionStatus event message.
	KrakenEventSubscriptionStatus struct {
		Status       string `json:"status"`       // subscribed|unsubscribed|error
		Pair         string `json:"pair"`         // Pair symbol base/quote ex.: "XBT/USD"
		ErrorMessage string `json:"errorMessage"` // error description
	}

	// KrakenPairsSummary defines the response structure for an Kraken pairs summary.
	KrakenPairsSummary struct {
		Result map[string]KrakenPairData `json:"result"`
	}

	// KrakenPairData defines the data response structure for an Kraken pair.
	KrakenPairData struct {
		WsName string `json:"wsname"`
	}
)

// NewKrakenProvider returns a new Kraken provider with the WS connection and msg handler.
func NewKrakenProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*KrakenProvider, error) {
	websocketUrl := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
	}
	provider := &KrakenProvider{}
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

func (p *KrakenProvider) poll() {
	for {
		err := p.updateTickers()
		if err != nil {
			p.logger.Warn().Err(err).Msg("failed to update tickers")
		}
		time.Sleep(p.endpoints.PollInterval)
	}
}

func (p *KrakenProvider) updateTickers() error {
	url := fmt.Sprintf("%s%s?pair=", p.endpoints.Rest, krakenRestPathTickers)
	symbols := map[string]string{}
	for symbol, pair := range p.pairs {
		krakenSymbol := krakenPairToSymbol(pair)
		symbols[krakenSymbol] = symbol
		url += symbol + ","
	}
	url = url[:len(url) - 1]
	tickersResponse, err := p.rest.Get(url)
	if err != nil {
		return err
	}
	defer tickersResponse.Body.Close()
	tickersContent, err := ioutil.ReadAll(tickersResponse.Body)
	if err != nil {
		return err
	}
	var tickers KrakenTickers
	err = json.Unmarshal(tickersContent, &tickers)
	if err != nil {
		return err
	}
	p.mtx.Lock()
	defer p.mtx.Unlock()
	for krakenSymbol, ticker := range tickers.Result {
		price, err := ticker.toTickerPrice(krakenSymbol)
		if err != nil {
			p.logger.Warn().Err(err).Str("symbol", krakenSymbol).Msg("failed to convert ticker price")
		} else {
			p.tickers[symbols[krakenSymbol]] = price
		}
		p.logger.Debug().Str("symbol", symbols[krakenSymbol]).Msg("updated ticker")
	}
	return nil
}

func (p *KrakenProvider) getSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 0, len(cps)*2)
	for _, cp := range cps {
		krakenPair := currencyPairToKrakenPair(cp)
		//subscriptionMsgs = append(subscriptionMsgs, newKrakenTickerSubscriptionMsg(krakenPair))
		subscriptionMsgs = append(subscriptionMsgs, newKrakenCandleSubscriptionMsg(krakenPair))
	}
	return subscriptionMsgs
}

func (candle KrakenCandle) toCandlePrice() (types.CandlePrice, error) {
	return types.NewCandlePrice(
		string(ProviderKraken),
		candle.Symbol,
		candle.Close,
		candle.Volume,
		candle.TimeStamp,
	)
}

// messageReceived handles any message sent by the provider.
func (p *KrakenProvider) messageReceived(messageType int, bz []byte) {
	if messageType != websocket.TextMessage {
		return
	}

	var (
		krakenEvent KrakenEvent
		krakenErr   error
		tickerErr   error
		candleErr   error
	)

	krakenErr = json.Unmarshal(bz, &krakenEvent)
	if krakenErr == nil {
		switch krakenEvent.Event {
		case krakenEventSystemStatus:
			return
		case krakenEventSubscriptionStatus:
			p.messageReceivedSubscriptionStatus(bz)
			return
		}
		return
	}

	tickerErr = p.messageReceivedTickerPrice(bz)
	if tickerErr == nil {
		return
	}

	candleErr = p.messageReceivedCandle(bz)
	if candleErr == nil {
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		AnErr("candle", candleErr).
		AnErr("event", krakenErr).
		Msg("Error on receive message")
}

// messageReceivedTickerPrice handles the ticker price msg.
func (p *KrakenProvider) messageReceivedTickerPrice(bz []byte) error {
	// the provider response is an array with different types at each index
	// kraken documentation https://docs.kraken.com/websockets/#message-ticker
	var tickerMessage []interface{}
	if err := json.Unmarshal(bz, &tickerMessage); err != nil {
		return err
	}

	if len(tickerMessage) != 4 {
		return fmt.Errorf("received an unexpected structure")
	}

	channelName, ok := tickerMessage[2].(string)
	if !ok || channelName != "ticker" {
		return fmt.Errorf("received an unexpected channel name")
	}

	tickerBz, err := json.Marshal(tickerMessage[1])
	if err != nil {
		p.logger.Err(err).Msg("could not marshal ticker message")
		return err
	}

	var krakenTicker KrakenTicker
	if err := json.Unmarshal(tickerBz, &krakenTicker); err != nil {
		p.logger.Err(err).Msg("could not unmarshal ticker message")
		return err
	}

	krakenPair, ok := tickerMessage[3].(string)
	if !ok {
		p.logger.Debug().Msg("received an unexpected pair")
		return err
	}

	krakenPair = normalizeKrakenBTCPair(krakenPair)
	currencyPairSymbol := krakenPairToCurrencyPairSymbol(krakenPair)

	tickerPrice, err := krakenTicker.toTickerPrice(currencyPairSymbol)
	if err != nil {
		p.logger.Err(err).Msg("could not parse kraken ticker to ticker price")
		return err
	}

	p.setTickerPair(currencyPairSymbol, tickerPrice)
	telemetryWebsocketMessage(ProviderKraken, MessageTypeTicker)
	return nil
}

func (candle *KrakenCandle) UnmarshalJSON(buf []byte) error {
	var tmp []interface{}
	if err := json.Unmarshal(buf, &tmp); err != nil {
		return err
	}
	if len(tmp) != 9 {
		return fmt.Errorf("wrong number of fields in candle")
	}

	// timestamps come as a float string
	time, ok := tmp[1].(string)
	if !ok {
		return fmt.Errorf("time field must be a string")
	}
	timeFloat, err := strconv.ParseFloat(time, 64)
	if err != nil {
		return fmt.Errorf("unable to convert time to float")
	}
	candle.TimeStamp = int64(timeFloat)

	close, ok := tmp[5].(string)
	if !ok {
		return fmt.Errorf("close field must be a string")
	}
	candle.Close = close

	volume, ok := tmp[7].(string)
	if !ok {
		return fmt.Errorf("volume field must be a string")
	}
	candle.Volume = volume

	return nil
}

// messageReceivedCandle handles the candle msg.
func (p *KrakenProvider) messageReceivedCandle(bz []byte) error {
	// the provider response is an array with different types at each index
	// kraken documentation https://docs.kraken.com/websockets/#message-ohlc
	var candleMessage []interface{}
	if err := json.Unmarshal(bz, &candleMessage); err != nil {
		return err
	}

	if len(candleMessage) != 4 {
		return fmt.Errorf("received something different than candle")
	}

	channelName, ok := candleMessage[2].(string)
	if !ok || channelName != "ohlc-1" {
		return fmt.Errorf("received an unexpected channel name")
	}

	tickerBz, err := json.Marshal(candleMessage[1])
	if err != nil {
		return fmt.Errorf("could not marshal candle message")
	}

	var krakenCandle KrakenCandle
	if err := krakenCandle.UnmarshalJSON(tickerBz); err != nil {
		return err
	}

	krakenPair, ok := candleMessage[3].(string)
	if !ok {
		return fmt.Errorf("received an unexpected pair")
	}

	krakenPair = normalizeKrakenBTCPair(krakenPair)
	currencyPairSymbol := krakenPairToCurrencyPairSymbol(krakenPair)
	krakenCandle.Symbol = currencyPairSymbol

	telemetryWebsocketMessage(ProviderKraken, MessageTypeCandle)
	p.setCandlePair(krakenCandle)
	return nil
}

// messageReceivedSubscriptionStatus handle the subscription status message
// sent by the provider.
func (p *KrakenProvider) messageReceivedSubscriptionStatus(bz []byte) {
	var subscriptionStatus KrakenEventSubscriptionStatus
	if err := json.Unmarshal(bz, &subscriptionStatus); err != nil {
		p.logger.Err(err).Msg("provider could not unmarshal KrakenEventSubscriptionStatus")
		return
	}

	switch subscriptionStatus.Status {
	case "error":
		p.logger.Error().Msg(subscriptionStatus.ErrorMessage)
	case "unsubscribed":
		p.logger.Info().Msgf("ticker %s was unsubscribed", subscriptionStatus.Pair)
	}
}

// setTickerPair sets an ticker to the map thread safe by the mutex.
func (p *KrakenProvider) setTickerPair(symbol string, ticker types.TickerPrice) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.tickers[symbol] = ticker
}

func (p *KrakenProvider) setCandlePair(candle KrakenCandle) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	// convert kraken timestamp seconds -> milliseconds
	candle.TimeStamp = SecondsToMilli(candle.TimeStamp)
	staleTime := PastUnixTime(providerCandlePeriod)
	candleList := []types.CandlePrice{}
	price, err := candle.toCandlePrice()
	if err != nil {
		p.logger.Warn().Err(err).Str("symbol", candle.Symbol).Msg("failed to convert candle price")
	} else {
		candleList = append(candleList, price)
	}
	for _, c := range p.candles[candle.Symbol] {
		if staleTime < c.TimeStamp {
			candleList = append(candleList, c)
		}
	}
	p.candles[krakenPairToCurrencyPairSymbol(candle.Symbol)] = candleList
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
func (p *KrakenProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := p.rest.Get(p.endpoints.Rest + krakenRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary KrakenPairsSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary.Result))
	for _, pair := range pairsSummary.Result {
		splitPair := strings.Split(pair.WsName, "/")
		if len(splitPair) != 2 {
			continue
		}

		cp := types.CurrencyPair{
			Base:  strings.ToUpper(splitPair[0]),
			Quote: strings.ToUpper(splitPair[1]),
		}
		availablePairs[cp.String()] = struct{}{}
	}

	return availablePairs, nil
}

// toTickerPrice return a TickerPrice based on the KrakenTicker.
func (ticker KrakenTicker) toTickerPrice(symbol string) (types.TickerPrice, error) {
	if len(ticker.C) != 2 || len(ticker.V) != 2 {
		return types.TickerPrice{}, fmt.Errorf("error converting KrakenTicker to TickerPrice")
	}
	// ticker.C has the Price in the first position.
	// ticker.V has the totla	Value over last 24 hours in the second position.
	return types.NewTickerPrice(string(ProviderKraken), symbol, ticker.C[0], ticker.V[1])
}

// newKrakenTickerSubscriptionMsg returns a new subscription Msg.
func newKrakenTickerSubscriptionMsg(pairs ...string) KrakenSubscriptionMsg {
	return KrakenSubscriptionMsg{
		Event: "subscribe",
		Pair:  pairs,
		Subscription: KrakenSubscriptionChannel{
			Name: "ticker",
		},
	}
}

// newKrakenSubscriptionMsg returns a new subscription Msg.
func newKrakenCandleSubscriptionMsg(pairs ...string) KrakenSubscriptionMsg {
	return KrakenSubscriptionMsg{
		Event: "subscribe",
		Pair:  pairs,
		Subscription: KrakenSubscriptionChannel{
			Name: "ohlc",
		},
	}
}

// krakenPairToCurrencyPairSymbol receives a kraken pair formated
// ex.: ATOM/USDT and return currencyPair Symbol ATOMUSDT.
func krakenPairToCurrencyPairSymbol(krakenPair string) string {
	return strings.ReplaceAll(krakenPair, "/", "")
}

// currencyPairToKrakenPair receives a currency pair
// and return kraken ticker symbol ATOM/USDT.
func currencyPairToKrakenPair(cp types.CurrencyPair) string {
	return strings.ToUpper(cp.Base + "/" + cp.Quote)
}

// normalizeKrakenBTCPair changes XBT pairs to BTC,
// since other providers list bitcoin as BTC.
func normalizeKrakenBTCPair(ticker string) string {
	return strings.Replace(ticker, "XBT", "BTC", 1)
}

func krakenPairToSymbol(pair types.CurrencyPair) string {
	base := pair.Base
	if base == "BTC" {
		base = "XBT"
	}
	quote := pair.Quote
	if quote == "BTC" {
		quote = "XBT"
	}
	return fmt.Sprintf("X%sZ%s", base, quote)
}