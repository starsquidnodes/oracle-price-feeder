package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"sync"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	defaultTimeout       = 10 * time.Second
	staleTickersCutoff   = 10 * time.Second
	providerCandlePeriod = 10 * time.Minute

	ProviderFin		  Name = "fin"
	ProviderKraken    Name = "kraken"
	ProviderBinance   Name = "binance"
	ProviderBinanceUS Name = "binanceus"
	ProviderOsmosis   Name = "osmosis"
	ProviderOsmosisV2 Name = "osmosisv2"
	ProviderHuobi     Name = "huobi"
	ProviderOkx       Name = "okx"
	ProviderGate      Name = "gate"
	ProviderCoinbase  Name = "coinbase"
	ProviderBitget    Name = "bitget"
	ProviderMexc      Name = "mexc"
	ProviderCrypto    Name = "crypto"
	ProviderMock      Name = "mock"
)

var ping = []byte("ping")

type (
	// Provider defines an interface an exchange price provider must implement.
	Provider interface {
		// GetTickerPrices returns the tickerPrices based on the provided pairs.
		GetTickerPrices(...types.CurrencyPair) (map[string]types.TickerPrice, error)

		// GetCandlePrices returns the candlePrices based on the provided pairs.
		GetCandlePrices(...types.CurrencyPair) (map[string][]types.CandlePrice, error)

		// GetAvailablePairs return all available pairs symbol to subscribe.
		GetAvailablePairs() (map[string]struct{}, error)

		// SubscribeCurrencyPairs sends subscription messages for the new currency
		// pairs and adds them to the providers subscribed pairs
		SubscribeCurrencyPairs(...types.CurrencyPair) error
	}

	provider struct {
		ctx context.Context
		endpoints Endpoint
		logger zerolog.Logger
		mtx sync.RWMutex
		pairs map[string]types.CurrencyPair
		tickers map[string]types.TickerPrice
		candles map[string][]types.CandlePrice
		rest *http.Client
		websocket *WebsocketController
	}

	// Name name of an oracle provider. Usually it is an exchange
	// but this can be any provider name that can give token prices
	// examples.: "binance", "osmosis", "kraken".
	Name string

	// AggregatedProviderPrices defines a type alias for a map
	// of provider -> asset -> TickerPrice
	AggregatedProviderPrices map[Name]map[string]types.TickerPrice

	// AggregatedProviderCandles defines a type alias for a map
	// of provider -> asset -> []types.CandlePrice
	AggregatedProviderCandles map[Name]map[string][]types.CandlePrice

	// Endpoint defines an override setting in our config for the
	// hardcoded rest and websocket api endpoints.
	Endpoint struct {
		// Name of the provider, ex. "binance"
		Name Name
		// Rest endpoint for the provider, ex. "https://api1.binance.com"
		Rest string
		// Websocket endpoint for the provider, ex. "stream.binance.com:9443"
		Websocket string
		// provider api poll interval
		PollInterval time.Duration
		// provider websocket ping duration
		PingDuration time.Duration
		// provider websocket ping message type
		PingType uint
	}
)

func (p *provider) Init(
	ctx context.Context,
	endpoints Endpoint,
	logger zerolog.Logger,
	pairs []types.CurrencyPair,
	websocketUrl url.URL,
	websocketMessageHandler MessageHandler,
	websocketSubscribeHandler SubscribeHandler,
) {
	p.ctx = ctx
	p.endpoints = endpoints
	p.endpoints.SetDefaults()
	p.logger = logger.With().Str("provider", p.endpoints.Name.String()).Logger()
	p.pairs = make(map[string]types.CurrencyPair, len(pairs))
	for _, pair := range pairs {
		p.pairs[pair.String()] = pair
	}
	p.tickers = make(map[string]types.TickerPrice, len(pairs))
	p.candles = make(map[string][]types.CandlePrice, len(pairs))
	p.rest = newDefaultHTTPClient()
	if p.endpoints.Websocket != "" {
		p.websocket = NewWebsocketController(
			ctx,
			p.endpoints.Name,
			websocketUrl,
			pairs,
			websocketMessageHandler,
			websocketSubscribeHandler,
			p.endpoints.PingDuration,
			p.endpoints.PingType,
			p.logger,
		)
	}
}

func (p *provider) GetTickerPrices(pairs ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	tickers := make(map[string]types.TickerPrice, len(pairs))
	for _, pair := range pairs {
		symbol := pair.String()
		price, ok := p.tickers[symbol]
		if !ok {
			p.logger.Warn().Str("pair", symbol).Msg("missing ticker price for pair")
		} else {
			if time.Since(price.Time) > staleTickersCutoff {
				p.logger.Warn().Str("pair", symbol).Time("time", price.Time).Msg("tickers data is stale")
			} else {
				tickers[symbol] = price
			}
		}
	}
	return tickers, nil
}

func (p *provider) GetCandlePrices(pairs ...types.CurrencyPair) (map[string][]types.CandlePrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	candles := make(map[string][]types.CandlePrice, len(pairs))
	for _, pair := range pairs {
		symbol := pair.String()
		candle, ok := p.candles[symbol]
		if !ok {
			p.logger.Warn().Str("symbol", symbol).Msg("missing candle prices for pair")
		} else {
			candles[symbol] = candle
		}
	}
	return candles, nil
}

func (p *provider) SubscribeCurrencyPairs(pairs ...types.CurrencyPair) error {
	if p.endpoints.Websocket == "" {
		return nil
	}
	p.mtx.Lock()
	defer p.mtx.Unlock()
	newPairs := p.addPairs(pairs...)
	return p.websocket.AddPairs(newPairs)
}

func (p *provider) addPairs(pairs ...types.CurrencyPair) []types.CurrencyPair {
	newPairs := []types.CurrencyPair{}
	for _, pair := range pairs {
		_, ok := p.pairs[pair.String()]
		if !ok {
			newPairs = append(newPairs, pair)
		}
	}
	return newPairs
}

func (e *Endpoint) SetDefaults() {
	var defaults Endpoint
	switch e.Name {
	case ProviderBinance:
		defaults = Endpoint{
			Name: ProviderBinance,
			Rest: "https://api1.binance.com",
			Websocket: "stream.binance.com:9443",
			PollInterval: 6 * time.Second,
			PingDuration: disabledPingDuration,
			PingType: websocket.PingMessage,
		}
	case ProviderBinanceUS:
		defaults = Endpoint{
			Name: ProviderBinanceUS,
			Rest: "https://api.binance.us",
			Websocket: "stream.binance.us:9443",
			PollInterval: 6 * time.Second,
			PingDuration: disabledPingDuration,
			PingType: websocket.PingMessage,
		}
	case ProviderBitget:
		defaults = Endpoint{
			Name: ProviderBitget,
			Rest: "https://api.bitget.com",
			Websocket: "ws.bitget.com",
			PingDuration: defaultPingDuration,
			PingType: websocket.TextMessage,
		}
	case ProviderCoinbase:
		defaults =  Endpoint{
			Name: ProviderCoinbase,
			Rest: "https://api.exchange.coinbase.com",
			Websocket: "ws-feed.exchange.coinbase.com",
			PingDuration: defaultPingDuration,
			PingType: websocket.PingMessage,
		}
	case ProviderCrypto:
		defaults = Endpoint{
			Name: ProviderCrypto,
			Rest: "https://api.crypto.com",
			Websocket: "stream.crypto.com",
			PingDuration: disabledPingDuration,
			PingType: websocket.PingMessage,
		}
	case ProviderFin:
		defaults = Endpoint{
			Name: ProviderFin,
			Rest: "https://api.kujira.app",
		}
	case ProviderGate:
		defaults = Endpoint{
			Name: ProviderGate,
			Rest: "https://api.gateio.ws",
			Websocket: "ws.gate.io",
			PingDuration: defaultPingDuration,
			PingType: websocket.PingMessage,
		}
	case ProviderHuobi:
		defaults = Endpoint{
			Name: ProviderHuobi,
			Rest: "https://api.huobi.pro",
			Websocket: "api-aws.huobi.pro",
			PingDuration: disabledPingDuration,
			PingType: websocket.PingMessage,
		}
	case ProviderKraken:
		defaults = Endpoint{
			Name: ProviderKraken,
			Rest: "https://api.kraken.com",
			Websocket: "ws.kraken.com",
			PingDuration: disabledPingDuration,
			PingType: websocket.PingMessage,
		}
	case ProviderMexc:
		defaults = Endpoint{
			Name: ProviderMexc,
			Rest: "https://www.mexc.com",
			Websocket: "wbs.mexc.com",
			PingDuration: defaultPingDuration,
			PingType: websocket.PingMessage,
		}
	case ProviderOkx:
		defaults = Endpoint{
			Name: ProviderOkx,
			Rest: "https://www.okx.com",
			Websocket: "ws.okx.com:8443",
			PingDuration: defaultPingDuration,
			PingType: websocket.PingMessage,
		}
	case ProviderOsmosis:
		defaults = Endpoint{
			Name: ProviderOsmosis,
			Rest: "https://api-osmosis.imperator.co",
		}
	case ProviderOsmosisV2:
		defaults = Endpoint{
			Name:      ProviderOsmosisV2,
			Rest:      "https://api.osmo-api.network.umee.cc",
			Websocket: "api.osmo-api.network.umee.cc",
			PingDuration: defaultPingDuration,
			PingType: websocket.PingMessage,
		}
	default:
		return
	}
	if e.Rest == "" {
		e.Rest = defaults.Rest
	}
	if e.Websocket == "" {
		e.Websocket = defaults.Websocket
	}
	if e.PollInterval == time.Duration(0) {
		e.PollInterval = defaults.PollInterval
	}
	if e.PingDuration == time.Duration(0) {
		e.PingDuration = defaults.PingDuration
	}
	if e.PingType == 0 {
		e.PingType = defaults.PingType
	}
}

// String cast provider name to string.
func (n Name) String() string {
	return string(n)
}

// preventRedirect avoid any redirect in the http.Client the request call
// will not return an error, but a valid response with redirect response code.
func preventRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

func newDefaultHTTPClient() *http.Client {
	return newHTTPClientWithTimeout(defaultTimeout)
}

func newHTTPClientWithTimeout(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		CheckRedirect: preventRedirect,
	}
}

// PastUnixTime returns a millisecond timestamp that represents the unix time
// minus t.
func PastUnixTime(t time.Duration) int64 {
	return time.Now().Add(t*-1).Unix() * int64(time.Second/time.Millisecond)
}

// SecondsToMilli converts seconds to milliseconds for our unix timestamps.
func SecondsToMilli(t int64) int64 {
	return t * int64(time.Second/time.Millisecond)
}

func checkHTTPStatus(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
	return nil
}

func strToDec(str string) sdk.Dec {
	if strings.Contains(str, ".") {
		split := strings.Split(str, ".")
		if len(split[1]) > 18 {
			// sdk.MustNewDecFromStr will panic if decimal precision is greater than 18
			str = split[0] + "." + split[1][0:18]
		}
	}
	return sdk.MustNewDecFromStr(str)
}

func floatToDec(f float64) sdk.Dec {
	return sdk.MustNewDecFromStr(strconv.FormatFloat(f, 'f', -1, 64))
}