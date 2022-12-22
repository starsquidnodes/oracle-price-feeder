package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"price-feeder/oracle/types"
)

const (
	osmosisV2WSPath   = "ws"
	osmosisV2RestPath = "/assetpairs"
)

var _ Provider = (*OsmosisV2Provider)(nil)

type (
	// OsmosisV2Provider defines an Oracle provider implemented by UMEE's
	// Osmosis API.
	//
	// REF: https://github.com/umee-network/osmosis-api
	OsmosisV2Provider struct {
		provider
	}

	OsmosisV2Ticker struct {
		Price  string `json:"Price"`
		Volume string `json:"Volume"`
	}

	OsmosisV2Candle struct {
		Close   string `json:"Close"`
		Volume  string `json:"Volume"`
		EndTime int64  `json:"EndTime"`
	}

	// OsmosisV2PairsSummary defines the response structure for an Osmosis pairs
	// summary.
	OsmosisV2PairsSummary struct {
		Data []OsmosisPairData `json:"data"`
	}

	// OsmosisV2PairData defines the data response structure for an Osmosis pair.
	OsmosisV2PairData struct {
		Base  string `json:"base_symbol"`
		Quote string `json:"quote_symbol"`
	}
)

func NewOsmosisV2Provider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*OsmosisV2Provider, error) {
	websocketUrl := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   osmosisV2WSPath,
	}
	provider := &OsmosisV2Provider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		websocketUrl,
		provider.messageReceived,
		nil, // TODO implement subscribe handler
	)
	//go provider.websocket.Start()
	return nil, fmt.Errorf("osmosisv2 not implemented")
}

func (p *OsmosisV2Provider) messageReceived(messageType int, bz []byte) {
	// check if message is an ack first
	if string(bz) == "ack" {
		return
	}

	var (
		messageResp map[string]interface{}
		messageErr  error
		tickerResp  OsmosisV2Ticker
		tickerErr   error
		candleResp  []OsmosisV2Candle
		candleErr   error
	)

	messageErr = json.Unmarshal(bz, &messageResp)
	if messageErr != nil {
		p.logger.Error().
			Int("length", len(bz)).
			AnErr("message", messageErr).
			Msg("Error on receive message")
	}

	// Check the response for currency pairs that the provider is subscribed
	// to and determine whether it is a ticker or candle.
	for _, pair := range p.pairs {
		osmosisV2Pair := currencyPairToOsmosisV2Pair(pair)
		if msg, ok := messageResp[osmosisV2Pair]; ok {
			switch v := msg.(type) {
			// ticker response
			case map[string]interface{}:
				tickerString, _ := json.Marshal(v)
				tickerErr = json.Unmarshal(tickerString, &tickerResp)
				if tickerErr != nil {
					p.logger.Error().
						Int("length", len(bz)).
						AnErr("ticker", tickerErr).
						Msg("Error on receive message")
					continue
				}
				p.setTickerPair(
					pair.String(),
					tickerResp,
				)
				telemetryWebsocketMessage(ProviderOsmosisV2, MessageTypeTicker)
				continue

			// candle response
			case []interface{}:
				// use latest candlestick in list if there is one
				if len(v) == 0 {
					continue
				}
				candleString, _ := json.Marshal(v)
				candleErr = json.Unmarshal(candleString, &candleResp)
				if candleErr != nil {
					p.logger.Error().
						Int("length", len(bz)).
						AnErr("candle", candleErr).
						Msg("Error on receive message")
					continue
				}
				for _, singleCandle := range candleResp {
					p.setCandlePair(
						pair.String(),
						singleCandle,
					)
				}
				telemetryWebsocketMessage(ProviderOsmosisV2, MessageTypeCandle)
				continue
			}
		}
	}
}

func (p *OsmosisV2Provider) setTickerPair(symbol string, tickerPair OsmosisV2Ticker) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	price, err := sdk.NewDecFromStr(tickerPair.Price)
	if err != nil {
		p.logger.Warn().Err(err).Msg("osmosisv2: failed to parse ticker price")
		return
	}
	volume, err := sdk.NewDecFromStr(tickerPair.Volume)
	if err != nil {
		p.logger.Warn().Err(err).Msg("osmosisv2: failed to parse ticker volume")
		return
	}

	p.tickers[symbol] = types.TickerPrice{
		Price:  price,
		Volume: volume,
	}
}

func (p *OsmosisV2Provider) setCandlePair(symbol string, candlePair OsmosisV2Candle) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	close, err := sdk.NewDecFromStr(candlePair.Close)
	if err != nil {
		p.logger.Warn().Err(err).Msg("osmosisv2: failed to parse candle close")
		return
	}
	volume, err := sdk.NewDecFromStr(candlePair.Volume)
	if err != nil {
		p.logger.Warn().Err(err).Msg("osmosisv2: failed to parse candle volume")
		return
	}
	candle := types.CandlePrice{
		Price:     close,
		Volume:    volume,
		TimeStamp: candlePair.EndTime,
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
func (p *OsmosisV2Provider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + osmosisV2RestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary OsmosisV2PairsSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary.Data))
	for _, pair := range pairsSummary.Data {
		cp := types.CurrencyPair{
			Base:  strings.ToUpper(pair.Base),
			Quote: strings.ToUpper(pair.Quote),
		}
		availablePairs[cp.String()] = struct{}{}
	}

	return availablePairs, nil
}

// currencyPairToOsmosisV2Pair receives a currency pair and return osmosisv2
// ticker symbol atomusdt@ticker.
func currencyPairToOsmosisV2Pair(cp types.CurrencyPair) string {
	return strings.ToUpper(cp.Base + "/" + cp.Quote)
}
