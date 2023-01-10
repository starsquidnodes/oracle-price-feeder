package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"price-feeder/oracle/types"
	"github.com/rs/zerolog"
)

const (
	osmosisTokenEndpoint  = "/tokens/v2"
	osmosisCandleEndpoint = "/tokens/v2/historical"
	osmosisPairsEndpoint  = "/pairs/v1/summary"
)

var (
	_ Provider = (*OsmosisProvider)(nil)
	osmosisDefaultEndpoints = Endpoint{
		Name: ProviderOsmosis,
		Rest: "https://api-osmosis.imperator.co",
	}
)

type (
	// OsmosisProvider defines an Oracle provider implemented by the Osmosis public
	// API.
	//
	// REF: https://api-osmosis.imperator.co/swagger/
	OsmosisProvider struct {
		provider
	}

	// OsmosisTokenResponse defines the response structure for an Osmosis token
	// request.
	OsmosisTokenResponse struct {
		Price  float64 `json:"price"`
		Symbol string  `json:"symbol"`
		Volume float64 `json:"volume_24h"`
	}

	// OsmosisCandleResponse defines the response structure for an Osmosis candle
	// request.
	OsmosisCandleResponse struct {
		Time   int64   `json:"time"`
		Close  float64 `json:"close"`
		Volume float64 `json:"volume"`
	}

	// OsmosisPairsSummary defines the response structure for an Osmosis pairs
	// summary.
	OsmosisPairsSummary struct {
		Data []OsmosisPairData `json:"data"`
	}

	// OsmosisPairData defines the data response structure for an Osmosis pair.
	OsmosisPairData struct {
		Base  string `json:"base_symbol"`
		Quote string `json:"quote_symbol"`
	}
)

func NewOsmosisProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*OsmosisProvider, error) {
	provider := &OsmosisProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		url.URL{},
		nil,
		nil,
	)
	go provider.poll()
	return provider, nil
}

func (p *OsmosisProvider) poll() {
	for {
		err := p.updateTickers()
		if err != nil {
			p.logger.Warn().Err(err).Msg("failed to update tickers")
		}
		err = p.updateCandles()
		if err != nil {
			p.logger.Warn().Err(err).Msg("failed to update candles")
		}
		time.Sleep(p.endpoints.PollInterval)
	}
}

func (p *OsmosisProvider) updateTickers() error {
	path := fmt.Sprintf("%s%s/all", p.endpoints.Rest, osmosisTokenEndpoint)
	resp, err := p.rest.Get(path)
	if err != nil {
		return err
	}
	err = checkHTTPStatus(resp)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bz, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var tokensResp []OsmosisTokenResponse
	if err := json.Unmarshal(bz, &tokensResp); err != nil {
		return err
	}
	baseDenomIdx := make(map[string]types.CurrencyPair)
	for _, pair := range p.pairs {
		baseDenomIdx[strings.ToUpper(pair.Base)] = pair
	}
	tickerPrices := make(map[string]types.TickerPrice, len(p.pairs))
	now := time.Now()
	for _, tr := range tokensResp {
		symbol := strings.ToUpper(tr.Symbol) // symbol == base in a currency pair
		pair, ok := baseDenomIdx[symbol]
		if !ok {
			// skip tokens that are not requested
			continue
		}
		if _, ok := tickerPrices[symbol]; ok {
			return fmt.Errorf("duplicate token found in Osmosis response: %s", symbol)
		}
		tickerPrices[pair.String()] = types.TickerPrice{
			Price: floatToDec(tr.Price),
			Volume: floatToDec(tr.Volume),
			Time: now,
		}
	}
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.tickers = tickerPrices
	return nil
}

func (p *OsmosisProvider) updateCandles() error {
	candles := make(map[string][]types.CandlePrice)
	for _, pair := range p.pairs {
		if _, ok := candles[pair.Base]; !ok {
			candles[pair.String()] = []types.CandlePrice{}
		}
		path := fmt.Sprintf("%s%s/%s/chart?tf=5", p.endpoints.Rest, osmosisCandleEndpoint, pair.Base)
		resp, err := p.rest.Get(path)
		if err != nil {
			return err
		}
		err = checkHTTPStatus(resp)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		bz, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		var candlesResp []OsmosisCandleResponse
		if err := json.Unmarshal(bz, &candlesResp); err != nil {
			return err
		}
		staleTime := PastUnixTime(providerCandlePeriod)
		candlePrices := []types.CandlePrice{}
		for _, responseCandle := range candlesResp {
			if staleTime >= responseCandle.Time {
				continue
			}
			candlePrices = append(candlePrices, types.CandlePrice{
				Price:  floatToDec(responseCandle.Close),
				Volume: floatToDec(responseCandle.Volume),
				// convert osmosis timestamp seconds -> milliseconds
				TimeStamp: SecondsToMilli(responseCandle.Time),
			})
		}
		candles[pair.String()] = candlePrices
	}
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.candles = candles
	return nil
}

// GetAvailablePairs return all available pairs symbol to susbscribe.
func (p *OsmosisProvider) GetAvailablePairs() (map[string]struct{}, error) {
	path := fmt.Sprintf("%s%s", p.endpoints.Rest, osmosisPairsEndpoint)

	resp, err := p.rest.Get(path)
	if err != nil {
		return nil, err
	}
	err = checkHTTPStatus(resp)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary OsmosisPairsSummary
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
