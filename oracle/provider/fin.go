package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"
	"time"

	"price-feeder/oracle/types"
	"github.com/rs/zerolog"
)

const (
	finPairsEndpoint         = "/api/coingecko/pairs"
	finTickersEndpoint       = "/api/coingecko/tickers"
	finCandlesEndpoint       = "/api/trades/candles"
	finCandleBinSizeMinutes  = 5
	finCandleWindowSizeHours = 240
)

var _ Provider = (*FinProvider)(nil)

type (
	FinProvider struct {
		provider
	}

	FinTickers struct {
		Tickers []FinTicker `json:"tickers"`
	}

	FinTicker struct {
		Base   string `json:"base_currency"`
		Target string `json:"target_currency"`
		Symbol string `json:"ticker_id"`
		Price  string `json:"last_price"`
		Volume string `json:"base_volume"`
	}

	FinCandles struct {
		Candles []FinCandle `json:"candles"`
	}

	FinCandle struct {
		Bin    string `json:"bin"`
		Close  string `json:"close"`
		Volume string `json:"volume"`
	}

	FinPairs struct {
		Pairs []FinPair `json:"pairs"`
	}

	FinPair struct {
		Base    string `json:"base"`
		Target  string `json:"target"`
		Symbol  string `json:"ticker_id"`
		Address string `json:"pool_id"`
	}
)

func NewFinProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*FinProvider, error) {
	provider := &FinProvider{}
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

func (p *FinProvider) poll() {
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

func (p *FinProvider) updateTickers() error {
	path := fmt.Sprintf("%s%s", p.endpoints.Rest, finTickersEndpoint)
	tickerResponse, err := p.rest.Get(path)
	if err != nil {
		return err
	}
	defer tickerResponse.Body.Close()
	tickerContent, err := ioutil.ReadAll(tickerResponse.Body)
	if err != nil {
		return err
	}
	var tickers FinTickers
	err = json.Unmarshal(tickerContent, &tickers)
	if err != nil {
		return err
	}
	p.mtx.Lock()
	defer p.mtx.Unlock()
	now := time.Now()
	for _, ticker := range tickers.Tickers {
		symbol := strings.ToUpper(strings.ReplaceAll(ticker.Symbol, "_", ""))
		_, ok := p.pairs[symbol]
		if !ok {
			continue  // FIN does not filter tickers in the response so we do it here
		}
		p.tickers[symbol] = types.TickerPrice{
			Price: strToDec(ticker.Price), 
			Volume: strToDec(ticker.Volume),
			Time: now,
		}
	}
	return nil
}

func (p *FinProvider) updateCandles() error {
	pairAddresses, err := p.getFinPairAddresses()
	if err != nil {
		return err
	}
	allCandlePrices := make(map[string][]types.CandlePrice, len(p.pairs))
	for _, pair := range p.pairs {
		address, ok := pairAddresses[pair.String()]
		if !ok {
			return fmt.Errorf("contract address lookup failed for pair: %s", pair.String())
		}
		windowEndTime := time.Now()
		windowStartTime := windowEndTime.Add(-finCandleWindowSizeHours * time.Hour)
		path := fmt.Sprintf("%s%s?contract=%s&precision=%d&from=%s&to=%s",
			p.endpoints.Rest,
			finCandlesEndpoint,
			address,
			finCandleBinSizeMinutes,
			windowStartTime.Format(time.RFC3339),
			windowEndTime.Format(time.RFC3339),
		)
		candlesResponse, err := p.rest.Get(path)
		if err != nil {
			return err
		}
		defer candlesResponse.Body.Close()
		candlesContent, err := ioutil.ReadAll(candlesResponse.Body)
		if err != nil {
			return err
		}
		var candles FinCandles
		err = json.Unmarshal(candlesContent, &candles)
		if err != nil {
			return err
		}
		candlePrices := []types.CandlePrice{}
		for _, candle := range candles.Candles {
			timeStamp, err := finBinToTimeStamp(candle.Bin)
			if err != nil {
				return fmt.Errorf("FIN candle timestamp failed to parse: %w", err)
			}
			candlePrices = append(candlePrices, types.CandlePrice{
				Price:     strToDec(candle.Close),
				Volume:    strToDec(candle.Volume),
				TimeStamp: timeStamp,
			})
		}
		allCandlePrices[pair.String()] = candlePrices
	}
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.candles = allCandlePrices
	return nil
}

func (p *FinProvider) GetAvailablePairs() (map[string]struct{}, error) {
	finPairs, err := p.getFinPairs()
	if err != nil {
		return nil, err
	}
	availablePairs := make(map[string]struct{}, len(finPairs.Pairs))
	for _, pair := range finPairs.Pairs {
		pair := types.CurrencyPair{
			Base:  strings.ToUpper(pair.Base),
			Quote: strings.ToUpper(pair.Target),
		}
		availablePairs[pair.String()] = struct{}{}
	}
	return availablePairs, nil
}

func (p *FinProvider) getFinPairs() (FinPairs, error) {
	path := fmt.Sprintf("%s%s", p.endpoints.Rest, finPairsEndpoint)
	pairsResponse, err := p.rest.Get(path)
	if err != nil {
		return FinPairs{}, err
	}
	defer pairsResponse.Body.Close()
	var pairs FinPairs
	err = json.NewDecoder(pairsResponse.Body).Decode(&pairs)
	if err != nil {
		return FinPairs{}, err
	}
	return pairs, nil
}

func (p *FinProvider) getFinPairAddresses() (map[string]string, error) {
	finPairs, err := p.getFinPairs()
	if err != nil {
		return nil, err
	}
	pairAddresses := make(map[string]string, len(finPairs.Pairs))
	for _, pair := range finPairs.Pairs {
		pairAddresses[strings.ToUpper(pair.Base+pair.Target)] = pair.Address
	}
	return pairAddresses, nil
}

func finBinToTimeStamp(bin string) (int64, error) {
	timeParsed, err := time.Parse(time.RFC3339, bin)
	if err != nil {
		return -1, err
	}
	return timeParsed.Unix(), nil
}
