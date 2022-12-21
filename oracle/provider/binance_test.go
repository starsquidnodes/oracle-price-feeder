package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestBinanceProvider_GetTickerPrices(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		if req.URL.String() == `/api/v3/tickers?type=MINI&symbol=["ATOMUSDT"]` {
			res.Write([]byte(`[{"symbol": "ATOMUSDT", "lastPrice": "34.69000000", "Volume": "2396974.02000000"}]`))
		} else if req.URL.String() == `/api/v3/tickers?type=MINI&symbol=["ATOMUSDT","LUNAUSDT"]` {
			res.Write([]byte(`[{"symbol": "ATOMUSDT", "lastPrice": "34.69000000", "Volume": "2396974.02000000"},{"symbol": "LUNAUSDT", "lastPrice": ""41.35000000"", "Volume": "2396974.02000000"}]`))
		} else {
			panic("unexpected request")
		}
	}))
	defer server.Close()
	p, err := NewBinanceProvider(
		context.TODO(),
		zerolog.Nop(),
		Endpoint{Rest: server.URL},
		false,
		types.CurrencyPair{Base: "ATOM", Quote: "USDT"},
	)
	require.NoError(t, err)
	lastPriceAtom := "34.69000000"
	lastPriceLuna := "41.35000000"
	volume := "2396974.02000000"
	p.tickers = map[string]types.TickerPrice{
		"ATOMUSDT": {Price: sdk.MustNewDecFromStr(lastPriceAtom), Volume: sdk.MustNewDecFromStr(volume)},
		"LUNAUSDT": {Price: sdk.MustNewDecFromStr(lastPriceLuna), Volume: sdk.MustNewDecFromStr(volume)},
	}
	p.tickersSyncTime = time.Now()
	t.Run("valid_request_single_ticker", func(t *testing.T) {
		prices, err := p.GetTickerPrices(types.CurrencyPair{Base: "ATOM", Quote: "USDT"})
		require.NoError(t, err)
		require.Len(t, prices, 1)
		require.Equal(t, sdk.MustNewDecFromStr(lastPriceAtom), prices["ATOMUSDT"].Price)
		require.Equal(t, sdk.MustNewDecFromStr(volume), prices["ATOMUSDT"].Volume)
	})
	t.Run("valid_request_multi_ticker", func(t *testing.T) {
		prices, err := p.GetTickerPrices(
			types.CurrencyPair{Base: "ATOM", Quote: "USDT"},
			types.CurrencyPair{Base: "LUNA", Quote: "USDT"},
		)
		require.NoError(t, err)
		require.Len(t, prices, 2)
		require.Equal(t, sdk.MustNewDecFromStr(lastPriceAtom), prices["ATOMUSDT"].Price)
		require.Equal(t, sdk.MustNewDecFromStr(volume), prices["ATOMUSDT"].Volume)
		require.Equal(t, sdk.MustNewDecFromStr(lastPriceLuna), prices["LUNAUSDT"].Price)
		require.Equal(t, sdk.MustNewDecFromStr(volume), prices["LUNAUSDT"].Volume)
	})

	t.Run("invalid_request_invalid_ticker", func(t *testing.T) {
		prices, err := p.GetTickerPrices(types.CurrencyPair{Base: "FOO", Quote: "BAR"})
		require.EqualError(t, err, "binance failed to get ticker price for FOOBAR")
		require.Nil(t, prices)
	})
}

func TestBinanceProvider_getSubscriptionMsgs(t *testing.T) {
	provider := &BinanceProvider{
		subscribedPairs: map[string]types.CurrencyPair{},
	}
	cps := []types.CurrencyPair{
		{Base: "ATOM", Quote: "USDT"},
	}

	subMsgs := provider.getSubscriptionMsgs(cps...)

	msg, _ := json.Marshal(subMsgs[0])
	require.Equal(t, "{\"method\":\"SUBSCRIBE\",\"params\":[\"atomusdt@kline_1m\"],\"id\":1}", string(msg))
}
