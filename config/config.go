package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/BurntSushi/toml"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/go-playground/validator/v10"
	"price-feeder/oracle/provider"
)

const (
	DenomUSD = "USD"

	defaultListenAddr      = "0.0.0.0:7171"
	defaultSrvWriteTimeout = 15 * time.Second
	defaultSrvReadTimeout  = 15 * time.Second
	defaultProviderTimeout = 100 * time.Millisecond
	defaultHeightPollInterval = 1 * time.Second
)

var (
	validate = validator.New()

	// ErrEmptyConfigPath defines a sentinel error for an empty config path.
	ErrEmptyConfigPath = errors.New("empty configuration file path")

	// SupportedProviders defines a lookup table of all the supported currency API
	// providers.
	SupportedProviders = map[provider.Name]struct{}{
		provider.ProviderKraken: {},
		provider.ProviderBinance: {},
		provider.ProviderBinanceUS: {},
		provider.ProviderOsmosis: {},
		provider.ProviderOsmosisV2: {},
		provider.ProviderOkx: {},
		provider.ProviderHuobi: {},
		provider.ProviderGate: {},
		provider.ProviderCoinbase: {},
		provider.ProviderBitget: {},
		provider.ProviderMexc: {},
		provider.ProviderCrypto: {},
		provider.ProviderFin: {},
		provider.ProviderMock: {},
	}

	// maxDeviationThreshold is the maxmimum allowed amount of standard
	// deviations which validators are able to set for a given asset.
	maxDeviationThreshold = sdk.MustNewDecFromStr("3.0")

	// SupportedQuotes defines a lookup table for which assets we support
	// using as quotes.
	SupportedQuotes = map[string]struct{}{
		DenomUSD:  {},
		"AXLUSDC": {},
		"USDC":    {},
		"USDT":    {},
		"DAI":     {},
		"BTC":     {},
		"ETH":     {},
		"ATOM":    {},
	}
)

type (
	// Config defines all necessary price-feeder configuration parameters.
	Config struct {
		Server            Server             `toml:"server"`
		CurrencyPairs     []CurrencyPair     `toml:"currency_pairs" validate:"dive"`
		Deviations        []Deviation        `toml:"deviation_thresholds"`
		Account           Account            `toml:"account" validate:"required,gt=0,dive,required"`
		Keyring           Keyring            `toml:"keyring" validate:"required,gt=0,dive,required"`
		RPC               RPC                `toml:"rpc" validate:"required,gt=0,dive,required"`
		Telemetry         Telemetry          `toml:"telemetry"`
		GasAdjustment     float64            `toml:"gas_adjustment" validate:"required"`
		GasPrices         string             `toml:"gas_prices" validate:"required"`
		ProviderTimeout   string             `toml:"provider_timeout"`
		ProviderPairs []ProviderPairs `toml:"provider_pairs" validate:"dive,required,gt=0"`
		ProviderEndpoints []ProviderEndpoint `toml:"provider_endpoints" validate:"dive"`
		ProviderMinOverride bool `toml:"provider_min_override"`
		EnableServer      bool               `toml:"enable_server"`
		EnableVoter       bool               `toml:"enable_voter"`
		Healthchecks      []Healthchecks     `toml:"healthchecks" validate:"dive"`
		HeightPollInterval string `toml:"height_poll_interval"`
	}

	ProviderEndpoint struct {
		Name provider.Name `toml:"name"`
		Rest string `toml:"rest"`
		Websocket string `toml:"websocket"`
		PollInterval string `toml:"poll_interval"`
	}

	// Server defines the API server configuration.
	Server struct {
		ListenAddr     string   `toml:"listen_addr"`
		WriteTimeout   string   `toml:"write_timeout"`
		ReadTimeout    string   `toml:"read_timeout"`
		VerboseCORS    bool     `toml:"verbose_cors"`
		AllowedOrigins []string `toml:"allowed_origins"`
	}

	ProviderPairs struct {
		Name provider.Name `toml:"name" validate:"required"`
		Base []string `toml:"base" validate:"required"`
		Quote string `toml:"quote" validate:"required"`
	}

	// CurrencyPair defines a price quote of the exchange rate for two different
	// currencies and the supported providers for getting the exchange rate.
	CurrencyPair struct {
		Base      string   `toml:"base" validate:"required"`
		Quote     string   `toml:"quote" validate:"required"`
		Providers []provider.Name `toml:"providers" validate:"required,gt=0,dive,required"`
	}

	// Deviation defines a maximum amount of standard deviations that a given asset can
	// be from the median without being filtered out before voting.
	Deviation struct {
		Base      string `toml:"base" validate:"required"`
		Threshold string `toml:"threshold" validate:"required"`
	}

	// Account defines account related configuration that is related to the
	// network and transaction signing functionality.
	Account struct {
		ChainID    string `toml:"chain_id" validate:"required"`
		Address    string `toml:"address" validate:"required"`
		Validator  string `toml:"validator" validate:"required"`
		FeeGranter string `toml:"fee_granter"`
		Prefix     string `toml:"prefix" validate:"required"`
	}

	// Keyring defines the required keyring configuration.
	Keyring struct {
		Backend string `toml:"backend" validate:"required"`
		Dir     string `toml:"dir" validate:"required"`
	}

	// RPC defines RPC configuration of both the gRPC and Tendermint nodes.
	RPC struct {
		TMRPCEndpoint string `toml:"tmrpc_endpoint" validate:"required"`
		GRPCEndpoint  string `toml:"grpc_endpoint" validate:"required"`
		RPCTimeout    string `toml:"rpc_timeout" validate:"required"`
	}

	// Telemetry defines the configuration options for application telemetry.
	Telemetry struct {
		// Prefixed with keys to separate services
		ServiceName string `toml:"service_name" mapstructure:"service-name"`

		// Enabled enables the application telemetry functionality. When enabled,
		// an in-memory sink is also enabled by default. Operators may also enabled
		// other sinks such as Prometheus.
		Enabled bool `toml:"enabled" mapstructure:"enabled"`

		// Enable prefixing gauge values with hostname
		EnableHostname bool `toml:"enable_hostname" mapstructure:"enable-hostname"`

		// Enable adding hostname to labels
		EnableHostnameLabel bool `toml:"enable_hostname_label" mapstructure:"enable-hostname-label"`

		// Enable adding service to labels
		EnableServiceLabel bool `toml:"enable_service_label" mapstructure:"enable-service-label"`

		// GlobalLabels defines a global set of name/value label tuples applied to all
		// metrics emitted using the wrapper functions defined in telemetry package.
		//
		// Example:
		// [["chain_id", "cosmoshub-1"]]
		GlobalLabels [][]string `toml:"global_labels" mapstructure:"global-labels"`

		// PrometheusRetentionTime, when positive, enables a Prometheus metrics sink.
		// It defines the retention duration in seconds.
		PrometheusRetentionTime int64 `toml:"prometheus_retention" mapstructure:"prometheus-retention-time"`
	}

	Healthchecks struct {
		URL string `toml:"url" validate:"required"`
		Timeout string `toml:"timeout" validate:"required"`
	}
)

// telemetryValidation is custom validation for the Telemetry struct.
func telemetryValidation(sl validator.StructLevel) {
	tel := sl.Current().Interface().(Telemetry)

	if tel.Enabled && (len(tel.GlobalLabels) == 0 || len(tel.ServiceName) == 0) {
		sl.ReportError(tel.Enabled, "enabled", "Enabled", "enabledNoOptions", "")
	}
}

// endpointValidation is custom validation for the ProviderEndpoint struct.
func endpointValidation(sl validator.StructLevel) {
	endpoint := sl.Current().Interface().(ProviderEndpoint)

	if len(endpoint.Name) < 1 || len(endpoint.Rest) < 1 || len(endpoint.Websocket) < 1 {
		sl.ReportError(endpoint, "endpoint", "Endpoint", "unsupportedEndpointType", "")
	}
	if _, ok := SupportedProviders[endpoint.Name]; !ok {
		sl.ReportError(endpoint.Name, "name", "Name", "unsupportedEndpointProvider", "")
	}
}

// Validate returns an error if the Config object is invalid.
func (c Config) Validate() error {
	validate.RegisterStructValidation(telemetryValidation, Telemetry{})
	validate.RegisterStructValidation(endpointValidation, provider.Endpoint{})
	return validate.Struct(c)
}

// ParseConfig attempts to read and parse configuration from the given file path.
// An error is returned if reading or parsing the config fails.
func ParseConfig(configPath string) (Config, error) {
	var cfg Config

	if configPath == "" {
		return cfg, ErrEmptyConfigPath
	}

	configData, err := ioutil.ReadFile(configPath)
	if err != nil {
		return cfg, fmt.Errorf("failed to read config: %w", err)
	}

	if _, err := toml.Decode(string(configData), &cfg); err != nil {
		return cfg, fmt.Errorf("failed to decode config: %w", err)
	}

	if cfg.Server.ListenAddr == "" {
		cfg.Server.ListenAddr = defaultListenAddr
	}
	if len(cfg.Server.WriteTimeout) == 0 {
		cfg.Server.WriteTimeout = defaultSrvWriteTimeout.String()
	}
	if len(cfg.Server.ReadTimeout) == 0 {
		cfg.Server.ReadTimeout = defaultSrvReadTimeout.String()
	}
	if len(cfg.ProviderTimeout) == 0 {
		cfg.ProviderTimeout = defaultProviderTimeout.String()
	}
	if cfg.HeightPollInterval == "" {
		cfg.HeightPollInterval = defaultHeightPollInterval.String()
	}

	pairs := make(map[string]CurrencyPair)
	bases := make(map[string]map[provider.Name]struct{})
	quotes := make(map[string]struct{})
	for _, providerPair := range cfg.ProviderPairs {
		_, ok := SupportedProviders[providerPair.Name]
		if !ok {
			return cfg, fmt.Errorf("unsupported provider: %s", providerPair.Name)
		}
		_, ok = SupportedQuotes[providerPair.Quote]
		if !ok {
			return cfg, fmt.Errorf("unsupported quote: %s", providerPair.Quote)
		}
		quotes[providerPair.Quote] = struct{}{}
		for _, base := range providerPair.Base {
			pairId := base + "/" + providerPair.Quote
			pair, ok := pairs[pairId]
			if ok {
				pair.Providers = append(pair.Providers, providerPair.Name)
			} else {
				pair = CurrencyPair {
					Base: base,
					Quote: providerPair.Quote,
					Providers: []provider.Name{providerPair.Name},
				}
			}
			pairs[pairId] = pair
			_, ok = bases[base]
			if !ok {
				bases[base] = make(map[provider.Name]struct{})
			}
			bases[base][providerPair.Name] = struct{}{}
		}
	}

	for quote := range quotes {
		if quote != DenomUSD {
			_, ok := bases[quote]
			if !ok {
				return cfg, fmt.Errorf("all non-usd quotes require a conversion rate feed")
			}
		}
	}

	if !cfg.ProviderMinOverride {
		for base, providers := range bases {
			_, ok := providers[provider.ProviderMock]
			if !ok && len(providers) < 3 {
				return cfg, fmt.Errorf("must have at least three providers for %s", base)
			}
		}
	}

	gatePairs := []string{}
	for base, providers := range bases {
		_, ok := providers[provider.ProviderGate]
		if ok {
			gatePairs = append(gatePairs, base)
		}
	}
	if len(gatePairs) > 1 {
		return cfg, fmt.Errorf("gate provider does not support multiple pairs: %v", gatePairs)
	}

	cfg.CurrencyPairs = make([]CurrencyPair, len(pairs))
	i := 0
	for _, pair := range pairs {
		cfg.CurrencyPairs[i] = pair
		i++
	}

	for _, deviation := range cfg.Deviations {
		threshold, err := sdk.NewDecFromStr(deviation.Threshold)
		if err != nil {
			return cfg, fmt.Errorf("deviation thresholds must be numeric: %w", err)
		}

		if threshold.GT(maxDeviationThreshold) {
			return cfg, fmt.Errorf("deviation thresholds must not exceed 3.0")
		}
	}

	for i, provider := range cfg.ProviderEndpoints {
		if provider.PollInterval == "" {
			cfg.ProviderEndpoints[i].PollInterval = "0s"
		}
	}

	return cfg, cfg.Validate()
}
