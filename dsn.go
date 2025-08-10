package redshiftdatasqldriver

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

type RedshiftDataConfig struct {
	ClusterIdentifier *string
	Database          *string
	DbUser            *string
	WorkgroupName     *string
	SecretsARN        *string

	Timeout time.Duration
	Polling time.Duration
	// BlockingRequests indicates whether the requests should be waiting for each other or not.
	BlockingRequests bool
	// EmulateTransactions indicates whether the driver should emulate transactions by gathering the statements and calling BatchExecuteStatement.
	EmulateTransactions bool

	Params             url.Values
	RedshiftDataOptFns []func(*redshiftdata.Options)
}

func (cfg *RedshiftDataConfig) String() string {
	base := strings.TrimPrefix(cfg.baseString(), "//")
	if base == "" {
		return ""
	}
	params := url.Values{}
	for key, value := range cfg.Params {
		params[key] = append([]string{}, value...)
	}
	if cfg.Timeout != 0 {
		params.Add("timeout", cfg.Timeout.String())
	} else {
		params.Del("timeout")
	}
	if cfg.Polling != 0 {
		params.Add("polling", cfg.Polling.String())
	} else {
		params.Del("polling")
	}
	if cfg.BlockingRequests {
		params.Add("requestMode", "blocking")
	}
	if !cfg.EmulateTransactions {
		params.Add("transactionMode", "non-transactional")
	}
	encodedParams := params.Encode()
	if encodedParams != "" {
		return base + "?" + encodedParams
	}
	return base
}

func (cfg *RedshiftDataConfig) setParams(params url.Values) error {
	var err error
	cfg.Params = params
	if params.Has("timeout") {
		cfg.Timeout, err = time.ParseDuration(params.Get("timeout"))
		if err != nil {
			return fmt.Errorf("parse timeout as duration: %w", err)
		}
		cfg.Params.Del("timeout")
	}
	if params.Has("polling") {
		cfg.Polling, err = time.ParseDuration(params.Get("polling"))
		if err != nil {
			return fmt.Errorf("parse polling as duration: %w", err)
		}
		cfg.Params.Del("polling")
	}
	if params.Has("region") {
		cfg = cfg.WithRegion(params.Get("region"))
	}
	if params.Has("requestMode") {
		requestMode := params.Get("requestMode")
		if requestMode == "blocking" {
			cfg.BlockingRequests = true
		} else if requestMode == "non-blocking" {
			cfg.BlockingRequests = false
		} else {
			return fmt.Errorf("invalid param requestMode: %s, must be 'blocking' or 'non-blocking' (default: 'non-blocking')", requestMode)
		}
		cfg.Params.Del("requestMode")
	}
	cfg.EmulateTransactions = true
	if params.Has("transactionMode") {
		transactionMode := params.Get("transactionMode")
		if transactionMode == "non-transactional" {
			cfg.EmulateTransactions = false
		} else {
			return fmt.Errorf("invalid param transactionMode: %s, must be 'transactional' or 'non-transactional' (default: 'non-transactional')", transactionMode)
		}
		cfg.Params.Del("transactionMode")
	}
	if len(cfg.Params) == 0 {
		cfg.Params = nil
	}
	return nil
}

func (cfg *RedshiftDataConfig) baseString() string {
	if cfg.SecretsARN != nil {
		return *cfg.SecretsARN
	}
	var u url.URL
	if cfg.ClusterIdentifier != nil && cfg.DbUser != nil {
		u.Host = fmt.Sprintf("cluster(%s)", *cfg.ClusterIdentifier)
		u.User = url.User(*cfg.DbUser)
	}
	if cfg.WorkgroupName != nil {
		u.Host = fmt.Sprintf("workgroup(%s)", *cfg.WorkgroupName)
	}
	if u.Host == "" || cfg.Database == nil {
		return ""
	}
	u.Path = *cfg.Database
	return u.String()
}

func ParseDSN(dsn string) (*RedshiftDataConfig, error) {
	if dsn == "" {
		return nil, ErrDSNEmpty
	}
	if strings.HasPrefix(dsn, "arn:") {
		parts := strings.Split(dsn, "?")
		cfg := &RedshiftDataConfig{
			SecretsARN: aws.String(parts[0]),
		}
		if len(parts) >= 2 {
			params, err := url.ParseQuery(strings.Join(parts[1:], "?"))
			if err != nil {
				return nil, fmt.Errorf("dsn is invalid: can not parse query params: %w", err)
			}
			if err := cfg.setParams(params); err != nil {
				return nil, fmt.Errorf("dsn is invalid: set query params: %w", err)
			}
		}
		return cfg, nil
	}
	u, err := url.Parse("redshift-data://" + dsn)
	if err != nil {
		return nil, fmt.Errorf("dsn is invalid: %w", err)
	}
	cfg := &RedshiftDataConfig{
		Database: nullif(strings.TrimPrefix(u.Path, "/")),
	}
	if cfg.Database == nil {
		return nil, errors.New("dsn is invalid: missing database")
	}
	if err := cfg.setParams(u.Query()); err != nil {
		return nil, fmt.Errorf("dsn is invalid: set query params: %w", err)
	}
	if strings.HasPrefix(u.Host, "cluster(") {
		cfg.DbUser = nullif(u.User.Username())
		cfg.ClusterIdentifier = nullif(strings.TrimSuffix(strings.TrimPrefix(u.Host, "cluster("), ")"))
		return cfg, nil
	}
	if strings.HasPrefix(u.Host, "workgroup(") {
		cfg.WorkgroupName = nullif(strings.TrimSuffix(strings.TrimPrefix(u.Host, "workgroup("), ")"))
		return cfg, nil
	}
	return nil, errors.New("dsn is invalid: workgroup(name)/database or username@cluster(name)/database or secrets_arn")
}

func (cfg *RedshiftDataConfig) WithRegion(region string) *RedshiftDataConfig {
	if cfg.Params == nil {
		cfg.Params = url.Values{}
	}
	cfg.Params.Set("region", region)
	cfg.RedshiftDataOptFns = append(cfg.RedshiftDataOptFns, func(o *redshiftdata.Options) {
		o.Region = region
	})
	return cfg
}
