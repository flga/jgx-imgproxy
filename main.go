package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/cenk/backoff"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/mailgun/groupcache/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var imgmu sync.RWMutex
var imgCache = make(map[string][]byte)
var pathmu sync.RWMutex
var pathCache = make(map[string]string)

func main() {
	listen := flag.String("listen", "localhost:8080", "host:port to listen on")
	https := flag.Bool("https", false, "enable https")
	cert := flag.String("cert", "", "cert file")
	key := flag.String("key", "", "key file")

	avatarConcurrency := flag.Int("avatar.concurrency", 10, "max number of in flight requests to jagex")
	avatarTTL := flag.Duration("avatar.ttl", 30*time.Minute, "avatar cache ttl")
	avatarSize := flag.Int64("avatar.size", 64, "max cache size in megabytes")

	clanConcurrency := flag.Int("clan.concurrency", 5, "max number of in flight requests to jagex")
	clanTTL := flag.Duration("clan.ttl", 30*time.Minute, "clan cache ttl")
	clanSize := flag.Int64("clan.size", 64, "max cache size in megabytes")

	gzip := flag.Bool("gzip", false, "enable gzip compression when serving")
	reqLog := flag.Bool("requestLogs", false, "enable request logging in combined log format")
	logLevel := flag.String("logLevel", "debug", "internal logger level, use none to disable internal logging")
	flag.Parse()

	if *https && (*cert == "" || *key == "") {
		fmt.Fprintln(os.Stderr, "when using https, -cert and -key are mandatory")
		os.Exit(1)
	}

	if err := run(
		*listen,
		*https,
		*cert,
		*key,
		*avatarConcurrency,
		*avatarTTL,
		*avatarSize,
		*clanConcurrency,
		*clanTTL,
		*clanSize,
		*gzip,
		*reqLog,
		*logLevel,
	); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run(
	listen string,
	https bool,
	cert, key string,
	avatarConcurrency int,
	avatarTTL time.Duration,
	avatarSize int64,
	clanConcurrency int,
	clanTTL time.Duration,
	clanSize int64,
	gzip bool,
	reqLog bool,
	logLevel string,
) error {
	var logger *zap.Logger
	if logLevel != "none" {
		var err error
		logger, err = newLogger(logLevel)
		if err != nil {
			return fmt.Errorf("unable to create logger: %w", err)
		}
		defer logger.Sync()
	} else {
		logger = zap.NewNop()
	}

	avatars := groupcache.NewGroup(
		"avatars",
		avatarSize<<20,
		fetch(
			make(chan struct{}, avatarConcurrency),
			func(key string) string {
				return fmt.Sprintf("https://secure.runescape.com/m=avatar-rs/%s/chat.png", key)
			},
			avatarTTL,
			logger.Named("avatar-cache"),
		),
	)

	clans := groupcache.NewGroup(
		"clans",
		clanSize<<20,
		fetch(
			make(chan struct{}, clanConcurrency),
			func(key string) string {
				return fmt.Sprintf("https://secure.runescape.com/m=avatar-rs/%s/clanmotif.png", key)
			},
			clanTTL,
			logger.Named("clans-cache"),
		),
	)

	router := mux.NewRouter()
	router.HandleFunc("/m=avatar-rs/{player}/chat.png", handle("player", avatars, logger.Named("player-handler")))
	router.HandleFunc("/m=avatar-rs/{clan}/clanmotif.png", handle("clan", clans, logger.Named("clan-handler")))

	var handler http.Handler = router
	if reqLog {
		handler = handlers.CombinedLoggingHandler(os.Stdout, router)
	}
	if gzip {
		handler = handlers.CompressHandler(handler)
	}

	logger.Info("starting server", zap.String("address", listen), zap.Bool("https", https))
	if https {
		return http.ListenAndServeTLS(listen, cert, key, handler)
	}

	return http.ListenAndServe(listen, handler)
}

func handle(keyName string, cache *groupcache.Group, log *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)[keyName]
		if key == "" {
			log.Error("invalid param", zap.String("key", keyName), zap.String("val", key))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			var data []byte
			err := cache.Get(r.Context(), key, groupcache.AllocatingByteSliceSink(&data))
			if err != nil {
				log.Error("could not get value", zap.Error(err))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			w.Write(data)
			return

		case http.MethodDelete:
			err := cache.Remove(r.Context(), key)
			if err != nil {
				log.Error("could not delete value", zap.Error(err))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			return

		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}
}

type urlFunc func(key string) string

func fetch(sem chan struct{}, urlFunc urlFunc, ttl time.Duration, log *zap.Logger) groupcache.GetterFunc {
	return func(ctx context.Context, key string, dest groupcache.Sink) error {
		url := urlFunc(key)
		log := log.With(zap.String("key", key), zap.String("url", url))

		op := func() error {
			sem <- struct{}{}
			defer func() { <-sem }()

			log.Info("fetching remote")
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				log.Error("invalid request", zap.Error(err))
				return err
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Error("could not send request", zap.Error(err))
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Error("unexpected status", zap.Int("status", resp.StatusCode))
				return fmt.Errorf("unexpected status: %d", resp.StatusCode)
			}

			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Error("invalid read", zap.Error(err))
				return err
			}

			return dest.SetBytes(data, time.Now().Add(ttl))
		}

		bconf := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
		return backoff.Retry(op, bconf)
	}
}

func newLogger(level string) (*zap.Logger, error) {
	l := zapcore.Level(0)
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = zapcore.ErrorLevel
	}

	logLevel := zap.NewAtomicLevelAt(l)
	cfg := zap.NewProductionConfig()
	cfg.DisableCaller = true
	cfg.DisableStacktrace = true
	cfg.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	cfg.Level = logLevel

	logger, err := cfg.Build()
	if err != nil {
		return nil, err
	}

	return logger, nil
}
