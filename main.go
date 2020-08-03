package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/cenk/backoff"
	"github.com/flga/jgx-imgproxy/prom"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/mailgun/groupcache/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

type CacheStats struct {
	Bytes     int64 `json:"bytes"`
	Items     int64 `json:"items"`
	Gets      int64 `json:"gets"`
	Hits      int64 `json:"hits"`
	Evictions int64 `json:"evictions"`
}

type CacheGroup struct {
	Main CacheStats `json:"main"`
	Hot  CacheStats `json:"hot"`
}

func main() {
	listen := flag.String("listen", "localhost:8080", "host:port to listen on")
	adminListen := flag.String("admin.listen", "localhost:6251", "host:port to listen on for admin stuff")
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
		*adminListen,
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
	adminListen string,
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

	group, ctx := errgroup.WithContext(context.Background())

	// api server
	group.Go(func() error {
		router := mux.NewRouter()
		router.Use(prom.Handler)
		router.HandleFunc("/m=avatar-rs/{player}/chat.png", handle("player", avatars, logger.Named("player-handler")))
		router.HandleFunc("/m=avatar-rs/{clan}/clanmotif.png", handle("clan", clans, logger.Named("clan-handler")))

		var handler http.Handler = router
		if reqLog {
			handler = handlers.CombinedLoggingHandler(os.Stdout, router)
		}
		if gzip {
			handler = handlers.CompressHandler(handler)
		}

		server := &http.Server{
			Addr:    listen,
			Handler: handler,
		}

		logger.Info("starting server", zap.String("address", listen), zap.Bool("https", https))
		if https {
			return startServerTLS(ctx, server, listen, cert, key, 30*time.Second, logger.Named("api"))
		}
		return startServer(ctx, server, listen, 30*time.Second, logger.Named("api"))
	})

	// admin server
	group.Go(func() error {
		router := mux.NewRouter()
		router.Handle("/metrics", promhttp.Handler())
		router.HandleFunc("/.stats", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var groups = map[string]CacheGroup{
				"avatars": stats(avatars),
				"clans":   stats(clans),
			}

			d, err := json.Marshal(groups)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			w.Write(d)
		}))

		var handler http.Handler = router
		if reqLog {
			handler = handlers.CombinedLoggingHandler(os.Stdout, router)
		}
		if gzip {
			handler = handlers.CompressHandler(handler)
		}

		server := &http.Server{
			Addr:    adminListen,
			Handler: router,
		}

		logger.Info("starting admin server", zap.String("address", adminListen))
		return startServer(ctx, server, adminListen, 30*time.Second, logger.Named("admin"))
	})

	// signal handler
	group.Go(func() error {
		logger.Info("listening")
		sigc := make(chan os.Signal)
		signal.Notify(sigc, os.Interrupt, os.Kill)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case s := <-sigc:
			return fmt.Errorf("received signal %s", s)
		}
	})

	return group.Wait()
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

func stats(g *groupcache.Group) CacheGroup {
	var group CacheGroup

	main := g.CacheStats(groupcache.MainCache)
	group.Main = CacheStats{
		Bytes:     main.Bytes,
		Items:     main.Items,
		Gets:      main.Gets,
		Hits:      main.Hits,
		Evictions: main.Evictions,
	}

	hot := g.CacheStats(groupcache.HotCache)
	group.Hot = CacheStats{
		Bytes:     hot.Bytes,
		Items:     hot.Items,
		Gets:      hot.Gets,
		Hits:      hot.Hits,
		Evictions: hot.Evictions,
	}

	return group
}

func startServer(ctx context.Context, srv *http.Server, addr string, gracefulShutdownTimeout time.Duration, logger *zap.Logger) error {
	errc := make(chan error)
	shutdown := make(chan struct{})
	go func() {
		<-ctx.Done()

		timeout, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
		defer cancel()

		logger.Info("starting shutdown process", zap.Duration("timeout", gracefulShutdownTimeout))

		err := srv.Shutdown(timeout)
		if err != nil {
			logger.Error("unclean shutdown", zap.Error(err))
		} else {
			logger.Info("shutdown complete")
		}
		close(shutdown)
		errc <- err
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	<-shutdown
	return <-errc
}

func startServerTLS(ctx context.Context, srv *http.Server, addr, cert, key string, gracefulShutdownTimeout time.Duration, logger *zap.Logger) error {
	errc := make(chan error)
	shutdown := make(chan struct{})
	go func() {
		<-ctx.Done()

		timeout, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
		defer cancel()

		logger.Info("starting shutdown process", zap.Duration("timeout", gracefulShutdownTimeout))

		err := srv.Shutdown(timeout)
		if err != nil {
			logger.Error("unclean shutdown", zap.Error(err))
		} else {
			logger.Info("shutdown complete")
		}
		close(shutdown)
		errc <- err
	}()

	if err := srv.ListenAndServeTLS(cert, key); err != nil && err != http.ErrServerClosed {
		return err
	}

	<-shutdown
	return <-errc
}
