// Command cnpg-dbclaim runs the DatabaseClaim controller.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Jwrede/cnpg-dbclaim/internal/controller"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var version = "dev"

func main() {
	kubeconfig := flag.String("kubeconfig", "", "path to a kubeconfig (default: in-cluster config, then $KUBECONFIG or ~/.kube/config)")
	resync := flag.Duration("resync", 30*time.Second, "interval of full reconcile passes")
	addr := flag.String("metrics-addr", ":8080", "address for /metrics and /healthz (empty disables)")
	namespaces := flag.String("cluster-namespaces", "", "comma-separated namespaces that hold the shared Clusters and DatabaseGrants (default: all)")
	once := flag.Bool("once", false, "run one reconcile pass and exit")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	logger := log.New(os.Stderr, "", log.LstdFlags)

	cfg, err := config(*kubeconfig)
	if err != nil {
		logger.Fatalf("kubeconfig: %v", err)
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		logger.Fatalf("client: %v", err)
	}
	c := &controller.Controller{Client: client, Log: logger}
	for _, n := range strings.Split(*namespaces, ",") {
		if n = strings.TrimSpace(n); n != "" {
			c.ClusterNamespaces = append(c.ClusterNamespaces, n)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *once {
		if err := c.Reconcile(ctx); err != nil {
			logger.Fatal(err)
		}
		return
	}
	if *addr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			c.Metrics(w)
		})
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
		srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("metrics: %v", err)
			}
		}()
		defer srv.Close()
	}
	logger.Printf("cnpg-dbclaim %s starting (resync %s)", version, *resync)
	c.Run(ctx, *resync)
}

func config(path string) (*rest.Config, error) {
	if path == "" {
		if cfg, err := rest.InClusterConfig(); err == nil {
			return cfg, nil
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if path != "" {
		rules.ExplicitPath = path
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
}
