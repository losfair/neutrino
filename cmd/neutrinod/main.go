package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/lightninglabs/neutrino"
)

func main() {
	var (
		dataDir  = flag.String("datadir", defaultDataDir(), "data directory")
		network  = flag.String("network", "mainnet", "bitcoin network (mainnet, testnet3, regtest, simnet)")
		listen   = flag.String("rpcbind", "127.0.0.1:8332", "RPC listen address")
		addPeers stringSlice
	)
	flag.Var(&addPeers, "addpeer", "add a peer to connect to (can be specified multiple times)")
	flag.Parse()

	chainParams, err := networkParams(*network)
	if err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(*dataDir, 0700); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	dbPath := filepath.Join(*dataDir, "neutrino.db")
	db, err := walletdb.Create("bdb", dbPath, true, 60*time.Second)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	cfg := neutrino.Config{
		DataDir:     *dataDir,
		Database:    db,
		ChainParams: *chainParams,
		AddPeers:    []string(addPeers),
	}

	cs, err := neutrino.NewChainService(cfg)
	if err != nil {
		log.Fatalf("Failed to create chain service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cs.Start(ctx); err != nil {
		log.Fatalf("Failed to start chain service: %v", err)
	}
	defer cs.Stop()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			best, err := cs.BestBlock()
			if err != nil {
				continue
			}
			if cs.IsCurrent() {
				log.Printf("Synced to block %d (%s)",
					best.Height, best.Hash)
				return
			}
			log.Printf("Syncing... block %d (%s)",
				best.Height,
				best.Timestamp.Format("2006-01-02 15:04:05"))
		}
	}()

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			log.Printf("Connected peers: %d",
				cs.ConnectedCount())
		}
	}()

	handler := &rpcHandler{cs: cs}
	srv := &http.Server{
		Addr:    *listen,
		Handler: handler,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		srv.Close()
	}()

	log.Printf("neutrinod listening on %s (network: %s)", *listen, *network)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
}

func defaultDataDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".neutrinod")
}

func networkParams(network string) (*chaincfg.Params, error) {
	switch network {
	case "mainnet":
		return &chaincfg.MainNetParams, nil
	case "testnet3":
		return &chaincfg.TestNet3Params, nil
	case "regtest":
		return &chaincfg.RegressionNetParams, nil
	case "simnet":
		return &chaincfg.SimNetParams, nil
	default:
		return nil, fmt.Errorf("unknown network %q", network)
	}
}

// stringSlice implements flag.Value for repeatable string flags.
type stringSlice []string

func (s *stringSlice) String() string { return fmt.Sprintf("%v", *s) }
func (s *stringSlice) Set(val string) error {
	*s = append(*s, val)
	return nil
}

// JSON-RPC types.

type rpcRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      json.RawMessage   `json:"id"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcHandler struct {
	cs *neutrino.ChainService
}

func (h *rpcHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, rpcResponse{
			JSONRPC: "1.0",
			Error:   &rpcError{Code: -32700, Message: "Parse error"},
		})
		return
	}

	var (
		result any
		rpcErr *rpcError
	)

	switch req.Method {
	case "getblockhash":
		result, rpcErr = h.handleGetBlockHash(req.Params)
	case "getblockheader":
		result, rpcErr = h.handleGetBlockHeader(req.Params)
	default:
		rpcErr = &rpcError{
			Code:    -32601,
			Message: fmt.Sprintf("Method not found: %s", req.Method),
		}
	}

	resp := rpcResponse{
		JSONRPC: "1.0",
		ID:      req.ID,
		Result:  result,
		Error:   rpcErr,
	}
	writeJSON(w, resp)
}

// handleGetBlockHash implements the getblockhash RPC.
// Params: [height]
func (h *rpcHandler) handleGetBlockHash(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, &rpcError{Code: -1, Message: "getblockhash requires 1 parameter"}
	}

	var height int64
	if err := json.Unmarshal(params[0], &height); err != nil {
		return nil, &rpcError{Code: -1, Message: "invalid height parameter"}
	}

	hash, err := h.cs.GetBlockHash(height)
	if err != nil {
		return nil, &rpcError{Code: -5, Message: "Block height out of range"}
	}

	return hash.String(), nil
}

// getBlockHeaderVerboseResult matches Bitcoin Core's verbose getblockheader response.
type getBlockHeaderVerboseResult struct {
	Hash          string  `json:"hash"`
	Confirmations int32   `json:"confirmations"`
	Height        int32   `json:"height"`
	Version       int32   `json:"version"`
	VersionHex    string  `json:"versionHex"`
	MerkleRoot    string  `json:"merkleroot"`
	Time          int64   `json:"time"`
	MedianTime    int64   `json:"mediantime"`
	Nonce         uint32  `json:"nonce"`
	Bits          string  `json:"bits"`
	Difficulty    float64 `json:"difficulty"`
	ChainWork     string  `json:"chainwork"`
	NTx           int32   `json:"nTx"`
	PreviousHash  string  `json:"previousblockhash,omitempty"`
	NextHash      string  `json:"nextblockhash,omitempty"`
}

// handleGetBlockHeader implements the getblockheader RPC.
// Params: [hash, verbose=true]
func (h *rpcHandler) handleGetBlockHeader(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, &rpcError{Code: -1, Message: "getblockheader requires 1 parameter"}
	}

	var hashStr string
	if err := json.Unmarshal(params[0], &hashStr); err != nil {
		return nil, &rpcError{Code: -1, Message: "invalid hash parameter"}
	}

	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		return nil, &rpcError{Code: -1, Message: "invalid block hash"}
	}

	verbose := true
	if len(params) >= 2 {
		if err := json.Unmarshal(params[1], &verbose); err != nil {
			return nil, &rpcError{Code: -1, Message: "invalid verbose parameter"}
		}
	}

	header, height, err := h.cs.BlockHeaders.FetchHeader(hash)
	if err != nil {
		return nil, &rpcError{Code: -5, Message: "Block not found"}
	}

	if !verbose {
		var buf bytes.Buffer
		if err := header.Serialize(&buf); err != nil {
			return nil, &rpcError{Code: -1, Message: "failed to serialize header"}
		}
		return hex.EncodeToString(buf.Bytes()), nil
	}

	best, err := h.cs.BestBlock()
	if err != nil {
		return nil, &rpcError{Code: -1, Message: "failed to get best block"}
	}

	confirmations := int32(best.Height) - int32(height) + 1
	if confirmations < 0 {
		confirmations = 0
	}

	result := getBlockHeaderVerboseResult{
		Hash:          hash.String(),
		Confirmations: confirmations,
		Height:        int32(height),
		Version:       header.Version,
		VersionHex:    fmt.Sprintf("%08x", header.Version),
		MerkleRoot:    header.MerkleRoot.String(),
		Time:          header.Timestamp.Unix(),
		MedianTime:    header.Timestamp.Unix(),
		Nonce:         header.Nonce,
		Bits:          fmt.Sprintf("%08x", header.Bits),
		Difficulty:    difficultyFromBits(header.Bits),
		NTx:           0,
	}

	if header.PrevBlock != (chainhash.Hash{}) {
		result.PreviousHash = header.PrevBlock.String()
	}

	// Attempt to find the next block header.
	nextHeight := height + 1
	if nextHeight <= uint32(best.Height) {
		nextHeader, err := h.cs.BlockHeaders.FetchHeaderByHeight(nextHeight)
		if err == nil {
			nextHash := nextHeader.BlockHash()
			result.NextHash = nextHash.String()
		}
	}

	return result, nil
}

// difficultyFromBits converts the compact "bits" representation to difficulty.
func difficultyFromBits(bits uint32) float64 {
	// Extract mantissa and exponent from compact representation.
	mantissa := bits & 0x007fffff
	exponent := bits >> 24

	var target big.Float
	target.SetInt(big.NewInt(int64(mantissa)))

	// target = mantissa * 2^(8*(exponent-3))
	shift := 8 * (int(exponent) - 3)
	if shift > 0 {
		multiplier := new(big.Float).SetInt(new(big.Int).Lsh(big.NewInt(1), uint(shift)))
		target.Mul(&target, multiplier)
	} else if shift < 0 {
		divisor := new(big.Float).SetInt(new(big.Int).Lsh(big.NewInt(1), uint(-shift)))
		target.Quo(&target, divisor)
	}

	if target.Sign() == 0 {
		return 0
	}

	// difficulty = genesis_target / target
	// genesis_target for mainnet: 0x00ffff * 2^(8*(0x1d - 3))
	var genesisTarget big.Float
	genesisTarget.SetInt(big.NewInt(0x00ffff))
	genShift := 8 * (0x1d - 3)
	genMultiplier := new(big.Float).SetInt(new(big.Int).Lsh(big.NewInt(1), uint(genShift)))
	genesisTarget.Mul(&genesisTarget, genMultiplier)

	var difficulty big.Float
	difficulty.Quo(&genesisTarget, &target)

	result, _ := difficulty.Float64()
	if math.IsInf(result, 0) {
		return 0
	}

	return result
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
