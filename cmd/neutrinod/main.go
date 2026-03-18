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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
	"github.com/lightninglabs/neutrino"
)

func main() {
	var (
		dataDir  = flag.String("datadir", defaultDataDir(), "data directory")
		network  = flag.String("network", "mainnet", "bitcoin network (mainnet, testnet3, testnet4, signet, regtest, simnet)")
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
		var lastHeight int32
		for range ticker.C {
			best, err := cs.BestBlock()
			if err != nil {
				continue
			}
			if best.Height == lastHeight {
				continue
			}
			lastHeight = best.Height
			if cs.IsCurrent() {
				log.Printf("Synced to block %d (%s)",
					best.Height, best.Hash)
			} else {
				log.Printf("Syncing... block %d (%s)",
					best.Height,
					best.Timestamp.Format("2006-01-02 15:04:05"))
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		known := make(map[int32]string) // peer ID -> addr
		var tickCount int
		for range ticker.C {
			peers := cs.Peers()
			current := make(map[int32]string, len(peers))
			for _, p := range peers {
				snap := p.StatsSnapshot()
				current[snap.ID] = snap.Addr
			}
			for id, addr := range current {
				if _, ok := known[id]; !ok {
					log.Printf("Peer connected: %s", addr)
				}
			}
			for id, addr := range known {
				if _, ok := current[id]; !ok {
					log.Printf("Peer disconnected: %s", addr)
				}
			}
			known = current
			tickCount++
			if tickCount%6 == 0 {
				log.Printf("Connected peers: %d", len(current))
			}
		}
	}()

	cache := newRecentBlocksCache()
	go runBlockFetcher(ctx, cs, cache)

	handler := &rpcHandler{cs: cs, cache: cache}
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
	case "testnet4":
		return &chaincfg.TestNet4Params, nil
	case "signet":
		return &chaincfg.SigNetParams, nil
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

const recentBlockCount = 20

// recentBlocksCache keeps the full block data for the most recent blocks
// in memory so they can be served via the getblock RPC.
type recentBlocksCache struct {
	mu     sync.RWMutex
	blocks map[chainhash.Hash]*btcutil.Block // hash -> block
	byHeight map[int32]*btcutil.Block        // height -> block
	tipHeight int32
}

func newRecentBlocksCache() *recentBlocksCache {
	return &recentBlocksCache{
		blocks:   make(map[chainhash.Hash]*btcutil.Block),
		byHeight: make(map[int32]*btcutil.Block),
	}
}

// add inserts a block and evicts any block older than recentBlockCount from tip.
func (c *recentBlocksCache) add(block *btcutil.Block) {
	c.mu.Lock()
	defer c.mu.Unlock()

	height := block.Height()
	hash := block.Hash()

	c.blocks[*hash] = block
	c.byHeight[height] = block

	if height > c.tipHeight {
		c.tipHeight = height
	}

	// Evict blocks outside the window.
	cutoff := c.tipHeight - recentBlockCount
	for h, b := range c.byHeight {
		if h <= cutoff {
			delete(c.blocks, *b.Hash())
			delete(c.byHeight, h)
		}
	}
}

// get returns the block for the given hash, or nil if not cached.
func (c *recentBlocksCache) get(hash *chainhash.Hash) *btcutil.Block {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blocks[*hash]
}

// getByHeight returns the block at the given height, or nil if not cached.
func (c *recentBlocksCache) getByHeight(height int32) *btcutil.Block {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.byHeight[height]
}

// handleReorg removes all blocks above the given height (used on reorg).
func (c *recentBlocksCache) handleReorg(newTipHeight int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for h, b := range c.byHeight {
		if h > newTipHeight {
			delete(c.blocks, *b.Hash())
			delete(c.byHeight, h)
		}
	}
	c.tipHeight = newTipHeight
}

// runBlockFetcher polls for new tip blocks and fetches full blocks for the
// latest recentBlockCount blocks.
func runBlockFetcher(ctx context.Context, cs *neutrino.ChainService, cache *recentBlocksCache) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastTip int32

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if !cs.IsCurrent() {
			continue
		}

		best, err := cs.BestBlock()
		if err != nil || best.Height == lastTip {
			continue
		}

		if best.Height < lastTip {
			// Reorg detected.
			cache.handleReorg(best.Height)
		}

		// Fetch blocks from lastTip+1 to best.Height (bounded to
		// recentBlockCount from the tip).
		startHeight := lastTip + 1
		minHeight := best.Height - recentBlockCount + 1
		if minHeight < 0 {
			minHeight = 0
		}
		if startHeight < minHeight {
			startHeight = minHeight
		}

		for h := startHeight; h <= best.Height; h++ {
			if cache.getByHeight(h) != nil {
				continue
			}

			hash, err := cs.GetBlockHash(int64(h))
			if err != nil {
				log.Printf("Failed to get block hash at height %d: %v", h, err)
				break
			}

			block, err := cs.GetBlock(*hash)
			if err != nil {
				log.Printf("Failed to fetch block %d (%s): %v", h, hash, err)
				break
			}
			block.SetHeight(h)
			cache.add(block)
			log.Printf("Fetched block %d (%s)", h, hash)
		}

		lastTip = best.Height
	}
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
	cs    *neutrino.ChainService
	cache *recentBlocksCache
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
	case "getbestblockhash":
		result, rpcErr = h.handleGetBestBlockHash()
	case "getblockhash":
		result, rpcErr = h.handleGetBlockHash(req.Params)
	case "getblock":
		result, rpcErr = h.handleGetBlock(req.Params)
	case "getblockheader":
		result, rpcErr = h.handleGetBlockHeader(req.Params)
	case "getpeerinfo":
		result, rpcErr = h.handleGetPeerInfo()
	case "addnode":
		result, rpcErr = h.handleAddNode(req.Params)
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

// handleGetBestBlockHash implements the getbestblockhash RPC.
func (h *rpcHandler) handleGetBestBlockHash() (any, *rpcError) {
	best, err := h.cs.BestBlock()
	if err != nil {
		return nil, &rpcError{Code: -1, Message: "failed to get best block"}
	}
	return best.Hash.String(), nil
}

// getBlockVerboseResult matches Bitcoin Core's verbose getblock response (verbosity=1).
type getBlockVerboseResult struct {
	Hash          string   `json:"hash"`
	Confirmations int32    `json:"confirmations"`
	Size          int      `json:"size"`
	StrippedSize  int      `json:"strippedsize"`
	Weight        int      `json:"weight"`
	Height        int32    `json:"height"`
	Version       int32    `json:"version"`
	VersionHex    string   `json:"versionHex"`
	MerkleRoot    string   `json:"merkleroot"`
	Tx            []string `json:"tx"`
	Time          int64    `json:"time"`
	MedianTime    int64    `json:"mediantime"`
	Nonce         uint32   `json:"nonce"`
	Bits          string   `json:"bits"`
	Difficulty    float64  `json:"difficulty"`
	NTx           int      `json:"nTx"`
	PreviousHash  string   `json:"previousblockhash,omitempty"`
	NextHash      string   `json:"nextblockhash,omitempty"`
}

// handleGetBlock implements the getblock RPC.
// Params: [hash, verbosity=1]
// verbosity 0: hex-encoded serialized block
// verbosity 1: JSON object with tx hashes
// Only blocks in the recent cache (latest 20) are available.
func (h *rpcHandler) handleGetBlock(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, &rpcError{Code: -1, Message: "getblock requires 1 parameter"}
	}

	var hashStr string
	if err := json.Unmarshal(params[0], &hashStr); err != nil {
		return nil, &rpcError{Code: -1, Message: "invalid hash parameter"}
	}

	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		return nil, &rpcError{Code: -1, Message: "invalid block hash"}
	}

	verbosity := 1
	if len(params) >= 2 {
		if err := json.Unmarshal(params[1], &verbosity); err != nil {
			return nil, &rpcError{Code: -1, Message: "invalid verbosity parameter"}
		}
	}

	block := h.cache.get(hash)
	if block == nil {
		return nil, &rpcError{Code: -1, Message: "Block not found (only recent blocks are available)"}
	}

	msgBlock := block.MsgBlock()

	if verbosity == 0 {
		var buf bytes.Buffer
		if err := msgBlock.Serialize(&buf); err != nil {
			return nil, &rpcError{Code: -1, Message: "failed to serialize block"}
		}
		return hex.EncodeToString(buf.Bytes()), nil
	}

	best, err := h.cs.BestBlock()
	if err != nil {
		return nil, &rpcError{Code: -1, Message: "failed to get best block"}
	}

	height := block.Height()
	confirmations := best.Height - height + 1
	if confirmations < 0 {
		confirmations = 0
	}

	txHashes := make([]string, len(msgBlock.Transactions))
	for i, tx := range msgBlock.Transactions {
		txHashes[i] = tx.TxHash().String()
	}

	header := &msgBlock.Header

	result := getBlockVerboseResult{
		Hash:          hash.String(),
		Confirmations: confirmations,
		Size:          msgBlock.SerializeSize(),
		StrippedSize:  msgBlock.SerializeSizeStripped(),
		Weight:        msgBlock.SerializeSizeStripped()*3 + msgBlock.SerializeSize(),
		Height:        height,
		Version:       header.Version,
		VersionHex:    fmt.Sprintf("%08x", header.Version),
		MerkleRoot:    header.MerkleRoot.String(),
		Tx:            txHashes,
		Time:          header.Timestamp.Unix(),
		MedianTime:    header.Timestamp.Unix(),
		Nonce:         header.Nonce,
		Bits:          fmt.Sprintf("%08x", header.Bits),
		Difficulty:    difficultyFromBits(header.Bits),
		NTx:           len(msgBlock.Transactions),
	}

	if header.PrevBlock != (chainhash.Hash{}) {
		result.PreviousHash = header.PrevBlock.String()
	}

	// Attempt to find the next block header.
	nextHeight := uint32(height) + 1
	if nextHeight <= uint32(best.Height) {
		nextHeader, err := h.cs.BlockHeaders.FetchHeaderByHeight(nextHeight)
		if err == nil {
			nextHash := nextHeader.BlockHash()
			result.NextHash = nextHash.String()
		}
	}

	return result, nil
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

	var nTx int32
	if cached := h.cache.get(hash); cached != nil {
		nTx = int32(len(cached.MsgBlock().Transactions))
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
		NTx:           nTx,
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

// peerInfoResult matches Bitcoin Core's getpeerinfo response for a single peer.
type peerInfoResult struct {
	ID             int32   `json:"id"`
	Addr           string  `json:"addr"`
	Services       string  `json:"services"`
	ServicesNames  string  `json:"servicesnames"`
	LastSend       int64   `json:"lastsend"`
	LastRecv       int64   `json:"lastrecv"`
	BytesSent      uint64  `json:"bytessent"`
	BytesRecv      uint64  `json:"bytesrecv"`
	ConnTime       int64   `json:"conntime"`
	TimeOffset     int64   `json:"timeoffset"`
	PingTime       float64 `json:"pingtime"`
	Version        uint32  `json:"version"`
	SubVer         string  `json:"subver"`
	Inbound        bool    `json:"inbound"`
	StartingHeight int32   `json:"startingheight"`
	SyncdHeaders   int32   `json:"synced_headers"`
}

// handleGetPeerInfo implements the getpeerinfo RPC.
func (h *rpcHandler) handleGetPeerInfo() (any, *rpcError) {
	peers := h.cs.Peers()
	result := make([]peerInfoResult, 0, len(peers))

	for _, p := range peers {
		stats := p.StatsSnapshot()

		services := fmt.Sprintf("%016x", uint64(stats.Services))
		servicesNames := servicesFlagString(stats.Services)

		var pingTime float64
		if stats.LastPingMicros > 0 {
			pingTime = float64(stats.LastPingMicros) / 1e6
		}

		result = append(result, peerInfoResult{
			ID:             stats.ID,
			Addr:           stats.Addr,
			Services:       services,
			ServicesNames:  servicesNames,
			LastSend:       stats.LastSend.Unix(),
			LastRecv:       stats.LastRecv.Unix(),
			BytesSent:      stats.BytesSent,
			BytesRecv:      stats.BytesRecv,
			ConnTime:       stats.ConnTime.Unix(),
			TimeOffset:     stats.TimeOffset,
			PingTime:       pingTime,
			Version:        stats.Version,
			SubVer:         stats.UserAgent,
			Inbound:        stats.Inbound,
			StartingHeight: stats.StartingHeight,
			SyncdHeaders:   stats.LastBlock,
		})
	}

	return result, nil
}

// handleAddNode implements the addnode RPC.
// Params: [node, command]
// command is one of "add", "remove", or "onetry".
func (h *rpcHandler) handleAddNode(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 2 {
		return nil, &rpcError{
			Code:    -1,
			Message: "addnode requires 2 parameters: node and command (add, remove, onetry)",
		}
	}

	var node, command string
	if err := json.Unmarshal(params[0], &node); err != nil {
		return nil, &rpcError{Code: -1, Message: "invalid node parameter"}
	}
	if err := json.Unmarshal(params[1], &command); err != nil {
		return nil, &rpcError{Code: -1, Message: "invalid command parameter"}
	}

	switch command {
	case "add":
		err := h.cs.ConnectNode(node, true)
		if err != nil {
			return nil, &rpcError{
				Code:    -23,
				Message: fmt.Sprintf("Node already added: %s", node),
			}
		}
	case "remove":
		err := h.cs.RemoveNodeByAddr(node)
		if err != nil {
			return nil, &rpcError{
				Code:    -24,
				Message: fmt.Sprintf("Node not found: %s", node),
			}
		}
	case "onetry":
		err := h.cs.ConnectNode(node, false)
		if err != nil {
			return nil, &rpcError{
				Code:    -23,
				Message: fmt.Sprintf("Failed to connect: %s", node),
			}
		}
	default:
		return nil, &rpcError{
			Code:    -1,
			Message: fmt.Sprintf("Invalid command: %s (use add, remove, or onetry)", command),
		}
	}

	return nil, nil
}

// servicesFlagString returns a human-readable string for service flags,
// matching Bitcoin Core's servicesnames format.
func servicesFlagString(services wire.ServiceFlag) string {
	var names []string
	if services&wire.SFNodeNetwork != 0 {
		names = append(names, "NETWORK")
	}
	if services&wire.SFNodeWitness != 0 {
		names = append(names, "WITNESS")
	}
	if services&wire.SFNodeCF != 0 {
		names = append(names, "COMPACT_FILTERS")
	}
	if services&wire.SFNodeBloom != 0 {
		names = append(names, "BLOOM")
	}
	if services&wire.SFNodeNetworkLimited != 0 {
		names = append(names, "NETWORK_LIMITED")
	}
	return strings.Join(names, " & ")
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
