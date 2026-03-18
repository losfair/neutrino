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
		dataDir    = flag.String("datadir", defaultDataDir(), "data directory")
		network    = flag.String("network", "mainnet", "bitcoin network (mainnet, testnet3, testnet4, signet, regtest, simnet)")
		listen     = flag.String("rpcbind", "127.0.0.1:8332", "RPC listen address")
		cacheBlocks = flag.Int("cacheblocks", 20, "number of recent blocks to keep in memory")
		addPeers   stringSlice
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

	cache := newRecentBlocksCache(int32(*cacheBlocks))
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

// txEntry records the location of a transaction within a cached block.
type txEntry struct {
	blockHash chainhash.Hash
	txIndex   int
}

// recentBlocksCache keeps the full block data for the most recent blocks
// in memory so they can be served via the getblock and getrawtransaction RPCs.
type recentBlocksCache struct {
	mu        sync.RWMutex
	blocks    map[chainhash.Hash]*btcutil.Block // block hash -> block
	byHeight  map[int32]*btcutil.Block          // height -> block
	txIndex   map[chainhash.Hash]txEntry         // txid -> block location
	tipHeight int32
	maxBlocks int32
}

func newRecentBlocksCache(maxBlocks int32) *recentBlocksCache {
	return &recentBlocksCache{
		blocks:    make(map[chainhash.Hash]*btcutil.Block),
		byHeight:  make(map[int32]*btcutil.Block),
		txIndex:   make(map[chainhash.Hash]txEntry),
		maxBlocks: maxBlocks,
	}
}

// add inserts a block and evicts any block older than maxBlocks from tip.
func (c *recentBlocksCache) add(block *btcutil.Block) {
	c.mu.Lock()
	defer c.mu.Unlock()

	height := block.Height()
	hash := block.Hash()

	c.blocks[*hash] = block
	c.byHeight[height] = block

	// Index all transactions in this block.
	for i, tx := range block.MsgBlock().Transactions {
		c.txIndex[tx.TxHash()] = txEntry{blockHash: *hash, txIndex: i}
	}

	if height > c.tipHeight {
		c.tipHeight = height
	}

	// Evict blocks outside the window.
	cutoff := c.tipHeight - c.maxBlocks
	for h, b := range c.byHeight {
		if h <= cutoff {
			// Remove tx index entries for the evicted block.
			for _, tx := range b.MsgBlock().Transactions {
				delete(c.txIndex, tx.TxHash())
			}
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

// getTx returns the transaction and its containing block for a given txid.
func (c *recentBlocksCache) getTx(txid *chainhash.Hash) (*wire.MsgTx, *btcutil.Block, int) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.txIndex[*txid]
	if !ok {
		return nil, nil, -1
	}
	block, ok := c.blocks[entry.blockHash]
	if !ok {
		return nil, nil, -1
	}
	txs := block.MsgBlock().Transactions
	if entry.txIndex >= len(txs) {
		return nil, nil, -1
	}
	return txs[entry.txIndex], block, entry.txIndex
}

// handleReorg removes all blocks above the given height (used on reorg).
func (c *recentBlocksCache) handleReorg(newTipHeight int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for h, b := range c.byHeight {
		if h > newTipHeight {
			for _, tx := range b.MsgBlock().Transactions {
				delete(c.txIndex, tx.TxHash())
			}
			delete(c.blocks, *b.Hash())
			delete(c.byHeight, h)
		}
	}
	c.tipHeight = newTipHeight
}

// runBlockFetcher polls for new tip blocks and fetches full blocks for the
// latest cached blocks.
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
		// maxBlocks from the tip).
		startHeight := lastTip + 1
		minHeight := best.Height - cache.maxBlocks + 1
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
	case "getrawtransaction":
		result, rpcErr = h.handleGetRawTransaction(req.Params)
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

// rawTxVerboseResult matches Bitcoin Core's verbose getrawtransaction response.
type rawTxVerboseResult struct {
	Hex       string      `json:"hex"`
	TxID      string      `json:"txid"`
	Hash      string      `json:"hash"`
	Size      int         `json:"size"`
	VSize     int         `json:"vsize"`
	Weight    int         `json:"weight"`
	Version   int32       `json:"version"`
	LockTime  uint32      `json:"locktime"`
	Vin       []txVinResult  `json:"vin"`
	Vout      []txVoutResult `json:"vout"`
	BlockHash string      `json:"blockhash"`
	Confirmations int32   `json:"confirmations"`
	BlockTime int64       `json:"blocktime"`
	Time      int64       `json:"time"`
}

type txVinResult struct {
	TxID      string    `json:"txid,omitempty"`
	Vout      uint32    `json:"vout,omitempty"`
	ScriptSig *scriptSigResult `json:"scriptSig,omitempty"`
	Coinbase  string    `json:"coinbase,omitempty"`
	TxInWitness []string `json:"txinwitness,omitempty"`
	Sequence  uint32    `json:"sequence"`
}

type scriptSigResult struct {
	Hex string `json:"hex"`
}

type txVoutResult struct {
	Value        float64          `json:"value"`
	N            int              `json:"n"`
	ScriptPubKey scriptPubKeyResult `json:"scriptPubKey"`
}

type scriptPubKeyResult struct {
	Hex  string `json:"hex"`
	Type string `json:"type"`
}

// handleGetRawTransaction implements the getrawtransaction RPC.
// Params: [txid, verbose=false]
// Searches only in cached recent blocks.
func (h *rpcHandler) handleGetRawTransaction(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, &rpcError{Code: -1, Message: "getrawtransaction requires 1 parameter"}
	}

	var txidStr string
	if err := json.Unmarshal(params[0], &txidStr); err != nil {
		return nil, &rpcError{Code: -1, Message: "invalid txid parameter"}
	}

	txid, err := chainhash.NewHashFromStr(txidStr)
	if err != nil {
		return nil, &rpcError{Code: -1, Message: "invalid transaction hash"}
	}

	verbose := false
	if len(params) >= 2 {
		// Bitcoin Core accepts both bool and int for this parameter.
		var verboseRaw json.RawMessage
		verboseRaw = params[1]
		var verboseBool bool
		var verboseInt int
		if json.Unmarshal(verboseRaw, &verboseBool) == nil {
			verbose = verboseBool
		} else if json.Unmarshal(verboseRaw, &verboseInt) == nil {
			verbose = verboseInt != 0
		} else {
			return nil, &rpcError{Code: -1, Message: "invalid verbose parameter"}
		}
	}

	tx, block, _ := h.cache.getTx(txid)
	if tx == nil {
		return nil, &rpcError{Code: -5, Message: "No such mempool or blockchain transaction (only recent blocks are available)"}
	}

	var buf bytes.Buffer
	if err := tx.Serialize(&buf); err != nil {
		return nil, &rpcError{Code: -1, Message: "failed to serialize transaction"}
	}
	txHex := hex.EncodeToString(buf.Bytes())

	if !verbose {
		return txHex, nil
	}

	best, err := h.cs.BestBlock()
	if err != nil {
		return nil, &rpcError{Code: -1, Message: "failed to get best block"}
	}

	blockHash := block.Hash()
	blockTime := block.MsgBlock().Header.Timestamp.Unix()
	confirmations := best.Height - block.Height() + 1
	if confirmations < 0 {
		confirmations = 0
	}

	// Build vin.
	vins := make([]txVinResult, len(tx.TxIn))
	for i, in := range tx.TxIn {
		vin := txVinResult{
			Sequence: in.Sequence,
		}
		if i == 0 && tx.TxIn[0].PreviousOutPoint.Hash == (chainhash.Hash{}) {
			vin.Coinbase = hex.EncodeToString(in.SignatureScript)
		} else {
			vin.TxID = in.PreviousOutPoint.Hash.String()
			vin.Vout = in.PreviousOutPoint.Index
			vin.ScriptSig = &scriptSigResult{
				Hex: hex.EncodeToString(in.SignatureScript),
			}
		}
		if len(in.Witness) > 0 {
			witness := make([]string, len(in.Witness))
			for j, w := range in.Witness {
				witness[j] = hex.EncodeToString(w)
			}
			vin.TxInWitness = witness
		}
		vins[i] = vin
	}

	// Build vout.
	vouts := make([]txVoutResult, len(tx.TxOut))
	for i, out := range tx.TxOut {
		vouts[i] = txVoutResult{
			Value: float64(out.Value) / 1e8,
			N:     i,
			ScriptPubKey: scriptPubKeyResult{
				Hex:  hex.EncodeToString(out.PkScript),
				Type: scriptType(out.PkScript),
			},
		}
	}

	// Compute wtxid (hash including witness).
	var wtxBuf bytes.Buffer
	tx.Serialize(&wtxBuf)
	wtxHash := chainhash.DoubleHashH(wtxBuf.Bytes())

	// Compute sizes.
	size := tx.SerializeSize()
	var noWitBuf bytes.Buffer
	tx.SerializeNoWitness(&noWitBuf)
	strippedSize := noWitBuf.Len()
	weight := strippedSize*3 + size
	vsize := (weight + 3) / 4

	return rawTxVerboseResult{
		Hex:           txHex,
		TxID:          txid.String(),
		Hash:          wtxHash.String(),
		Size:          size,
		VSize:         vsize,
		Weight:        weight,
		Version:       tx.Version,
		LockTime:      tx.LockTime,
		Vin:           vins,
		Vout:          vouts,
		BlockHash:     blockHash.String(),
		Confirmations: confirmations,
		BlockTime:     blockTime,
		Time:          blockTime,
	}, nil
}

// scriptType returns a basic classification of the output script type.
func scriptType(pkScript []byte) string {
	switch {
	case len(pkScript) == 25 && pkScript[0] == 0x76 && pkScript[1] == 0xa9 &&
		pkScript[2] == 0x14 && pkScript[23] == 0x88 && pkScript[24] == 0xac:
		return "pubkeyhash"
	case len(pkScript) == 23 && pkScript[0] == 0xa9 && pkScript[1] == 0x14 &&
		pkScript[22] == 0x87:
		return "scripthash"
	case len(pkScript) == 22 && pkScript[0] == 0x00 && pkScript[1] == 0x14:
		return "witness_v0_keyhash"
	case len(pkScript) == 34 && pkScript[0] == 0x00 && pkScript[1] == 0x20:
		return "witness_v0_scripthash"
	case len(pkScript) == 34 && pkScript[0] == 0x51 && pkScript[1] == 0x20:
		return "witness_v1_taproot"
	case len(pkScript) == 35 && pkScript[34] == 0xac:
		return "pubkey"
	case len(pkScript) > 0 && pkScript[len(pkScript)-1] == 0xae:
		return "multisig"
	case len(pkScript) > 1 && pkScript[0] == 0x6a:
		return "nulldata"
	default:
		return "nonstandard"
	}
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
