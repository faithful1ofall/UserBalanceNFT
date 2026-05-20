const fs   = require("fs");
const path = require("path");
const { ethers } = require("ethers");

/////////////////////////////
// CONFIG
/////////////////////////////

const RPC_URL            = "https://arc-testnet.g.alchemy.com/v2/o1k50yOLGXHrczBA8KDOf";
const NFT_CONTRACT       = "0x9e05c6075f9e890fc515ef86091414c77036f8fa";
const NFT_CREATION_BLOCK = 9435462;
const BLOCK_BATCH        = 10000;
const MAX_RETRIES        = 5;
const RETRY_DELAY_MS     = 1000;

// Many RPC providers cap eth_getLogs at 1000 results per call. Some throw an
// error when the limit is hit; others silently return exactly 1000 and drop
// the rest. We treat hitting this limit as a signal to split the range, even
// when no error is thrown.
const RPC_LOG_LIMIT = 1000;

const OUTPUT_FILE     = path.resolve(__dirname, "holders.csv");
const CHECKPOINT_FILE = path.resolve(__dirname, ".checkpoint.json");

const TRANSFER_TOPIC = ethers.id("Transfer(address,address,uint256)");
const ZERO_ADDR      = "0x0000000000000000000000000000000000000000";

/////////////////////////////
// CHECKPOINT
/////////////////////////////

function loadCheckpoint() {
    if (!fs.existsSync(CHECKPOINT_FILE)) return null;
    try { return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8")); }
    catch { return null; }
}

function saveCheckpoint(nextBlock, balances) {
    let json = `{"contract":${JSON.stringify(NFT_CONTRACT.toLowerCase())},"nextBlock":${nextBlock},"balances":{`;
    let first = true;
    for (const [addr, bal] of balances) {
        if (!first) json += ",";
        json += `${JSON.stringify(addr)}:${bal}`;
        first = false;
    }
    json += "}}";

    const fd = fs.openSync(CHECKPOINT_FILE, "w");
    try {
        fs.writeSync(fd, json, 0, "utf8");
        fs.fsyncSync(fd);
    } finally {
        fs.closeSync(fd);
    }
}

/////////////////////////////
// CSV WRITER
/////////////////////////////

function writeCSV(balances) {
    const sorted = Array.from(balances.entries())
        .sort(([addrA, balA], [addrB, balB]) => {
            if (balB !== balA) return balB - balA;
            return addrA.localeCompare(addrB);
        });

    let csv = "address,balance\n";
    for (const [addr, bal] of sorted) {
        csv += `${addr},${bal}\n`;
    }

    const fd = fs.openSync(OUTPUT_FILE, "w");
    try {
        fs.writeSync(fd, csv, 0, "utf8");
        fs.fsyncSync(fd);
    } finally {
        fs.closeSync(fd);
    }
}

/////////////////////////////
// PROVIDER
/////////////////////////////

const provider = new ethers.JsonRpcProvider(RPC_URL);

/////////////////////////////
// RETRY WRAPPER
/////////////////////////////

async function withRetry(fn, label) {
    let lastErr;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try { return await fn(); }
        catch (err) {
            lastErr = err;
            const delay = RETRY_DELAY_MS * Math.pow(2, attempt - 1);
            process.stderr.write(
                `\n[retry ${attempt}/${MAX_RETRIES}] ${label} — ${err.message} — waiting ${delay}ms\n`
            );
            await new Promise(r => setTimeout(r, delay));
        }
    }
    throw lastErr;
}

/////////////////////////////
// SAFE LOG FETCH
//
// Splits the block range recursively when:
//   (a) the RPC throws (e.g. "query returned more than N results"), OR
//   (b) the response contains exactly RPC_LOG_LIMIT entries — silent
//       truncation where the provider caps at 1000 with HTTP 200.
/////////////////////////////

async function getLogs(fromBlock, toBlock) {
    let logs;
    try {
        logs = await withRetry(
            () => provider.getLogs({
                address: NFT_CONTRACT,
                fromBlock,
                toBlock,
                topics: [TRANSFER_TOPIC]
            }),
            `getLogs(${fromBlock}-${toBlock})`
        );
    } catch (err) {
        if (fromBlock === toBlock) throw err;
        const mid = Math.floor((fromBlock + toBlock) / 2);
        const [left, right] = await Promise.all([
            getLogs(fromBlock, mid),
            getLogs(mid + 1, toBlock)
        ]);
        return [...left, ...right];
    }

    // Silent truncation guard: split and re-fetch if we hit the cap.
    if (logs.length >= RPC_LOG_LIMIT && fromBlock !== toBlock) {
        const mid = Math.floor((fromBlock + toBlock) / 2);
        const [left, right] = await Promise.all([
            getLogs(fromBlock, mid),
            getLogs(mid + 1, toBlock)
        ]);
        return [...left, ...right];
    }

    if (logs.length >= RPC_LOG_LIMIT && fromBlock === toBlock) {
        process.stderr.write(
            `[WARN] Block ${fromBlock} returned ${logs.length} logs (>= RPC_LOG_LIMIT=${RPC_LOG_LIMIT}). ` +
            `Logs may be truncated — consider a provider with cursor pagination.\n`
        );
    }

    return logs;
}

/////////////////////////////
// APPLY LOGS TO BALANCES
//
// Derives balances purely from Transfer event arithmetic — no balanceOf
// RPC calls needed. Each log adjusts two counters at most:
//   sender (non-zero from): balance -= 1
//   receiver (non-zero to):  balance += 1
// Mints have from=0x0 (only receiver credited).
// Burns have to=0x0 (only sender debited).
/////////////////////////////

function applyLogs(logs, balances) {
    let newAddrs = 0;
    for (const log of logs) {
        if (!log.topics[1] || log.topics[1].length !== 66 ||
            !log.topics[2] || log.topics[2].length !== 66) {
            process.stderr.write(
                `[WARN] Skipping malformed log in block ${log.blockNumber}: topics=${JSON.stringify(log.topics)}\n`
            );
            continue;
        }

        const from = "0x" + log.topics[1].slice(26).toLowerCase();
        const to   = "0x" + log.topics[2].slice(26).toLowerCase();

        if (from !== ZERO_ADDR) {
            if (!balances.has(from)) { balances.set(from, 0); newAddrs++; }
            balances.set(from, balances.get(from) - 1);
        }
        if (to !== ZERO_ADDR) {
            if (!balances.has(to)) { balances.set(to, 0); newAddrs++; }
            balances.set(to, balances.get(to) + 1);
        }
    }
    return newAddrs;
}

/////////////////////////////
// MAIN
/////////////////////////////

async function main() {

    const latestBlock = process.argv[2]
        ? Number(process.argv[2])
        : await withRetry(() => provider.getBlockNumber(), "getBlockNumber");

    console.log(`Target block: ${latestBlock}\n`);

    const cp = loadCheckpoint();

    if (cp && cp.contract && cp.contract !== NFT_CONTRACT.toLowerCase()) {
        console.error(
            `[checkpoint] Contract mismatch — expected ${NFT_CONTRACT}, got ${cp.contract}. ` +
            `Delete .checkpoint.json to reset.`
        );
        process.exit(1);
    }

    // balances: address -> token count, derived entirely from Transfer logs.
    const balances = new Map(
        cp ? Object.entries(cp.balances).map(([a, b]) => [a, Number(b)]) : []
    );

    const resumeFrom = (cp && cp.nextBlock) ? cp.nextBlock : NFT_CREATION_BLOCK;

    if (resumeFrom > NFT_CREATION_BLOCK) {
        const aboveZero = [...balances.values()].filter(b => b > 0).length;
        console.log(`Resuming from block ${resumeFrom}`);
        console.log(`  ${balances.size} addresses in checkpoint (${aboveZero} with balance > 0)\n`);
    } else {
        console.log(`Starting fresh scan from block ${NFT_CREATION_BLOCK}\n`);
    }

    if (resumeFrom > latestBlock) {
        console.log(`Already up to date (checkpoint nextBlock ${resumeFrom} > target ${latestBlock})`);
        process.exit(0);
    }

    let currentScanBlock = resumeFrom;
    let shuttingDown     = false;
    let totalLogs        = 0;

    /////////////////////////////
    // SIGINT / SIGTERM handler
    /////////////////////////////

    function shutdown(signal) {
        if (shuttingDown) return;
        shuttingDown = true;
        process.stdout.write(`\n[${signal}] Saving checkpoint before exit...\n`);
        try {
            saveCheckpoint(currentScanBlock, balances);
            writeCSV(balances);
            process.stdout.write(`Checkpoint saved at block ${currentScanBlock}. Re-run to resume.\n`);
        } catch (e) {
            process.stderr.write(`Failed to save checkpoint: ${e.message}\n`);
        }
        process.exit(0);
    }

    process.on("SIGINT",  () => shutdown("SIGINT"));
    process.on("SIGTERM", () => shutdown("SIGTERM"));

    /////////////////////////////
    // LOG SCAN
    /////////////////////////////

    for (let from = resumeFrom; from <= latestBlock; from += BLOCK_BATCH) {
        if (shuttingDown) break;
        const to = Math.min(from + BLOCK_BATCH - 1, latestBlock);

        const logs = await getLogs(from, to);
        applyLogs(logs, balances);

        totalLogs       += logs.length;
        currentScanBlock = from + BLOCK_BATCH;

        const aboveZero = [...balances.values()].filter(b => b > 0).length;
        process.stdout.write(
            `\r[scan] ${from}->${to}  logs=${logs.length}  unique=${balances.size}  bal>0=${aboveZero}   `
        );

        saveCheckpoint(currentScanBlock, balances);
    }

    if (shuttingDown) return;

    writeCSV(balances);
    saveCheckpoint(latestBlock + 1, balances);

    const aboveZero = [...balances.values()].filter(b => b > 0).length;

    console.log(`\n\n--- Summary ---`);
    console.log(`Blocks scanned      : ${resumeFrom} -> ${latestBlock}`);
    console.log(`Total logs processed: ${totalLogs}`);
    console.log(`Total unique addrs  : ${balances.size}`);
    console.log(`  — with balance > 0: ${aboveZero}`);
    console.log(`  — with balance = 0: ${balances.size - aboveZero}`);
    console.log(`CSV                 : ${OUTPUT_FILE}`);
    console.log(`Next run resumes at : block ${latestBlock + 1}`);
}

main().catch(err => {
    console.error("\nFatal error:", err.message);
    console.error("Progress saved — re-run to resume.");
    process.exit(1);
});
