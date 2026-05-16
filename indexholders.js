const fs   = require("fs");
const path = require("path");
const { ethers } = require("ethers");

/////////////////////////////
// CONFIG
/////////////////////////////

const RPC_URL            = "https://arc-testnet.g.alchemy.com/v2/o1k50yOLGXHrczBA8KDOf";
const NFT_CONTRACT       = "0x9e05c6075f9e890fc515ef86091414c77036f8fa";
const NFT_CREATION_BLOCK = 9435462;
const BLOCK_BATCH        = 2000;
const CONCURRENCY        = 20;
const MAX_RETRIES        = 5;
const RETRY_DELAY_MS     = 1000;

const OUTPUT_FILE     = path.resolve(__dirname, "holders.csv");
const CHECKPOINT_FILE = path.resolve(__dirname, ".checkpoint.json");

const TRANSFER_TOPIC  = ethers.id("Transfer(address,address,uint256)");
const ZERO_ADDR       = "0x0000000000000000000000000000000000000000";

/////////////////////////////
// CHECKPOINT
/////////////////////////////

function loadCheckpoint() {
    if (!fs.existsSync(CHECKPOINT_FILE)) return null;
    try { return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8")); }
    catch { return null; }
}

// Writes checkpoint by iterating the Map directly — no full object copy.
// Uses open('w')+write+fsync+close — no tmp file, no rename.
// This avoids the ENOENT crash on Android/Termux where renameSync fails
// when the source .tmp file isn't visible on the filesystem yet.
// fsync ensures bytes are on disk before returning.
function saveCheckpoint(nextBlock, balances, failed) {
    let json = `{"contract":${JSON.stringify(NFT_CONTRACT.toLowerCase())},"nextBlock":${nextBlock},"balances":{`;

    let first = true;
    for (const [addr, bal] of balances) {
        if (!first) json += ",";
        json += `${JSON.stringify(addr)}:${bal}`;
        first = false;
    }

    json += `},"failed":[`;
    let fi = 0;
    for (const addr of failed) {
        if (fi++ > 0) json += ",";
        json += JSON.stringify(addr);
    }
    json += "]}";

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
//
// Rewrites the full sorted CSV on every call.
// Serialized via a write queue — concurrent completions never interleave.
// Uses writeFileSync (not a stream) to avoid the ENOENT rename race on
// Termux/Android where finish fires before the file is visible on disk.
// Called on every resolved balance so the file is always current.
/////////////////////////////

let csvWriteQueue = Promise.resolve();

function writeCSV(balances) {
    // Chain onto the queue — concurrent completions never interleave
    csvWriteQueue = csvWriteQueue.then(() => {
        const sorted = Array.from(balances.entries())
            .sort(([addrA, balA], [addrB, balB]) => {
                if (balB !== balA) return balB - balA;
                return addrA.localeCompare(addrB);
            });

        // Build CSV string then write via open('w')+write+fsync+close.
        // No tmp file, no rename — avoids ENOENT on Android/Termux.
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
    });
    return csvWriteQueue;
}

/////////////////////////////
// PROVIDER + CONTRACT
/////////////////////////////

const provider = new ethers.JsonRpcProvider(RPC_URL);

const contract = new ethers.Contract(
    NFT_CONTRACT,
    ["function balanceOf(address) view returns (uint256)"],
    provider
);

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
/////////////////////////////

async function getLogs(fromBlock, toBlock) {
    try {
        return await withRetry(
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
}

/////////////////////////////
// SEMAPHORE
/////////////////////////////

function makeSemaphore(limit) {
    let active = 0;
    const queue = [];
    return {
        acquire() {
            return new Promise(resolve => {
                if (active < limit) { active++; resolve(); }
                else queue.push(resolve);
            });
        },
        release() {
            active--;
            if (queue.length > 0) { active++; queue.shift()(); }
        }
    };
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
        console.error(`[checkpoint] Contract mismatch — expected ${NFT_CONTRACT}, got ${cp.contract}. Delete .checkpoint.json to reset.`);
        process.exit(1);
    }

    const balances = new Map(
        cp ? Object.entries(cp.balances).map(([a, b]) => [a, Number(b)]) : []
    );
    const failed  = new Set(cp ? (cp.failed || []) : []);
    const seen    = new Set([...balances.keys(), ...failed]);

    const resumeFrom = (cp && cp.nextBlock) ? cp.nextBlock : NFT_CREATION_BLOCK;

    if (resumeFrom > NFT_CREATION_BLOCK) {
        console.log(`Resuming from block ${resumeFrom}`);
        console.log(`  ${balances.size} holders already found, ${seen.size} addresses already processed\n`);
    } else {
        console.log(`Starting fresh scan from block ${NFT_CREATION_BLOCK}\n`);
    }

    if (resumeFrom > latestBlock) {
        console.log(`Already up to date (checkpoint nextBlock ${resumeFrom} > target ${latestBlock})`);
        console.log(`Run without a block argument to scan to the latest block, or pass a higher block number.`);
        process.exit(0);
    }

    const sem = makeSemaphore(CONCURRENCY);

    let pendingCount       = 0;
    let resolveIdle        = null;
    let currentScanBlock   = resumeFrom;
    let shuttingDown       = false;

    // Flush checkpoint synchronously — called after every batch and on signal
    function flushCheckpoint() {
        saveCheckpoint(currentScanBlock, balances, failed);
    }

    /////////////////////////////
    // SIGINT / SIGTERM handler
    // Saves checkpoint with all resolved balances then exits cleanly.
    /////////////////////////////

    function shutdown(signal) {
        if (shuttingDown) return;
        shuttingDown = true;
        process.stdout.write(`\n[${signal}] Saving checkpoint before exit...\n`);
        try {
            saveCheckpoint(currentScanBlock, balances, failed);
            process.stdout.write(`Checkpoint saved at block ${currentScanBlock}. Re-run to resume.\n`);
        } catch (e) {
            process.stderr.write(`Failed to save checkpoint: ${e.message}\n`);
        }
        process.exit(0);
    }

    process.on("SIGINT",  () => shutdown("SIGINT"));
    process.on("SIGTERM", () => shutdown("SIGTERM"));

    /////////////////////////////
    // dispatch — fires balanceOf the instant an address is seen in a log.
    // Updates CSV and checkpoint immediately on every resolved balance.
    /////////////////////////////

    function dispatch(addr) {
        if (seen.has(addr) || shuttingDown) return;
        seen.add(addr);
        pendingCount++;

        (async () => {
            await sem.acquire();
            try {
                const bal = await withRetry(
                    () => contract.balanceOf(addr),
                    `balanceOf(${addr})`
                );

                const balNum = Number(bal);
                if (balNum > 0) {
                    balances.set(addr, balNum);
                    console.log(`${addr}  =>  ${balNum}`);

                    // Update CSV immediately on every new balance — real time
                    await writeCSV(balances);
                }

            } catch (err) {
                failed.add(addr);
                process.stderr.write(`[FAILED] ${addr}: ${err.message}\n`);
            } finally {
                sem.release();
                pendingCount--;

                if (pendingCount === 0 && resolveIdle) {
                    resolveIdle();
                    resolveIdle = null;
                }
            }
        })();
    }

    function waitForIdle() {
        if (pendingCount === 0) return Promise.resolve();
        return new Promise(resolve => { resolveIdle = resolve; });
    }

    /////////////////////////////
    // LOG SCAN
    /////////////////////////////

    for (let from = resumeFrom; from <= latestBlock; from += BLOCK_BATCH) {
        const to = Math.min(from + BLOCK_BATCH - 1, latestBlock);
        currentScanBlock = from + BLOCK_BATCH;
        process.stdout.write(`\r[scan] Logs ${from} → ${to}  (${seen.size} dispatched, ${balances.size} holders)   `);

        const logs = await getLogs(from, to);

        for (const log of logs) {
            const fromAddr = "0x" + log.topics[1].slice(26).toLowerCase();
            const toAddr   = "0x" + log.topics[2].slice(26).toLowerCase();
            if (fromAddr !== ZERO_ADDR) dispatch(fromAddr);
            if (toAddr   !== ZERO_ADDR) dispatch(toAddr);
        }

        // Save checkpoint after every batch — includes all balances resolved so far
        flushCheckpoint();
    }

    console.log(`\n[scan] Complete — ${seen.size} addresses dispatched`);
    console.log(`Waiting for ${pendingCount} in-flight balanceOf calls to settle...`);

    await waitForIdle();

    // Final CSV + checkpoint after all calls settle
    await writeCSV(balances);
    saveCheckpoint(latestBlock + 1, balances, failed);

    if (failed.size > 0) {
        fs.writeFileSync(
            path.resolve(__dirname, "failed.txt"),
            Array.from(failed).join("\n") + "\n"
        );
    }

    console.log(`\n--- Summary ---`);
    console.log(`Blocks scanned      : ${resumeFrom} → ${latestBlock}`);
    console.log(`Addresses processed : ${seen.size}`);
    console.log(`Holders (bal > 0)   : ${balances.size}`);
    console.log(`Failed (all retries): ${failed.size}${failed.size > 0 ? "  → see failed.txt" : ""}`);
    console.log(`CSV                 : ${OUTPUT_FILE}`);
    console.log(`Next run resumes at : block ${latestBlock + 1}`);
}

main().catch(err => {
    console.error("\nFatal error:", err.message);
    console.error("Progress saved — re-run to resume.");
    process.exit(1);
});
