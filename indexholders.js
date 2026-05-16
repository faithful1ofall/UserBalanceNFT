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
const CONCURRENCY        = 20;    // parallel balanceOf calls
const MAX_RETRIES        = 5;
const RETRY_DELAY_MS     = 1000;

// How many balanceOf results to buffer before flushing CSV + checkpoint.
// Higher = fewer flushes = less I/O. Lower = more frequent live updates.
// At 500k holders, each flush sorts + writes ~24 MB. 500 is a good balance.
const FLUSH_EVERY        = 500;

const OUTPUT_FILE     = path.resolve(__dirname, "holders.csv");
const CHECKPOINT_FILE = path.resolve(__dirname, ".checkpoint.json");

const TRANSFER_TOPIC  = ethers.id("Transfer(address,address,uint256)");
const ZERO_ADDR       = "0x0000000000000000000000000000000000000000";

/////////////////////////////
// CHECKPOINT SCHEMA
//
// {
//   contract  : string   — NFT contract address (guards against wrong file)
//   nextBlock : number   — next block to scan on the next run
//   balances  : { [addr]: number }  — resolved balances > 0, stored as Number
//   failed    : string[] — addresses that exhausted all retries
// }
//
// Checkpoint is NEVER deleted. nextBlock advances to latestBlock+1 so
// the next run extends from where this one ended.
/////////////////////////////

function loadCheckpoint() {
    if (!fs.existsSync(CHECKPOINT_FILE)) return null;
    try { return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8")); }
    catch { return null; }
}

// Checkpoint is written incrementally using a streaming JSON approach:
// instead of JSON.stringify(entireObject) we build the file manually
// so we never allocate a full copy of the balances map in memory.
function saveCheckpoint(nextBlock, balances, failed) {
    const tmp = CHECKPOINT_FILE + ".tmp";
    const ws  = fs.createWriteStream(tmp);

    ws.write(`{"contract":${JSON.stringify(NFT_CONTRACT.toLowerCase())},"nextBlock":${nextBlock},"balances":{`);

    let first = true;
    for (const [addr, bal] of balances) {
        if (!first) ws.write(",");
        ws.write(`${JSON.stringify(addr)}:${bal}`);
        first = false;
    }

    ws.write(`},"failed":[`);
    let fi = 0;
    for (const addr of failed) {
        if (fi++ > 0) ws.write(",");
        ws.write(JSON.stringify(addr));
    }
    ws.write("]}");
    ws.end();

    // Wait for the stream to finish before renaming
    return new Promise((resolve, reject) => {
        ws.on("finish", () => {
            try { fs.renameSync(tmp, CHECKPOINT_FILE); resolve(); }
            catch (e) { reject(e); }
        });
        ws.on("error", reject);
    });
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
// CSV WRITER
//
// Writes the full sorted CSV using a streaming approach:
// - Sorts entries by balance desc, then address asc
// - Streams rows directly to disk — never builds the full file in memory
// - Uses Number for balances (safe: NFT counts fit in 53-bit integers)
// - Serialized via a write queue so concurrent calls never interleave
/////////////////////////////

let csvWriteQueue = Promise.resolve();

function writeCSV(balances) {
    csvWriteQueue = csvWriteQueue.then(() => new Promise((resolve, reject) => {
        // Sort: highest balance first, alphabetical on ties
        // Balances stored as Number — sort is ~2.7x faster than BigInt
        const sorted = Array.from(balances.entries())
            .sort(([addrA, balA], [addrB, balB]) => {
                if (balB !== balA) return balB - balA;
                return addrA.localeCompare(addrB);
            });

        const tmp = OUTPUT_FILE + ".tmp";
        const ws  = fs.createWriteStream(tmp);

        ws.write("address,balance\n");

        let i = 0;
        function writeNext() {
            let ok = true;
            while (i < sorted.length && ok) {
                const [addr, bal] = sorted[i++];
                ok = ws.write(`${addr},${bal}\n`);
            }
            if (i < sorted.length) {
                ws.once("drain", writeNext);
            } else {
                ws.end();
            }
        }

        ws.on("finish", () => {
            try { fs.renameSync(tmp, OUTPUT_FILE); resolve(); }
            catch (e) { reject(e); }
        });
        ws.on("error", reject);

        writeNext();
    }));
    return csvWriteQueue;
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

    // balances: addr → Number (not BigInt — Number is sufficient for NFT counts
    // and is 2.7x faster to sort; max safe integer is 9 quadrillion)
    const balances = new Map(
        cp ? Object.entries(cp.balances).map(([a, b]) => [a, Number(b)]) : []
    );
    const failed   = new Set(cp ? (cp.failed || []) : []);
    // seen tracks every address dispatched (resolved or failed) so we never
    // re-query on resume. Rebuilt from balances + failed on load.
    const seen     = new Set([...balances.keys(), ...failed]);

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

    // pendingCount: number of dispatched balanceOf calls not yet settled.
    // Used instead of an inFlight array — avoids holding 500k promise refs.
    let pendingCount  = 0;
    let resolveIdle   = null; // set when we're waiting for all pending to drain

    // resolvedSinceFlush: how many balanceOf calls completed since last flush
    let resolvedSinceFlush = 0;
    let currentScanBlock   = resumeFrom; // updated by the scan loop

    async function flush() {
        resolvedSinceFlush = 0;
        await Promise.all([
            writeCSV(balances),
            saveCheckpoint(currentScanBlock, balances, failed)
        ]);
    }

    /////////////////////////////
    // dispatch — fires balanceOf the instant an address is seen in a log.
    // Does NOT push to an array — uses a counter to track in-flight work.
    /////////////////////////////

    function dispatch(addr) {
        if (seen.has(addr)) return;
        seen.add(addr);
        pendingCount++;

        (async () => {
            await sem.acquire();
            try {
                const bal = await withRetry(
                    () => contract.balanceOf(addr),
                    `balanceOf(${addr})`
                );

                // Store as Number — safe for NFT balances (max ~9 quadrillion)
                const balNum = Number(bal);
                if (balNum > 0) {
                    balances.set(addr, balNum);
                    console.log(`${addr}  =>  ${balNum}`);
                }

            } catch (err) {
                failed.add(addr);
                process.stderr.write(`[FAILED] ${addr}: ${err.message}\n`);
            } finally {
                sem.release();
                pendingCount--;
                resolvedSinceFlush++;

                // Flush CSV + checkpoint every FLUSH_EVERY completions
                if (resolvedSinceFlush >= FLUSH_EVERY) {
                    await flush();
                }

                // If the scan loop is waiting for all pending to drain, signal it
                if (pendingCount === 0 && resolveIdle) {
                    resolveIdle();
                    resolveIdle = null;
                }
            }
        })();
    }

    // Returns a promise that resolves when all in-flight dispatches settle
    function waitForIdle() {
        if (pendingCount === 0) return Promise.resolve();
        return new Promise(resolve => { resolveIdle = resolve; });
    }

    /////////////////////////////
    // LOG SCAN
    /////////////////////////////

    for (let from = resumeFrom; from <= latestBlock; from += BLOCK_BATCH) {
        const to = Math.min(from + BLOCK_BATCH - 1, latestBlock);
        currentScanBlock = from + BLOCK_BATCH; // next block to resume from on crash
        process.stdout.write(`\r[scan] Logs ${from} → ${to}  (${seen.size} dispatched, ${balances.size} holders)   `);

        const logs = await getLogs(from, to);

        for (const log of logs) {
            const fromAddr = "0x" + log.topics[1].slice(26).toLowerCase();
            const toAddr   = "0x" + log.topics[2].slice(26).toLowerCase();
            if (fromAddr !== ZERO_ADDR) dispatch(fromAddr);
            if (toAddr   !== ZERO_ADDR) dispatch(toAddr);
        }

        // Lightweight checkpoint after every batch — only writes nextBlock
        // Full flush (balances + CSV) happens every FLUSH_EVERY completions
        await saveCheckpoint(currentScanBlock, balances, failed);
    }

    console.log(`\n[scan] Complete — ${seen.size} addresses dispatched`);
    console.log(`Waiting for ${pendingCount} in-flight balanceOf calls to settle...`);

    await waitForIdle();

    // Final flush — write complete sorted CSV and final checkpoint
    await flush();

    // Advance nextBlock to latestBlock+1 for the next incremental run
    await saveCheckpoint(latestBlock + 1, balances, failed);

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
