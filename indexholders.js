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
const CONCURRENCY        = 20;   // max parallel balanceOf calls at any time
const MAX_RETRIES        = 5;
const RETRY_DELAY_MS     = 1000;

const OUTPUT_FILE     = path.resolve(__dirname, "holders.csv");
const CHECKPOINT_FILE = path.resolve(__dirname, ".checkpoint.json");

const TRANSFER_TOPIC  = ethers.id("Transfer(address,address,uint256)");
const ZERO_ADDR       = "0x0000000000000000000000000000000000000000";

/////////////////////////////
// CHECKPOINT SCHEMA
//
// {
//   latestBlock : number    — target block for this run
//   nextBlock   : number    — next batch to fetch on resume
//   balances    : { [addr]: string }  — all resolved balances so far
//   failed      : string[]  — addresses that exhausted all retries
// }
/////////////////////////////

function loadCheckpoint() {
    if (!fs.existsSync(CHECKPOINT_FILE)) return null;
    try { return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8")); }
    catch { return null; }
}

function saveCheckpoint(data) {
    const tmp = CHECKPOINT_FILE + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify(data), "utf8");
    fs.renameSync(tmp, CHECKPOINT_FILE);
}

function deleteCheckpoint() {
    for (const f of [CHECKPOINT_FILE, CHECKPOINT_FILE + ".tmp"]) {
        if (fs.existsSync(f)) fs.unlinkSync(f);
    }
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
            process.stderr.write(`\n[retry ${attempt}/${MAX_RETRIES}] ${label} — ${err.message} — waiting ${delay}ms\n`);
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
// CONCURRENCY LIMITER
// Ensures at most CONCURRENCY balanceOf calls are in-flight at once.
// Callers await acquire() before starting a call, release() when done.
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
            if (queue.length > 0) {
                active++;
                queue.shift()();
            }
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

    // Load checkpoint — discard if it's for a different target block
    let cp = loadCheckpoint();
    if (cp && cp.latestBlock !== latestBlock) {
        console.log(`[checkpoint] Discarding stale checkpoint (was for block ${cp.latestBlock})`);
        deleteCheckpoint();
        cp = null;
    }

    // Restore state from checkpoint or start fresh
    // balances: address → balance string (only addresses with bal > 0 are stored)
    const balances = new Map(
        cp ? Object.entries(cp.balances) : []
    );
    const seen   = new Set(cp ? Object.keys(cp.balances).concat(cp.failed || []) : []);
    const failed = new Set(cp ? cp.failed || [] : []);

    const resumeFrom = (cp && cp.nextBlock) ? cp.nextBlock : NFT_CREATION_BLOCK;

    if (resumeFrom > NFT_CREATION_BLOCK) {
        console.log(`Resuming from block ${resumeFrom} (${seen.size} addresses already processed, ${balances.size} holders found)`);
    } else {
        console.log(`Scanning from block ${NFT_CREATION_BLOCK} → ${latestBlock}`);
    }

    const sem = makeSemaphore(CONCURRENCY);

    // Tracks all in-flight balanceOf promises so we can await them at the end
    const inFlight = [];

    // Checkpoint flush counter — save every 100 resolved addresses
    let resolvedSinceLastSave = 0;
    const CHECKPOINT_EVERY = 100;

    function flushCheckpoint(nextBlock) {
        saveCheckpoint({
            latestBlock,
            nextBlock,
            balances: Object.fromEntries(balances),
            failed  : Array.from(failed)
        });
        resolvedSinceLastSave = 0;
    }

    /////////////////////////////
    // dispatch: called the moment a new address is extracted from a log.
    // Fires balanceOf immediately without blocking the log scan loop.
    /////////////////////////////

    function dispatch(addr) {
        if (seen.has(addr)) return; // already querying or resolved
        seen.add(addr);

        const p = (async () => {
            await sem.acquire();
            try {
                const bal = await withRetry(
                    () => contract.balanceOf(addr),
                    `balanceOf(${addr})`
                );

                if (bal > 0n) {
                    balances.set(addr, bal.toString());
                    console.log(`${addr}  =>  ${bal.toString()}`);
                }

                resolvedSinceLastSave++;

            } catch (err) {
                failed.add(addr);
                process.stderr.write(`[FAILED] ${addr}: ${err.message}\n`);
                resolvedSinceLastSave++;
            } finally {
                sem.release();
            }
        })();

        inFlight.push(p);
    }

    /////////////////////////////
    // LOG SCAN — fires balanceOf for each new address immediately
    /////////////////////////////

    for (let from = resumeFrom; from <= latestBlock; from += BLOCK_BATCH) {
        const to = Math.min(from + BLOCK_BATCH - 1, latestBlock);
        process.stdout.write(`\r[scan] Logs ${from} → ${to}  (${seen.size} addresses dispatched)   `);

        const logs = await getLogs(from, to);

        for (const log of logs) {
            const fromAddr = "0x" + log.topics[1].slice(26).toLowerCase();
            const toAddr   = "0x" + log.topics[2].slice(26).toLowerCase();

            if (fromAddr !== ZERO_ADDR) dispatch(fromAddr);
            if (toAddr   !== ZERO_ADDR) dispatch(toAddr);
        }

        // Save checkpoint after each batch — nextBlock is where we'd resume
        // Only flush if enough have resolved to be worth writing
        if (resolvedSinceLastSave >= CHECKPOINT_EVERY) {
            flushCheckpoint(from + BLOCK_BATCH);
        } else {
            // Always update nextBlock even if we skip the full flush
            saveCheckpoint({
                latestBlock,
                nextBlock: from + BLOCK_BATCH,
                balances : Object.fromEntries(balances),
                failed   : Array.from(failed)
            });
        }
    }

    console.log(`\n[scan] Complete — ${seen.size} unique addresses dispatched`);
    console.log(`Waiting for ${inFlight.length} in-flight balanceOf calls to finish...`);

    // Wait for all outstanding balanceOf calls to complete
    await Promise.allSettled(inFlight);

    /////////////////////////////
    // SORT: highest balance first, alphabetical on ties
    /////////////////////////////

    const holders = Array.from(balances.entries())
        .map(([addr, bal]) => [addr, BigInt(bal)])
        .sort(([addrA, balA], [addrB, balB]) => {
            if (balB !== balA) return Number(balB - balA);
            return addrA.localeCompare(addrB);
        });

    /////////////////////////////
    // WRITE CSV
    /////////////////////////////

    const writeStream = fs.createWriteStream(OUTPUT_FILE);

    function writeLine(line) {
        return new Promise(resolve => {
            const ok = writeStream.write(line + "\n");
            if (ok) resolve();
            else writeStream.once("drain", resolve);
        });
    }

    await writeLine("address,balance");
    for (const [addr, bal] of holders) {
        await writeLine(`${addr},${bal}`);
    }
    await new Promise(resolve => writeStream.end(resolve));

    /////////////////////////////
    // CLEANUP + SUMMARY
    /////////////////////////////

    deleteCheckpoint();

    if (failed.size > 0) {
        fs.writeFileSync(
            path.resolve(__dirname, "failed.txt"),
            Array.from(failed).join("\n") + "\n"
        );
    }

    console.log(`\n--- Summary ---`);
    console.log(`Addresses scanned   : ${seen.size}`);
    console.log(`Holders (bal > 0)   : ${holders.length}`);
    console.log(`Failed (all retries): ${failed.size}${failed.size > 0 ? "  → see failed.txt" : ""}`);
    console.log(`CSV saved           : ${OUTPUT_FILE}`);
}

main().catch(err => {
    console.error("\nFatal error:", err.message);
    console.error("Progress saved — re-run to resume.");
    process.exit(1);
});
