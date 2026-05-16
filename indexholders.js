const fs   = require("fs");
const path = require("path");
const { ethers } = require("ethers");

/////////////////////////////
// CONFIG
/////////////////////////////

const RPC_URL          = "https://arc-testnet.g.alchemy.com/v2/o1k50yOLGXHrczBA8KDOf";
const NFT_CONTRACT     = "0x9e05c6075f9e890fc515ef86091414c77036f8fa";
const NFT_CREATION_BLOCK = 9435462;
const BLOCK_BATCH      = 2000;
const CONCURRENCY      = 20;   // parallel balanceOf calls
const MAX_RETRIES      = 5;    // retries per RPC call before giving up
const RETRY_DELAY_MS   = 1000; // base delay between retries (doubles each attempt)

const OUTPUT_FILE      = path.resolve(__dirname, "holders.csv");
const CHECKPOINT_FILE  = path.resolve(__dirname, ".checkpoint.json");
const ADDRESSES_FILE   = path.resolve(__dirname, ".addresses.json");

const TRANSFER_TOPIC   = ethers.id("Transfer(address,address,uint256)");
const ZERO_ADDR        = "0x0000000000000000000000000000000000000000";

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
// Retries any async fn up to MAX_RETRIES times with exponential backoff.
// Throws only after all retries are exhausted.
/////////////////////////////

async function withRetry(fn, label) {
    let lastErr;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            return await fn();
        } catch (err) {
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
// Auto-splits range on RPC errors, retries transient failures.
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
        // If range is a single block and still failing, propagate
        if (fromBlock === toBlock) throw err;

        // Split range and retry each half independently
        const mid = Math.floor((fromBlock + toBlock) / 2);
        const [left, right] = await Promise.all([
            getLogs(fromBlock, mid),
            getLogs(mid + 1, toBlock)
        ]);
        return [...left, ...right];
    }
}

/////////////////////////////
// CHECKPOINT HELPERS
// Saves/loads scan progress so a crashed run resumes from where it stopped.
/////////////////////////////

function loadCheckpoint() {
    if (fs.existsSync(CHECKPOINT_FILE)) {
        try {
            return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8"));
        } catch {
            return null;
        }
    }
    return null;
}

function saveCheckpoint(data) {
    fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(data), "utf8");
}

function loadAddresses() {
    if (fs.existsSync(ADDRESSES_FILE)) {
        try {
            return new Set(JSON.parse(fs.readFileSync(ADDRESSES_FILE, "utf8")));
        } catch {
            return new Set();
        }
    }
    return new Set();
}

function saveAddresses(addresses) {
    fs.writeFileSync(ADDRESSES_FILE, JSON.stringify(Array.from(addresses)), "utf8");
}

/////////////////////////////
// MAIN
/////////////////////////////

async function main() {

    const latestBlock = process.argv[2]
        ? Number(process.argv[2])
        : await withRetry(() => provider.getBlockNumber(), "getBlockNumber");

    console.log(`Target block: ${latestBlock}`);

    /////////////////////////////
    // PHASE 1: COLLECT ADDRESSES FROM TRANSFER LOGS
    // Resumes from last saved checkpoint block if interrupted.
    // Saves address set and checkpoint to disk after every batch.
    /////////////////////////////

    const checkpoint = loadCheckpoint();
    const addresses  = loadCheckpoint() ? loadAddresses() : new Set();

    // Resume from the block after the last completed batch
    const resumeFrom = (checkpoint && checkpoint.latestBlock === latestBlock)
        ? checkpoint.nextBlock
        : NFT_CREATION_BLOCK;

    if (resumeFrom > NFT_CREATION_BLOCK) {
        console.log(`Resuming log scan from block ${resumeFrom} (${addresses.size} addresses already collected)`);
    } else {
        console.log(`Starting log scan from block ${NFT_CREATION_BLOCK}`);
    }

    for (
        let from = resumeFrom;
        from <= latestBlock;
        from += BLOCK_BATCH
    ) {
        const to = Math.min(from + BLOCK_BATCH - 1, latestBlock);
        process.stdout.write(`\rScanning logs ${from} -> ${to}  (${addresses.size} addresses)   `);

        const logs = await getLogs(from, to);

        for (const log of logs) {
            const fromAddr = "0x" + log.topics[1].slice(26).toLowerCase();
            const toAddr   = "0x" + log.topics[2].slice(26).toLowerCase();

            if (fromAddr !== ZERO_ADDR) addresses.add(fromAddr);
            if (toAddr   !== ZERO_ADDR) addresses.add(toAddr);
        }

        // Persist progress after every batch so we can resume on crash
        saveCheckpoint({ latestBlock, nextBlock: from + BLOCK_BATCH });
        saveAddresses(addresses);
    }

    console.log(`\nUnique addresses found: ${addresses.size}`);

    /////////////////////////////
    // PHASE 2: QUERY balanceOf() FOR EVERY ADDRESS
    // - 20 concurrent workers, each pulling the next address atomically
    // - Retries each failed call up to MAX_RETRIES times
    // - Prints wallet + balance immediately as each result arrives
    // - Streams results to CSV as they come in (no full array in memory)
    // - Tracks failed addresses and reports them at the end
    /////////////////////////////

    const addrList = Array.from(addresses);
    console.log(`Querying balanceOf for ${addrList.length} addresses with ${CONCURRENCY} workers...\n`);

    // Stream CSV writes directly to disk — avoids holding all results in memory
    const writeStream = fs.createWriteStream(OUTPUT_FILE);

    // Write helper that respects backpressure
    function writeLine(line) {
        return new Promise(resolve => {
            const ok = writeStream.write(line + "\n");
            if (ok) resolve();
            else writeStream.once("drain", resolve);
        });
    }

    const holders  = [];   // kept for final sort — only addresses with bal > 0
    const seen     = new Set(); // guards against any duplicate address in addrList
    const failed   = [];   // addresses that exhausted all retries
    let   idx      = 0;
    let   queried  = 0;

    async function worker() {
        while (true) {
            // Atomically grab the next address (JS is single-threaded — safe)
            const i = idx++;
            if (i >= addrList.length) break;

            const addr = addrList[i];

            // Skip if already processed (defensive — Set guarantees uniqueness
            // but guards against any future code path that could introduce dupes)
            if (seen.has(addr)) continue;
            seen.add(addr);

            queried++;

            try {
                const bal = await withRetry(
                    () => contract.balanceOf(addr),
                    `balanceOf(${addr})`
                );

                if (bal > 0n) {
                    // Print immediately as each result arrives
                    console.log(`${addr}  =>  ${bal.toString()}`);
                    holders.push([addr, bal]);
                }

            } catch (err) {
                // All retries exhausted — record for reporting, don't silently drop
                failed.push(addr);
                process.stderr.write(`[FAILED] ${addr}: ${err.message}\n`);
            }
        }
    }

    await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

    /////////////////////////////
    // PHASE 3: SORT + WRITE FINAL CSV
    // Sort highest balance first, then alphabetical for ties.
    // CSV is written only after sorting — one entry per address, no duplicates.
    /////////////////////////////

    holders.sort(([addrA, balA], [addrB, balB]) => {
        if (balB !== balA) return Number(balB - balA);
        return addrA.localeCompare(addrB);
    });

    // Write header then sorted rows — nothing was written to the stream before this
    writeStream.write("address,balance\n");

    for (const [addr, bal] of holders) {
        await writeLine(`${addr},${bal}`);
    }

    await new Promise(resolve => writeStream.end(resolve));

    /////////////////////////////
    // PHASE 4: CLEANUP + SUMMARY
    /////////////////////////////

    // Remove checkpoint files — scan is complete
    if (fs.existsSync(CHECKPOINT_FILE)) fs.unlinkSync(CHECKPOINT_FILE);
    if (fs.existsSync(ADDRESSES_FILE))  fs.unlinkSync(ADDRESSES_FILE);

    console.log(`\n--- Summary ---`);
    console.log(`Addresses queried : ${queried}`);
    console.log(`Holders (bal > 0) : ${holders.length}`);
    console.log(`Failed (all retries exhausted) : ${failed.length}`);
    if (failed.length > 0) {
        console.log(`Failed addresses saved to: failed.txt`);
        fs.writeFileSync(path.resolve(__dirname, "failed.txt"), failed.join("\n") + "\n");
    }
    console.log(`CSV saved: ${OUTPUT_FILE}`);
}

main().catch(err => {
    console.error("\nFatal error:", err.message);
    console.error("Progress has been saved. Re-run the script to resume.");
    process.exit(1);
});
