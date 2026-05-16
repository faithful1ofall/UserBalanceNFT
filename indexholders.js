const fs = require("fs");
const path = require("path");
const { ethers } = require("ethers");

/////////////////////////////
// CONFIG
/////////////////////////////

const RPC_URL = "https://arc-testnet.g.alchemy.com/v2/o1k50yOLGXHrczBA8KDOf";

const NFT_CONTRACT = "0x9e05c6075f9e890fc515ef86091414c77036f8fa";

const NFT_CREATION_BLOCK = 9435462;

// Keep batches small to avoid RPC range limits
const BLOCK_BATCH = 2000;

// Concurrent ownerOf calls
const CONCURRENCY = 10;

const OUTPUT_FILE = path.resolve(__dirname, "holders.csv");

const TRANSFER_TOPIC = ethers.id("Transfer(address,address,uint256)");
const ZERO_ADDR = "0x0000000000000000000000000000000000000000";

/////////////////////////////
// PROVIDER + CONTRACT
/////////////////////////////

const provider = new ethers.JsonRpcProvider(RPC_URL);

const contract = new ethers.Contract(
    NFT_CONTRACT,
    [
        "function ownerOf(uint256 tokenId) view returns (address)",
        "function totalSupply() view returns (uint256)"
    ],
    provider
);

/////////////////////////////
// SAFE LOG FETCH (auto-splits on RPC range errors)
/////////////////////////////

async function getLogs(fromBlock, toBlock) {
    try {
        return await provider.getLogs({
            address: NFT_CONTRACT,
            fromBlock,
            toBlock,
            topics: [TRANSFER_TOPIC]
        });
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
// MAIN
/////////////////////////////

async function main() {

    const latestBlock = process.argv[2]
        ? Number(process.argv[2])
        : await provider.getBlockNumber();

    console.log(`Scanning blocks ${NFT_CREATION_BLOCK} → ${latestBlock}`);

    /////////////////////////////
    // 1. COLLECT ALL MINTED TOKEN IDs
    //    Only mint events (from == 0x0) are used to build the token list.
    //    This guarantees we never miss a token regardless of transfer history.
    /////////////////////////////

    const mintedTokenIds = new Set();

    for (
        let from = NFT_CREATION_BLOCK;
        from <= latestBlock;
        from += BLOCK_BATCH
    ) {
        const to = Math.min(from + BLOCK_BATCH - 1, latestBlock);
        console.log(`Fetching logs ${from} → ${to}`);

        const logs = await getLogs(from, to);

        for (const log of logs) {
            const fromAddr = "0x" + log.topics[1].slice(26).toLowerCase();

            // Only mint events
            if (fromAddr === ZERO_ADDR) {
                const tokenId = BigInt(log.topics[3]);
                mintedTokenIds.add(tokenId);
            }
        }
    }

    console.log(`Total minted tokens found: ${mintedTokenIds.size}`);

    /////////////////////////////
    // 2. RESOLVE CURRENT OWNER FOR EACH TOKEN via ownerOf()
    //    ownerOf() is the ground truth — it matches what block explorers show.
    //    balanceOf() is derived from this, so we tally from ownerOf results.
    /////////////////////////////

    const balanceMap = new Map(); // address → count (BigInt)

    const tokenList = Array.from(mintedTokenIds);
    let index = 0;
    let resolved = 0;
    let burned = 0;

    async function worker() {
        while (index < tokenList.length) {
            const tokenId = tokenList[index++];

            try {
                const owner = (await contract.ownerOf(tokenId)).toLowerCase();

                // ownerOf reverts for burned tokens; if it returns 0x0 treat as burned
                if (owner === ZERO_ADDR) {
                    burned++;
                    continue;
                }

                balanceMap.set(owner, (balanceMap.get(owner) ?? 0n) + 1n);
                resolved++;

            } catch {
                // Token was burned (ownerOf reverts) — skip it
                burned++;
            }
        }
    }

    await Promise.all(
        Array.from({ length: CONCURRENCY }, () => worker())
    );

    console.log(`Resolved: ${resolved} tokens | Burned/invalid: ${burned}`);

    /////////////////////////////
    // 3. SORT: highest balance first, then alphabetical by address
    /////////////////////////////

    const holders = Array.from(balanceMap.entries());

    holders.sort(([addrA, balA], [addrB, balB]) => {
        if (balB !== balA) return Number(balB - balA);
        return addrA.localeCompare(addrB);
    });

    /////////////////////////////
    // 4. SAVE CSV
    /////////////////////////////

    const csv = ["address,balance", ...holders.map(([addr, bal]) => `${addr},${bal}`)];

    fs.writeFileSync(OUTPUT_FILE, csv.join("\n") + "\n");

    console.log(`Saved: ${OUTPUT_FILE}`);
    console.log(`Total holders: ${holders.length}`);
}

main().catch(console.error);
