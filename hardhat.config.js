require("@nomicfoundation/hardhat-toolbox");
require("@nomiclabs/hardhat-solpp");
require('dotenv').config();
require("./scripts/deploy");

const config = {
    ZKSYNC_ADDRESS: process.env.CONTRACTS_DIAMOND_PROXY_ADDR,
    ERC20_BRIDGE_ADDRESS: process.env.CONTRACTS_L1_ERC20_BRIDGE_PROXY_ADDR
};

/** @type import('hardhat/config').HardhatUserConfig */
module.exports = {
    solidity: "0.8.19",
    solpp: {
        defs: config
    },
    paths: {
        cache: "contracts/cache",
        artifacts: "contracts/artifacts",
    },
    networks: {
        lineatest: {
            url: process.env.ETH_CLIENT_HTTP_URL,
            accounts: [process.env.WITHDRAWAL_FINALIZER_ACCOUNT_PRIVATE_KEY]
        }
    },
    etherscan: {
        apiKey: {
            lineatest: process.env.MISC_ETHERSCAN_API_KEY
        },
        customChains: [
            {
                network: "lineatest",
                chainId: 59140,
                urls: {
                    apiURL: "https://api-testnet.lineascan.build/api",
                    browserURL: "https://goerli.lineascan.build"
                }
            }
        ]
    }
};
