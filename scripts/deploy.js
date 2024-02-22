const { task } = require('hardhat/config');

task("deploy", "Deploy the WithdrawalFinalizer contract", async (taskArgs, hre) => {
  console.log("Deploying WithdrawalFinalizer...");
  console.log("Deploying to network: ", hre.network.name);
  console.log("deployer: ", hre.network.config.accounts[0]);
  
  const finalizer = await hre.ethers.deployContract("WithdrawalFinalizer", []);
  await finalizer.waitForDeployment();
  console.log(`WithdrawalFinalizer deployed at ${finalizer.target}`);

  console.log("Verify WithdrawalFinalizer contract on Etherscan...");
  while (true) {
    try {
      await hre.run("verify:verify", {
        address: finalizer.target,
        constructorArguments: [],
      });
      console.log("WithdrawalFinalizer contract verified!");
      return;
    } catch (e) {
      if (
        e.message.includes('Already Verified') ||
        e.message.includes('already verified') ||
        e.message.includes('Contract source code already verified') ||
        e.message.includes('Smart-contract already verified')
      ) {
        console.log('WithdrawalFinalizer contract code already verified');
        return;
      } else {
        console.warn('Verify code failed: %s', e.message);
      }
    }
    await new Promise((r) => setTimeout(r, 60000));
  }
});
