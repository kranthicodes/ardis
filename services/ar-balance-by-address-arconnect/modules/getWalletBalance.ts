export function getWalletBalance(ownerAddress: string) {
  return fetch(`https://arweave.net/wallet/${ownerAddress}/balance`)
    .then((response) => {
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      return response.text();
    })
    .then((balance: string) => {
      const arBalance = Number(balance) / 1000000000000;
      const winBalance = Number(balance);
      if (isNaN(arBalance) || isNaN(winBalance)) {
        throw new Error("Invalid balance received");
      }

      return { arBalance, winBalance };
    })
    .catch((error) => {
      throw error;
    });
}
