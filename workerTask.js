import { parentPort, workerData } from 'worker_threads';
import { 
  fetchAndStoreContractsByCode, 
  fetchAndStoreContractHistory, 
  fetchAndStoreTokenOwners 
} from './cw721Helper.js';

(async () => {
  try {
    const { restAddress, db, contractsToProcess, taskType } = workerData;
    
    if (taskType === 'fetchContractsByCode') {
      await fetchAndStoreContractsByCode(restAddress, db, contractsToProcess);
    } else if (taskType === 'fetchContractHistory') {
      await fetchAndStoreContractHistory(restAddress, db, contractsToProcess);
    } else if (taskType === 'fetchTokenOwners') {
      await fetchAndStoreTokenOwners(restAddress, db, contractsToProcess);
    }

    // Notify the main thread when this worker is done
    parentPort.postMessage({ status: 'done' });
  } catch (error) {
    parentPort.postMessage({ status: 'error', message: error.message });
  }
})();
