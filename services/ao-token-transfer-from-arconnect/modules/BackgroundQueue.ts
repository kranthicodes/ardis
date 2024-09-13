import { EventEmitter } from 'events';

class BackgroundQueue extends EventEmitter {
  private queue: (() => Promise<void>)[] = [];
  private isProcessing: boolean = false;

  constructor() {
    super();
    this.startProcessing();
  }

  public enqueue(task: () => Promise<void>): void {
    this.queue.push(task);
    this.emit('taskAdded');
  }

  private async startProcessing(): Promise<void> {
    while (true) {
      if (this.queue.length > 0 && !this.isProcessing) {
        await this.processQueue();
      } else {
        await this.sleep(10000); // Sleep for 10 seconds
      }
    }
  }

  private async processQueue(): Promise<void> {
    this.isProcessing = true;

    while (this.queue.length > 0) {
      const task = this.queue.shift();
      if (task) {
        try {
          await task();
        } catch (error) {
          console.error('Error processing task:', error);
        }
      }
    }

    this.isProcessing = false;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

export const backgroundQueue = new BackgroundQueue();