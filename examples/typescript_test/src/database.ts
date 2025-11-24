import { Client, ClientConfig } from 'pg';
import { dbConfig } from './config';

export class DatabaseClient {
  private client: Client;

  constructor() {
    this.client = new Client(dbConfig as ClientConfig);
  }

  async connect(): Promise<void> {
    try {
      await this.client.connect();
      console.log('Connected to database successfully');
    } catch (error) {
      console.error('Failed to connect to database:', error);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    try {
      await this.client.end();
      console.log('Disconnected from database');
    } catch (error) {
      console.error('Error disconnecting from database:', error);
      throw error;
    }
  }

  async query(text: string, params?: any[]): Promise<any> {
    try {
      const result = await this.client.query(text, params);
      return result;
    } catch (error) {
      console.error('Query error:', error);
      throw error;
    }
  }
}