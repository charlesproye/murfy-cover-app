/* eslint-disable @typescript-eslint/no-explicit-any */

/**
 * Secure storage utilities using encrypted cookies and session storage
 * Provides a secure alternative to localStorage
 */

import { encryptData, decryptData, hashData } from '@/services/auth/crypto';

// Cookie utilities
function setCookie(name: string, value: string, days: number = 7): void {
  const expires = new Date();
  expires.setTime(expires.getTime() + days * 24 * 60 * 60 * 1000);

  document.cookie = `${name}=${value}; expires=${expires.toUTCString()}; path=/; secure; samesite=strict`;
}

function getCookie(name: string): string | null {
  const nameEQ = name + '=';
  const ca = document.cookie.split(';');

  for (let i = 0; i < ca.length; i++) {
    let c = ca[i];
    while (c.charAt(0) === ' ') c = c.substring(1, c.length);
    if (c.indexOf(nameEQ) === 0) return c.substring(nameEQ.length, c.length);
  }
  return null;
}

function deleteCookie(name: string): void {
  document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;`;
}

// Secure storage interface
export interface SecureStorage {
  setItem(key: string, value: any): Promise<void>;
  getItem(key: string): Promise<any>;
  removeItem(key: string): void;
  clear(): void;
}

// Encrypted cookie storage
export class EncryptedCookieStorage implements SecureStorage {
  private cookieName: string;
  private maxAge: number;

  constructor(cookieName: string = 'evalue_secure_data', maxAge: number = 7) {
    this.cookieName = cookieName;
    this.maxAge = maxAge;
  }

  async setItem(key: string, value: any): Promise<void> {
    try {
      const data = JSON.stringify(value);
      const encrypted = await encryptData(data);
      const hash = await hashData(data);

      // Store encrypted data and hash separately
      setCookie(`${this.cookieName}_${key}`, encrypted, this.maxAge);
      setCookie(`${this.cookieName}_${key}_hash`, hash, this.maxAge);
    } catch (error) {
      console.error('Failed to store encrypted data:', error);
      throw new Error('Failed to store data securely');
    }
  }

  async getItem(key: string): Promise<any> {
    try {
      const encrypted = getCookie(`${this.cookieName}_${key}`);
      const storedHash = getCookie(`${this.cookieName}_${key}_hash`);

      if (!encrypted || !storedHash) {
        return null;
      }

      const decrypted = await decryptData(encrypted);
      const currentHash = await hashData(decrypted);

      // Verify integrity
      if (currentHash !== storedHash) {
        console.error('Data integrity check failed');
        this.removeItem(key);
        return null;
      }

      return JSON.parse(decrypted);
    } catch (error) {
      console.error('Failed to retrieve encrypted data:', error);
      this.removeItem(key);
      return null;
    }
  }

  removeItem(key: string): void {
    deleteCookie(`${this.cookieName}_${key}`);
    deleteCookie(`${this.cookieName}_${key}_hash`);
  }

  clear(): void {
    // Clear all cookies that start with our prefix
    const cookies = document.cookie.split(';');
    cookies.forEach((cookie) => {
      const eqPos = cookie.indexOf('=');
      const name = eqPos > -1 ? cookie.substr(0, eqPos).trim() : cookie.trim();
      if (name.startsWith(this.cookieName)) {
        deleteCookie(name);
      }
    });
  }
}

// Session storage with encryption (for temporary data)
export class EncryptedSessionStorage implements SecureStorage {
  async setItem(key: string, value: any): Promise<void> {
    try {
      const data = JSON.stringify(value);
      const encrypted = await encryptData(data);
      sessionStorage.setItem(`evalue_encrypted_${key}`, encrypted);
    } catch (error) {
      console.error('Failed to store encrypted session data:', error);
      throw new Error('Failed to store session data securely');
    }
  }

  async getItem(key: string): Promise<any> {
    try {
      const encrypted = sessionStorage.getItem(`evalue_encrypted_${key}`);
      if (!encrypted) return null;

      const decrypted = await decryptData(encrypted);
      return JSON.parse(decrypted);
    } catch (error) {
      console.error('Failed to retrieve encrypted session data:', error);
      sessionStorage.removeItem(`evalue_encrypted_${key}`);
      return null;
    }
  }

  removeItem(key: string): void {
    sessionStorage.removeItem(`evalue_encrypted_${key}`);
  }

  clear(): void {
    const keys = Object.keys(sessionStorage);
    keys.forEach((key) => {
      if (key.startsWith('evalue_encrypted_')) {
        sessionStorage.removeItem(key);
      }
    });
  }
}

// Create instances
export const secureStorage = new EncryptedCookieStorage();
export const secureSessionStorage = new EncryptedSessionStorage();

// Utility functions for backward compatibility
export const secureLocalStorage = {
  async setItem(key: string, value: any): Promise<void> {
    return secureStorage.setItem(key, value);
  },

  async getItem(key: string): Promise<any> {
    return secureStorage.getItem(key);
  },

  removeItem(key: string): void {
    secureStorage.removeItem(key);
  },

  clear(): void {
    secureStorage.clear();
  },
};
