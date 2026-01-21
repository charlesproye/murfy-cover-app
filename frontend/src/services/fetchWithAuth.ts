/**
 * Fetch utility that uses httpOnly cookies for authentication
 * Provides automatic token refresh and secure request handling
 */

import { logoutRequest } from '@/services/auth/authService';
import { attemptRefresh, getIsLoggingOut } from '@/services/auth/authState';
import { toast } from 'sonner';

export interface FetchOptions extends RequestInit {
  headers?: Record<string, string>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  body?: any;
}

/**
 * Fetch with automatic authentication handling
 * Uses httpOnly cookies instead of Authorization headers
 */
async function fetchWithAuth<T>(url: string, options: FetchOptions = {}): Promise<T> {
  const { body, ...restOptions } = options;
  const defaultOptions: RequestInit = {
    credentials: 'include', // Include httpOnly cookies
    headers: {
      'Content-Type': 'application/json',
      ...restOptions.headers,
    },
  };

  // Handle body serialization
  if (body && !(body instanceof FormData)) {
    if (typeof body === 'object') {
      defaultOptions.body = JSON.stringify(body);
    } else {
      defaultOptions.body = body;
    }
  } else if (body instanceof FormData) {
    // Remove Content-Type header for FormData
    delete (defaultOptions.headers as Record<string, string>)['Content-Type'];
    defaultOptions.body = body;
  }

  const makeRequest = async (): Promise<Response> => {
    return fetch(url, { ...defaultOptions, ...restOptions });
  };

  try {
    let response = await makeRequest();

    // Handle token refresh on 401/403
    if (response.status === 401 || response.status === 403) {
      // Use shared refresh coordination to prevent multiple simultaneous attempts
      const refreshResponse = await attemptRefresh();

      if (refreshResponse.ok) {
        // Retry the original request
        response = await makeRequest();
      } else {
        // Only logout once, even if multiple requests fail
        if (!getIsLoggingOut()) {
          await logoutRequest();
          console.error('Authentication failed - please login again');
          toast.error('Authentication failed - please login again');
        }
        // Throw error to prevent further processing
        throw new Error('Authentication failed - please login again');
      }
    }

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const contentType = response.headers.get('content-type');
    if (contentType?.includes('application/json')) {
      return response.json();
    }

    // Handle text responses
    const text = await response.text();
    return text as unknown as T;
  } catch (error) {
    if (error instanceof Error) {
      throw error;
    }
    throw new Error('An unexpected error occurred');
  }
}

export default fetchWithAuth;
