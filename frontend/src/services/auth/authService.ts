/**
 * Authentication service using encrypted cookies and session storage
 * Provides a secure alternative to localStorage-based authentication
 */

import { ROUTES } from '../../routes';
import { secureStorage, secureSessionStorage } from '@/services/auth/secureStorage';
import { AuthResponse } from '@/interfaces/auth/auth';

/**
 * Login request that uses httpOnly cookies
 */
export const loginRequest = async (
  email: string,
  password: string,
): Promise<AuthResponse> => {
  const response: Response = await fetch(ROUTES.LOGIN, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    credentials: 'include', // Important: include cookies
    body: JSON.stringify({ email, password }),
  });

  if (response.status !== 200) {
    throw new Error(response.statusText);
  }

  const data: AuthResponse = await response.json();

  // Store non-sensitive user data in encrypted session storage
  await secureSessionStorage.setItem('user_data', {
    user: data.user,
    company: data.company,
  });

  return data;
};

/**
 * Logout request that clears httpOnly cookies
 */
export const logoutRequest = async (): Promise<void> => {
  try {
    await fetch(ROUTES.LOGOUT, {
      method: 'POST',
      credentials: 'include',
    });
  } catch (error) {
    console.error('Logout request failed:', error);
  } finally {
    // Clear local storage regardless of server response
    secureSessionStorage.clear();
    secureStorage.clear();
    window.location.href = '/auth/login';
  }
};

/**
 * Get current user data from secure storage
 */
export const getCurrentUserData = async (): Promise<AuthResponse | null> => {
  try {
    return await secureSessionStorage.getItem('user_data');
  } catch (error) {
    console.error('Failed to get user data:', error);
    return null;
  }
};

/**
 * Refresh token using httpOnly cookies
 */
export const refreshToken = async (): Promise<boolean> => {
  try {
    const response = await fetch(ROUTES.REFRESH, {
      method: 'POST',
      credentials: 'include', // Include httpOnly cookies
    });

    return response.ok;
  } catch (error) {
    console.error('Token refresh failed:', error);
    return false;
  }
};

/**
 * Check if user is authenticated by verifying session data
 */
export const isAuthenticated = async (): Promise<boolean> => {
  try {
    const userData = await getCurrentUserData();
    return userData !== null;
  } catch (error) {
    console.error('Authentication check failed:', error);
    return false;
  }
};

/**
 * Get session data from cookies (non-sensitive data only)
 */
export const getSessionData = (): AuthResponse | null => {
  try {
    // This reads the non-httpOnly session cookie
    const sessionCookie = document.cookie
      .split('; ')
      .find((row) => row.startsWith('evalue_session_data='));

    if (!sessionCookie) {
      return null;
    }

    const sessionToken = sessionCookie.split('=')[1];

    // Decode JWT payload (without verification since it's just session data)
    const payload = JSON.parse(atob(sessionToken.split('.')[1]));
    return payload;
  } catch (error) {
    console.error('Failed to get session data:', error);
    return null;
  }
};
