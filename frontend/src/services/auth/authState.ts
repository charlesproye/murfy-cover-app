/**
 * Shared authentication state manager
 * Coordinates refresh attempts and logout calls across the application
 * Prevents multiple simultaneous refresh attempts and duplicate logout calls
 */

import { ROUTES } from '@/routes';

// Shared state for refresh coordination
let refreshPromise: Promise<Response> | null = null;
let isLoggingOut = false;
let checkAuthPromise: Promise<void> | null = null;

/**
 * Attempt to refresh the authentication token
 * Returns the same promise if a refresh is already in progress
 */
export const attemptRefresh = async (): Promise<Response> => {
  if (!refreshPromise) {
    refreshPromise = fetch(ROUTES.REFRESH, {
      method: 'POST',
      credentials: 'include',
    }).finally(() => {
      // Clear the promise after it completes (success or failure)
      refreshPromise = null;
    });
  }
  return refreshPromise;
};

/**
 * Check if a refresh is currently in progress
 */
export const isRefreshing = (): boolean => {
  return refreshPromise !== null;
};

/**
 * Check if logout is in progress
 */
export const getIsLoggingOut = (): boolean => {
  return isLoggingOut;
};

/**
 * Set the logout state (should only be called by logout functions)
 */
export const setIsLoggingOut = (value: boolean): void => {
  isLoggingOut = value;
};

/**
 * Coordinate checkAuth calls to prevent multiple simultaneous executions
 */
export const coordinateCheckAuth = async (
  checkAuthFn: () => Promise<void>,
): Promise<void> => {
  if (checkAuthPromise) {
    return checkAuthPromise;
  }

  checkAuthPromise = checkAuthFn().finally(() => {
    checkAuthPromise = null;
  });

  return checkAuthPromise;
};
