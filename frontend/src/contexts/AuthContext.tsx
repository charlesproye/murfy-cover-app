'use client';

/**
 * Authentication context using encrypted cookies and session storage
 * Provides secure authentication with localStorage compatibility
 */

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { usePathname, useRouter } from 'next/navigation';
import { Company, Fleet, User } from '@/interfaces/auth/auth';
import {
  loginRequest,
  getCurrentUserData,
  getSessionData,
  logoutRequest,
} from '@/services/auth/authService';
import { ROUTES } from '@/routes';
import { toast } from 'sonner';

interface AuthContextProps {
  isLoading: boolean;
  user: User | null;
  company: Company | null;
  fleet: Fleet | null;
  setFleet: (fleet: Fleet | null) => void;
  isAuthenticated: boolean;
  login: (email: string, password: string) => Promise<boolean>;
  handleLogout: () => Promise<void>;
  refetchSessionData: () => Promise<void>;
}

const AUTHORIZED_ROUTES = ['/auth/login', '/flash-report'];

const AuthContext = createContext<AuthContextProps | undefined>(undefined);

interface AuthProviderProps {
  children: ReactNode;
}

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const [isLoading, setIsLoading] = useState(true);
  const [user, setUser] = useState<User | null>(null);
  const [company, setCompany] = useState<Company | null>(null);
  const [isAuth, setIsAuth] = useState(false);
  const [fleet, setFleet] = useState<Fleet | null>(null);
  const router = useRouter();
  const pathname = usePathname();

  const setUserFromUserData = async () => {
    // Get full user data from secure storage
    const userData = await getCurrentUserData();
    if (userData) {
      setUser(userData.user);
      setCompany(userData.company);
      if (userData.user.fleets.length > 0) {
        setFleet(userData.user.fleets[0]);
      } else {
        setFleet(null);
      }
      setIsAuth(true);
    } else {
      await handleLogout();
    }
  };

  const checkAuth = async (): Promise<void> => {
    setIsLoading(true);

    try {
      // First check if we have session data from cookies
      const sessionData = getSessionData();

      if (sessionData) {
        await setUserFromUserData();
      } else {
        if (!AUTHORIZED_ROUTES.find((route) => pathname.includes(route))) {
          // if Login page, no refresh
          const refreshResponse = await fetch(ROUTES.REFRESH, {
            method: 'POST',
            credentials: 'include', // Include httpOnly cookies
          });

          if (refreshResponse.ok) {
            const sessionData = getSessionData();

            if (sessionData) {
              await setUserFromUserData();
            } else {
              handleLogout();
            }
          } else {
            // No user data, logout
            handleLogout();
          }
        }
      }
    } catch (error) {
      console.error('Auth check failed:', error);
      await handleLogout();
    } finally {
      setIsLoading(false);
    }
  };

  const login = async (email: string, password: string): Promise<boolean> => {
    try {
      setIsLoading(true);
      const response = await loginRequest(email, password);

      if (response && response.user && response.company) {
        setUser(response.user);
        setCompany(response.company);
        if (response.user.fleets.length > 0) {
          setFleet(response.user.fleets[0]);
        } else {
          setFleet(null);
        }
        setIsAuth(true);
        return true;
      }
      return false;
    } catch (error) {
      console.error('Login failed:', error);
      return false;
    } finally {
      setIsLoading(false);
    }
  };

  const refetchSessionData = async (): Promise<void> => {
    // if Login page, no refresh
    const refreshResponse = await fetch(ROUTES.REFRESH, {
      method: 'POST',
      credentials: 'include', // Include httpOnly cookies
    });

    if (refreshResponse.ok) {
      const sessionData = getSessionData();

      if (sessionData) {
        await setUserFromUserData();
      } else {
        handleLogout();
      }
    } else {
      // No user data, logout
      toast.error('Authentication failed');
      handleLogout();
    }
  };

  const handleLogout = async (): Promise<void> => {
    try {
      await logoutRequest();
    } catch (error) {
      console.error('Logout error:', error);
    } finally {
      setUser(null);
      setCompany(null);
      setIsAuth(false);
      setFleet(null);
      router.push('/auth/login');
    }
  };

  useEffect(() => {
    checkAuth();

    // Listen for storage changes (for multi-tab support)
    const handleStorageChange = () => {
      checkAuth();
    };

    window.addEventListener('storage', handleStorageChange);

    return () => {
      window.removeEventListener('storage', handleStorageChange);
    };
  }, []);

  const value: AuthContextProps = {
    isLoading,
    user,
    company,
    fleet,
    isAuthenticated: isAuth,
    login,
    setFleet,
    handleLogout,
    refetchSessionData,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export const useAuth = (): AuthContextProps => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};

export default useAuth;
