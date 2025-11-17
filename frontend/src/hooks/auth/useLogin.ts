'use client';

import React, { FormEvent } from 'react';
import { useState } from 'react';
import { useRouter } from 'next/navigation';
import useAuth from '@/contexts/AuthContext';

const useLogin = (): {
  email: string;
  setEmail: (email: string) => void;
  password: string;
  setPassword: (password: string) => void;
  error: string;
  handleSubmit: (e: React.FormEvent<HTMLFormElement>) => void;
  isLoading: boolean;
} => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [isLoading, setIsLoading] = useState(false);

  const router = useRouter();
  const { login } = useAuth();

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setIsLoading(true);
    setError('');

    try {
      const success = await login(email, password);

      if (success) {
        router.push('/dashboard');
      } else {
        setError('Login failed. Please check your credentials.');
      }
    } catch (error) {
      setError(
        error instanceof Error
          ? error.message
          : 'Login failed. Please check your credentials.',
      );
    }

    setIsLoading(false);
  };

  return {
    email,
    setEmail,
    password,
    setPassword,
    error,
    handleSubmit,
    isLoading,
  };
};

export default useLogin;
