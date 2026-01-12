'use client';

import { Loading } from '@/components/common/loading/loading';
import DashboardLayout from '@/components/layout/DashboardLayout';
import useAuth from '@/contexts/AuthContext';
import { useRouter } from 'next/navigation';
import React from 'react';

export default function Layout({
  children,
}: {
  children: React.ReactNode;
}): React.ReactElement {
  const { isAuthenticated, isLoading } = useAuth();
  const router = useRouter();

  if (isLoading) {
    return <Loading />;
  }

  if (!isAuthenticated) {
    router.push('/auth/login');
    return <></>;
  }

  return <DashboardLayout>{children}</DashboardLayout>;
}
