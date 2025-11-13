'use client';

import React from 'react';
import { useRouter } from 'next/navigation';

const PassPort: React.FC = () => {
  const router = useRouter();
  router.push('/dashboard');
  return <></>;
};

export default PassPort;
