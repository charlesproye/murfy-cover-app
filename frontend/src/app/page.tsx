'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';

export default function Home(): void {
  const router = useRouter();

  useEffect(() => {
    router.push('/dashboard');
  }, [router]);
}
