import type { Metadata } from 'next';
import './globals.css';
import React from 'react';
import { Suspense } from 'react';
import { Loading } from '@/components/common/loading/loading';
import { AuthProvider } from '@/contexts/AuthContext';

export const metadata: Metadata = {
  title: 'Bib batteries',
  description:
    'An amazing tool to monitor your fleet, manage your contracts and your finances. The future of your fleet is here.',
  icons: {
    icon: '/logo/icon.svg',
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>): React.ReactElement {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="bg-[#F7F6F9]" suppressHydrationWarning>
        <Suspense fallback={<Loading />}>
          <AuthProvider>{children}</AuthProvider>
        </Suspense>
      </body>
    </html>
  );
}
