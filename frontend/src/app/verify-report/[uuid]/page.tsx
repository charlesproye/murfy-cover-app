'use client';

import React from 'react';
import { useParams } from 'next/navigation';
import Link from 'next/link';
import { ReportVerification } from '@/components/entities/verify-report/ReportVerification';
import Logo from '@/components/common/Logo';
import LandingFooter from '@/components/common/LandingFooter';

export default function VerifyReportPage() {
  const params = useParams();
  const uuid = params.uuid as string;

  if (!uuid) {
    return (
      <div className="min-h-screen flex items-center justify-center p-8">
        <p className="text-gray-500">
          Invalid link.{' '}
          <Link
            href="mailto:support@bib-batteries.fr"
            className="text-primary hover:underline"
          >
            Contact support
          </Link>
        </p>
      </div>
    );
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-8">
      <div className="flex flex-col gap-4 w-full max-w-md">
        <Logo title="Report Verification" />
        <ReportVerification reportUuid={uuid} />
        <LandingFooter className="mt-2" />
      </div>
    </div>
  );
}
