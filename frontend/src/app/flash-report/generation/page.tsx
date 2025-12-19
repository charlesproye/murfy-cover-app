'use client';

import React from 'react';
import { useSearchParams } from 'next/navigation';
import Link from 'next/link';
import { FlashReportGeneration } from '@/components/entities/flash-report/FlashReportGeneration';

const FlashReportGenerationPage = (): React.ReactElement => {
  const searchParams = useSearchParams();
  const token = searchParams.get('token');

  if (!token) {
    return (
      <p className="italic mt-10">
        Link is incorrect, please try again or{' '}
        <Link
          href="mailto:support@bib-batteries.fr"
          className="text-primary hover:underline"
        >
          contact support
        </Link>
        .
      </p>
    );
  }

  return <FlashReportGeneration token={token} />;
};

export default FlashReportGenerationPage;
