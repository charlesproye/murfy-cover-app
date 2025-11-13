'use client';

import { FlashReportForm } from '@/components/flash-report/forms/FlashReportForm';
import React from 'react';

import { useSearchParams, useRouter } from 'next/navigation';

const FlashReportStep1Page = () => {
  const searchParams = useSearchParams();
  const router = useRouter();

  const has_trendline = searchParams.get('has_trendline');
  const make = searchParams.get('make');
  const model = searchParams.get('model');
  const type = searchParams.get('type');
  const version = searchParams.get('version');
  const vin = searchParams.get('vin');

  if (!vin) {
    router.push('/flash-report');
    return null;
  }

  return (
    <div className="flex flex-col gap-8 w-3/4 mx-auto mt-8">
      <FlashReportForm
        vin={vin}
        has_trendline={has_trendline === 'true'}
        make={make}
        model={model}
        type={type}
        version={version}
      />
    </div>
  );
};

export default FlashReportStep1Page;
