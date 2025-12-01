'use client';

import { FlashReportForm } from '@/components/flash-report/forms/FlashReportForm';
import React, { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import type { VinDecoderResponse } from '@/components/flash-report/forms/VinForm';
import { Loading } from '@/components/common/loading/loading';

interface FlashReportData extends VinDecoderResponse {
  vin: string;
}

const FlashReportStep1Page = () => {
  const router = useRouter();
  const [flashReportData, setFlashReportData] = useState<FlashReportData | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    try {
      const storedData = localStorage.getItem('flash-report-data');
      if (!storedData) {
        router.push('/flash-report');
        return;
      }

      const parsedData: FlashReportData = JSON.parse(storedData);
      if (!parsedData.vin) {
        router.push('/flash-report');
        return;
      }

      setFlashReportData(parsedData);
    } catch (error) {
      console.error('Error parsing flash report data from localStorage:', error);
      localStorage.removeItem('flash-report-data');
      router.push('/flash-report');
    } finally {
      setIsLoading(false);
    }
  }, [router]);

  if (isLoading) {
    return <Loading />;
  }

  if (!flashReportData) {
    return null;
  }

  return (
    <div className="flex flex-col gap-8 w-5/6 mx-auto mt-6">
      <FlashReportForm
        vin={flashReportData.vin}
        has_trendline={flashReportData.has_trendline}
        make={flashReportData.make ?? undefined}
        model={flashReportData.model ?? undefined}
        type_version_list={flashReportData.type_version_list}
      />
    </div>
  );
};

export default FlashReportStep1Page;
