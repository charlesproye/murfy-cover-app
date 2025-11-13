'use client';

import { ROUTES } from '@/routes';
import { fetchWithoutAuth } from '@/services/fetchWithoutAuth';
import { PDFDownloadLink } from '@react-pdf/renderer';

import useSWR from 'swr';
import { FlashReportPdf } from '../pdf/flash-report/FlashReportPdf';
import { IconDownload } from '@tabler/icons-react';
import { GetGenerationInfo } from '@/interfaces/flash-reports';
import LoadingSmall from '../common/loading/loadingSmall';
import Link from 'next/link';

interface FlashReportGenerationProps {
  token: string;
}

export const FlashReportGeneration = ({ token }: FlashReportGenerationProps) => {
  const { data: infoVehicle, isLoading } = useSWR(
    `${ROUTES.GENERATION_DATA}?token=${token}`,
    fetchWithoutAuth<GetGenerationInfo>,
  );

  if (isLoading) {
    return <LoadingSmall />;
  }

  if (!isLoading && !infoVehicle) {
    return (
      <>
        <p className="italic mt-10">
          Cannot get vehicle information, the url link may be incorrect.
        </p>
        <p className="italic">
          If the problem persists,{' '}
          <Link
            href="mailto:support@bib-batteries.fr"
            className="text-primary hover:underline"
          >
            contact support
          </Link>
          .
        </p>
      </>
    );
  }

  return (
    <div className="flex justify-center w-full mt-10">
      <PDFDownloadLink
        document={<FlashReportPdf data={infoVehicle as GetGenerationInfo} />}
        fileName="Battery_Report.pdf"
      >
        {({ loading }) => (
          <div
            className={`bg-white border border-primary/30 shadow-md rounded-full px-4 py-2 text-xs flex items-center gap-2 font-bold text-primary hover:bg-primary/10 transition-colors w-auto min-w-[120px] justify-center ${
              loading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'
            }`}
          >
            <IconDownload className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            Download Report
          </div>
        )}
      </PDFDownloadLink>
    </div>
  );
};
