'use client';

import React, { useState, useMemo } from 'react';
import { useParams, useRouter } from 'next/navigation';
import { toast } from 'sonner';
import useSWR from 'swr';
import { IconDownload } from '@tabler/icons-react';

import { ROUTES } from '@/routes';
import { Loading } from '@/components/common/loading/loading';
import fetchWithAuth from '@/services/fetchWithAuth';
import useGetReportHtml from '@/hooks/dashboard/passport/useGetReportHtml';
import useGetInfoVehicle from '@/hooks/dashboard/passport/useGetInfoVehicle';
import BibRecommendation from '@/components/entities/dashboard/passport/report/BibRecommendation';
import PinVehicleSwitch from '@/components/entities/dashboard/passport/datacard/PinVehicleSwitch';
import useGetPriceForecast from '@/hooks/dashboard/passport/useGetPriceForecast';
import { formatNumber } from '@/lib/dataDisplay';

const PassPort = () => {
  const router = useRouter();
  const params = useParams();
  const vin = params.vin as string;
  const [reportType, setReportType] = useState<'premium' | 'readout'>('premium');
  const [isPdfLoading, setIsPdfLoading] = useState(false);

  const {
    data: vinCheck,
    isLoading: isCheckingVin,
    error: vinError,
  } = useSWR(
    `${ROUTES.IS_VIN_IN_FLEETS}/${vin}`,
    fetchWithAuth<{ is_in_fleets: boolean }>,
  );

  const {
    htmlContent,
    isLoading: isLoadingReport,
    error: reportError,
  } = useGetReportHtml(vin, reportType);
  const { data: infoVehicle, isLoading: isLoadingInfo } = useGetInfoVehicle(vin);
  const { data: priceForecast } = useGetPriceForecast(vin);

  // Check VIN access
  React.useEffect(() => {
    if (!isCheckingVin && !vinError && vinCheck?.is_in_fleets === false) {
      toast.error('This vehicle is not in your fleets', {
        description:
          'Please contact your administrator to add this vehicle to your fleets',
      });
      return router.push('/dashboard');
    }
  }, [isCheckingVin, vinError, vinCheck?.is_in_fleets, router]);

  const handleDownloadPdf = async () => {
    if (!vin) return;

    setIsPdfLoading(true);
    try {
      const baseRoute =
        reportType === 'readout'
          ? ROUTES.READOUT_REPORT_HTML
          : ROUTES.PREMIUM_REPORT_PDF_SYNC;
      const response = await fetchWithAuth<{ vin: string; url: string; message: string }>(
        `${baseRoute}/${vin}/generate_report_sync`,
        { method: 'POST' },
      );

      if (
        !response?.url ||
        typeof response.url !== 'string' ||
        response.url.trim() === ''
      ) {
        throw new Error('Invalid PDF URL received from server');
      }

      window.open(response.url, '_blank');
      toast.success('Report downloaded successfully');
    } catch (error) {
      console.error('Error downloading PDF:', error);
      toast.error('Failed to generate PDF. Please try again.');
    } finally {
      setIsPdfLoading(false);
    }
  };

  const scaledHtmlContent = useMemo(() => {
    if (!htmlContent) return null;
    const script = `
      <script>
        function fitToWidth() {
          const REPORT_WIDTH = 794;
          const REPORT_HEIGHT = 1123;
          const scale = window.innerWidth / REPORT_WIDTH;
          document.body.style.transform = 'scale(' + scale + ')';
          document.body.style.transformOrigin = 'top left';
          document.body.style.width = REPORT_WIDTH + 'px';
          document.body.style.height = REPORT_HEIGHT + 'px';
          document.body.style.overflowX = 'hidden';
          document.body.style.overflowY = 'hidden';
        }
        window.addEventListener('resize', fitToWidth);
        document.addEventListener('DOMContentLoaded', fitToWidth);
        fitToWidth();
      </script>
    `;
    return htmlContent + script;
  }, [htmlContent]);

  if (isCheckingVin || isLoadingInfo) {
    return <Loading />;
  }

  if (vinError) {
    toast.error('Error fetching vehicle data', {
      description: 'Please try again later',
    });
    return router.push('/dashboard');
  }

  return (
    <div className="flex flex-col h-full w-full space-y-6 pt-2 pb-16">
      {/* Header with Switch and Download Button */}
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <div className="flex items-center bg-white rounded-full p-1 shadow-md border border-gray-200">
            <button
              onClick={() => setReportType('premium')}
              className={`px-4 py-2 text-sm font-medium rounded-full transition-all ${
                reportType === 'premium'
                  ? 'bg-primary text-white shadow-sm'
                  : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Premium
            </button>
            <button
              onClick={() => setReportType('readout')}
              className={`px-4 py-2 text-sm font-medium rounded-full transition-all ${
                reportType === 'readout'
                  ? 'bg-primary text-white shadow-sm'
                  : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Readout
            </button>
          </div>
        </div>

        <button
          onClick={handleDownloadPdf}
          disabled={isPdfLoading}
          className={`bg-primary text-white shadow-md rounded-full px-6 py-3 text-sm flex items-center gap-2 font-semibold hover:bg-primary/90 transition-colors ${
            isPdfLoading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'
          }`}
        >
          <IconDownload className={`w-5 h-5 ${isPdfLoading ? 'animate-spin' : ''}`} />
          Download Report
        </button>
      </div>

      {/* Report Preview */}
      <div
        className="
            flex justify-center items-start
            bg-white
            rounded-[20px]
            shadow-lg
            center
            overflow-hidden
            w-auto
          "
        style={{ aspectRatio: '794 / 1123' }}
      >
        <div className="w-full h-full relative">
          {isLoadingReport && (
            <div className="flex items-center justify-center h-full">
              <div className="w-12 h-12 border-4 border-primary border-t-transparent rounded-full animate-spin" />
            </div>
          )}

          {!!reportError && !isLoadingReport && !htmlContent && (
            <div className="flex items-center justify-center h-full text-red-500">
              <p>{reportError.detail || 'Failed to load report. Please try again.'}</p>
            </div>
          )}

          {scaledHtmlContent && (
            <iframe
              srcDoc={scaledHtmlContent}
              className="w-full h-full border-0 overflow-hidden"
              title={`${reportType === 'premium' ? 'Premium' : 'Readout'} Report`}
              sandbox="allow-scripts allow-same-origin"
            />
          )}
        </div>
      </div>

      {/* Channelling Recommendation Section */}
      {infoVehicle && (
        <div className="w-full bg-white rounded-[20px] shadow-lg p-6">
          <div className="flex items-center gap-3 mb-6">
            <div className="bg-blue-100 p-2 rounded-xl">
              <svg
                className="w-6 h-6 text-primary"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                />
              </svg>
            </div>
            <h2 className="text-xl font-semibold text-gray-900">
              Channelling Recommendation
            </h2>
          </div>

          <div className="space-y-4">
            <BibRecommendation score={infoVehicle.vehicle_info.score} />
            <PinVehicleSwitch vin={vin} />

            {priceForecast !== undefined &&
              priceForecast.price !== null &&
              priceForecast.price_discount !== null && (
                <>
                  <div className="w-full border-t border-gray-200 my-4" />

                  <div className="grid grid-cols-2 gap-4">
                    <div className="flex flex-col gap-1">
                      <span className="text-sm text-gray-600">Advertised Price</span>
                      <span className="text-lg font-semibold text-gray-900">
                        {formatNumber(priceForecast.price.toFixed(0))} €
                      </span>
                    </div>

                    <div className="flex flex-col gap-1">
                      <span className="text-sm text-gray-600">SoH Adjustment</span>
                      <span
                        className={`text-lg font-semibold ${
                          priceForecast.price_discount > 0
                            ? 'text-green-600'
                            : 'text-red-600'
                        }`}
                      >
                        {priceForecast.price_discount > 0 ? '+ ' : '- '}
                        {formatNumber(
                          Math.abs(priceForecast.price_discount).toFixed(0),
                        )}{' '}
                        €
                      </span>
                    </div>
                  </div>
                </>
              )}
          </div>
        </div>
      )}
    </div>
  );
};

export default PassPort;
