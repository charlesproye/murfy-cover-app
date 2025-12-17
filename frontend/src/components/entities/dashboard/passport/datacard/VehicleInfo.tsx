'use client';

import React, { useState } from 'react';
import {
  IconCar,
  IconBattery,
  IconChartBar,
  IconDownload,
  IconEye,
} from '@tabler/icons-react';
import useGetInfoVehicle from '@/hooks/dashboard/passport/useGetInfoVehicle';
import { formatNumber } from '@/lib/dataDisplay';
import BibRecommendation from '@/components/entities/dashboard/passport/report/BibRecommendation';
import PinVehicleSwitch from '@/components/entities/dashboard/passport/datacard/PinVehicleSwitch';
import { PDFDownloadLink } from '@react-pdf/renderer';
import PassportPdf from '@/components/pdf/PassportPdf';
import ReportModal from '@/components/entities/dashboard/passport/report/ReportModal';
import MiniScoreCard from '@/components/entities/dashboard/passport/datacard/MiniScoreCard';
import useGetPriceForecast from '@/hooks/dashboard/passport/useGetPriceForecast';
import useGetKpiAdditional from '@/hooks/dashboard/passport/useGetKpiAdditional';

const labelClass = 'text-black text-xs md:text-sm';
const valueClass = 'text-right text-gray text-sm';
const boxClass =
  'bg-white rounded-3xl shadow-lg p-4 flex flex-col gap-4 min-h-[180px] shadow-sm';
const titleClass = 'flex items-center gap-2 mb-2 text-primary text-base text-left';

interface VehicleInfoProps {
  vin: string | undefined;
}

const VehicleInfoMain: React.FC<{ vin: string | undefined }> = ({ vin }) => {
  const { data: infoVehicle, isLoading } = useGetInfoVehicle(vin);
  const [showReportModal, setShowReportModal] = useState(false);

  if (isLoading || !infoVehicle) {
    return (
      <div className={boxClass}>
        <div className="w-full h-[120px] animate-pulse flex items-center justify-center">
          <div className="w-8 h-8 border-4 border-primary border-t-transparent rounded-full animate-spin"></div>
        </div>
      </div>
    );
  }
  return (
    <div className={boxClass}>
      <div className={titleClass}>
        <span className="bg-blue-100 p-1.5 rounded-xl flex items-center justify-center">
          <IconCar className="w-5 h-5 text-gray" />
        </span>
        Vehicle Information
      </div>
      <div className="flex flex-col gap-2">
        <div className="flex justify-between items-center">
          <span className={labelClass}>Make</span>
          <span className={valueClass}>{infoVehicle.vehicle_info.brand}</span>
        </div>
        <div className="flex justify-between items-center">
          <span className={labelClass}>Model</span>
          <span className="text-right text-xs md:text-sm text-gray">
            {infoVehicle.vehicle_info.model}
          </span>
        </div>
        <div className="flex justify-between items-center">
          <span className={labelClass}>VIN</span>
          <span className="text-right text-xs md:text-sm text-gray">
            {infoVehicle.vehicle_info.vin}
          </span>
        </div>
        <div className="flex justify-between items-center">
          <span className={labelClass}>Plate Number</span>
          <span className={valueClass}>{infoVehicle.vehicle_info.licence_plate}</span>
        </div>
        <div className="flex justify-between items-center">
          <span className={labelClass}>Start Date</span>
          <span className="text-right text-xs md:text-sm text-gray">
            {infoVehicle.vehicle_info.start_date}
          </span>
        </div>
        <div className="w-full flex flex-row items-center justify-center gap-3 mt-2">
          <button
            className="bg-white border border-primary/30 shadow-md rounded-full px-4 py-2 text-xs flex items-center gap-2 font-bold text-primary hover:bg-primary/10 transition-colors w-auto min-w-[120px] justify-center"
            onClick={() => setShowReportModal(true)}
          >
            <IconEye className="w-4 h-4" /> See Report
          </button>
          <PDFDownloadLink
            document={<PassportPdf data={infoVehicle} />}
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
      </div>
      {showReportModal && (
        <ReportModal onClose={() => setShowReportModal(false)} data={infoVehicle} />
      )}
    </div>
  );
};

const VehicleStats: React.FC<{ vin: string | undefined }> = ({ vin }) => {
  const { data: infoVehicle, isLoading } = useGetInfoVehicle(vin);
  const { data: kpiAdditional, isLoading: isKpiAdditionalLoading } =
    useGetKpiAdditional(vin);

  if (isLoading || isKpiAdditionalLoading) {
    return (
      <div className={boxClass}>
        <div className="w-full h-[120px] animate-pulse flex items-center justify-center">
          <div className="w-8 h-8 border-4 border-primary border-t-transparent rounded-full animate-spin"></div>
        </div>
      </div>
    );
  }

  if (!infoVehicle || !kpiAdditional) {
    return <div className={boxClass}>No data</div>;
  }

  return (
    <div className={boxClass}>
      <div className={titleClass}>
        <span className="bg-blue-100 p-1.5 rounded-xl flex items-center justify-center">
          <IconBattery className="w-5 h-5 text-gray" />
        </span>
        Vehicle Data
      </div>
      <div className="flex flex-col gap-2">
        <div className="flex justify-between items-center">
          <span className={labelClass}>SoH</span>
          <span className="text-right text-xs md:text-sm text-gray">
            {formatNumber(infoVehicle.battery_info.soh, '%')}
          </span>
        </div>
        <div className="flex justify-between items-center">
          <span className={labelClass}>Mileage</span>
          <span className="text-right text-xs md:text-sm text-gray">
            {formatNumber(infoVehicle.vehicle_info.odometer, 'km')}
          </span>
        </div>
        <div className="flex justify-between items-center">
          <span className={labelClass}>Consumption</span>
          <span className="text-right text-xs md:text-sm text-gray">
            {formatNumber(kpiAdditional.consumption, 'kWh/100 km')}
          </span>
        </div>
        <div className="flex justify-between items-center">
          <span className={labelClass}>Total Cycles</span>
          <span className="text-right text-xs md:text-sm text-gray">
            {formatNumber(infoVehicle.vehicle_info.cycles, 'cycles')}
          </span>
        </div>
        <div className="flex flex-col gap-y-1 w-full">
          <span className={labelClass}>Score</span>
          <div className="w-full flex justify-center">
            <MiniScoreCard score={infoVehicle.vehicle_info.score} />
          </div>
        </div>
      </div>
    </div>
  );
};

interface VehicleActionsProps {
  vin: string | undefined;
}

const VehicleActions: React.FC<VehicleActionsProps> = ({ vin }) => {
  const { data: infoVehicle, isLoading } = useGetInfoVehicle(vin);
  const { data: priceForecast } = useGetPriceForecast(vin);

  if (isLoading || !infoVehicle) {
    return (
      <div className={boxClass}>
        <div className="w-full h-[120px] animate-pulse flex items-center justify-center">
          <div className="w-8 h-8 border-4 border-primary border-t-transparent rounded-full animate-spin"></div>
        </div>
      </div>
    );
  }
  return (
    <div className={boxClass}>
      <div className={titleClass}>
        <span className="bg-blue-100 p-1.5 rounded-xl flex items-center justify-center">
          <IconChartBar className="w-5 h-5 text-gray" />
        </span>
        Channelling Recommendation
      </div>
      <div className="w-full flex flex-col justify-between gap-2">
        <BibRecommendation score={infoVehicle.vehicle_info.score} />
        <PinVehicleSwitch vin={vin} />

        <div className="w-full flex justify-center">
          <div className="w-[80%] border-b border-gray/30 my-1" />
        </div>

        {priceForecast !== undefined &&
          priceForecast.price !== null &&
          priceForecast.price_discount !== null && (
            <>
              <div className="flex justify-between items-center">
                <span className={labelClass}>Advertised Price</span>
                <span className="text-right text-xs md:text-sm text-gray">
                  {formatNumber(priceForecast.price.toFixed(0))} €
                </span>
              </div>
              <div className="flex justify-between items-center">
                <span className={labelClass}>SoH Adjustement</span>
                <span
                  className={`text-right text-xs md:text-sm ${priceForecast.price_discount > 0 ? 'text-green-price' : 'text-red-price'}`}
                >
                  {priceForecast.price_discount > 0 ? '+ ' : '- '}
                  {formatNumber(Math.abs(priceForecast.price_discount).toFixed(0))} €
                </span>
              </div>
            </>
          )}
      </div>
    </div>
  );
};

const VehicleInfo: React.FC<VehicleInfoProps> = ({ vin }) => {
  return (
    <div className="w-full bg-[#f7f8fa] p-2">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <VehicleInfoMain vin={vin} />
        <VehicleStats vin={vin} />
        <VehicleActions vin={vin} />
      </div>
    </div>
  );
};

export default VehicleInfo;
