import React from 'react';
import VehicleHeader from '@/components/entities/dashboard/passport/report/web-report/VehicleHeader';
import ReportFooter from '@/components/entities/dashboard/passport/report/web-report/ReportFooter';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';
import SohChart from '@/components/entities/dashboard/passport/report/web-report/SohChart';
import RangeInfo from '@/components/entities/dashboard/passport/report/web-report/RangeInfo';
import WarrantyInfo from '@/components/entities/dashboard/passport/report/web-report/WarrantyInfo';

interface FirstPageProps {
  reportData: InfoVehicleResult;
}

const FirstPage: React.FC<FirstPageProps> = ({ reportData }) => {
  return (
    <div className="flex flex-col rounded-lg overflow-hidden bg-gray-background">
      <VehicleHeader
        brand={reportData.vehicle_info?.brand}
        model={reportData.vehicle_info?.model}
        vin={reportData.vehicle_info?.vin}
        mileage={reportData.vehicle_info?.odometer}
        immatriculation={reportData.vehicle_info?.licence_plate}
        score={reportData.vehicle_info?.score}
        start_date={reportData.vehicle_info?.start_date}
        image_url={reportData.vehicle_info?.image_url}
        vehicleBatteryInfo={reportData}
      />

      {/* Container pour centrer les autres bulles */}
      <div className="flex flex-col items-center px-6 pb-6">
        {/* <BatteryInfo vehicleBatteryInfo={reportData} /> */}

        <SohChart reportData={reportData} />
        <div className="flex flex-col md:flex-row gap-6 mb-6 max-w-4xl w-full">
          <RangeInfo reportData={reportData} />
          <WarrantyInfo vehicleBatteryInfo={reportData} />
        </div>
        {/* Container pour les informations de charge */}
      </div>
      <ReportFooter />
    </div>
  );
};

export default FirstPage;
