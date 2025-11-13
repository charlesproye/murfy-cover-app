import useGetKpiAdditional from '@/hooks/dashboard/passport/useGetKpiAdditional';
import { IconGasStation, IconBolt } from '@tabler/icons-react';
import React from 'react';
import ChargingCyclesDonut from './ChargingCyclesDonut';

const KpiAdditional: React.FC<{ vin: string | undefined }> = ({ vin }) => {
  const { data } = useGetKpiAdditional(vin);

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      <ChargingCyclesDonut vin={vin} formatDate="monthly" />

      <div className="bg-white rounded-[20px] py-6 px-8 flex flex-col">
        <div className="flex items-center gap-2 mb-6">
          <IconBolt className="text-primary" size={20} />
          <h3 className="text-primary font-medium">Total Cycles</h3>
        </div>
        <div className="flex-1 flex flex-col items-center justify-center">
          <p className="text-4xl font-semibold text-primary mb-4">
            {data?.cycles || '0'}
          </p>
          <p className="text-gray-blue">Estimated Total Charging Cycles</p>
        </div>
      </div>

      <div className="bg-white rounded-[20px] py-6 px-8 flex flex-col">
        <div className="flex items-center gap-2 mb-6">
          <IconGasStation className="text-primary" size={20} />
          <h3 className="text-primary font-medium">Consumption</h3>
        </div>
        <div className="flex-1 flex flex-col items-center justify-center">
          <p className="text-4xl font-semibold text-primary mb-4">
            {data?.consumption ? data.consumption.toFixed(2) : 'Unknown'}
          </p>
          <p className="text-gray-blue">kWh per 100km</p>
        </div>
      </div>
    </div>
  );
};

export default KpiAdditional;
