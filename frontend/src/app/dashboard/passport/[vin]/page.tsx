'use client';

import React, { useEffect, useState } from 'react';
import LineChart from '@/components/dashboard/chart/LineChart';

import { useParams } from 'next/navigation';
import VehicleInfo from '@/components/dashboard/passport/VehicleInfo';
import { toast } from 'sonner';
import EstimatedRange from '@/components/dashboard/passport/EstimatedRange';
import KpiAdditional from '@/components/dashboard/passport/KpiAdditional';
import DisplayFilterButtons from '@/components/common/DisplayFilterButtons';
import TitleBox from '@/components/common/TitleBox';
import {
  usePostPinVehicle,
  useGetPinVehicle,
} from '@/hooks/dashboard/passport/usePinVehicle';

const PassPort: React.FC = () => {
  const [activateValue, setActivateValue] = useState<string>('Week');
  const params = useParams();
  const vin = params.vin as string;

  const { data: pinData, isLoading: isPinLoading } = useGetPinVehicle(vin);
  const { isSubmitting, error, postPinVehicle } = usePostPinVehicle(vin);

  const [isPinned, setIsPinned] = useState(false);

  useEffect(() => {
    if (pinData && typeof pinData.is_pinned === 'boolean') {
      setIsPinned(pinData.is_pinned);
    }
  }, [pinData]);

  const handlePinVehicle = async (isPinned: boolean): Promise<void> => {
    if (isSubmitting) {
      toast.error('Please wait for the request to complete');
      return;
    }
    try {
      await postPinVehicle(isPinned);
      if (error) {
        toast.error(error);
        return;
      }
      setIsPinned((prev) => !prev);
      toast.success(isPinned ? 'Vehicle pinned' : 'Vehicle unpinned');
    } catch (error: unknown) {
      toast.error('Error pinning vehicle' + error);
    }
  };

  return (
    <div className="flex flex-col h-full w-full space-y-8 pt-2">
      <VehicleInfo
        vin={vin}
        isPinned={isPinned}
        isPinLoading={isPinLoading}
        onPinChange={handlePinVehicle}
      />
      {/* <div className="flex justify-center gap-2 mac:justify-between mt-4">
        <DataCardsGraph vin={vin} />
      </div> */}
      <div className="w-full h-[500px] bg-white rounded-[20px] p-4 pb-12 px-8 box-border">
        <div className="flex justify-between items-center mb-4">
          <TitleBox title={'Vehicle overview'} titleSecondary={'SoH evolution'} />
          <div className="flex items-center gap-6">
            <div className="flex gap-4">
              <span className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-blue-almost-filled"></div>
                <p className="text-gray-blue text-sm">Vehicle SoH</p>
                <div className="w-4 h-[2px] border-t-2 border-dashed border-blue-almost-filled"></div>
                <p className="text-gray-blue text-sm">Prediction</p>
              </span>
              <span className="flex items-center gap-2">
                <div className="w-4 h-[2px] border-t-2 border-dashed border-[#9BA3AF]"></div>
                <p className="text text-sm text-gray-blue">Average Model SoH</p>
              </span>
            </div>
          </div>

          <div className="text-gray-blue text-sm font-extralight bg-white-ghost rounded-[14px] flex items-center h-[40px] p-2 shadow-md">
            <DisplayFilterButtons
              selected={activateValue}
              setSelected={setActivateValue}
              filters={['Week', 'Month', 'Year']}
            />
          </div>
        </div>

        <div className="h-[calc(100%-60px)]">
          <LineChart formatDate={activateValue} vin={vin} />
        </div>
      </div>
      <div className="flex flex-col lg:flex-col gap-4 mt-4">
        <div className="w-full lg:w-full">
          <KpiAdditional vin={vin} />
        </div>
        <div className="w-full lg:w-full lg:mt-0 mb-16">
          <EstimatedRange vin={vin} />
        </div>
      </div>
    </div>
  );
};

export default PassPort;
