import React from 'react';
import HorizontalBarPassport from './HorizontalBarPassport';
import useGetEstimatedRange from '@/hooks/dashboard/passport/useGetEstimatedRange';
import LoadingSmall from '@/components/common/loading/loadingSmall';

const EstimatedRange: React.FC<{ vin: string | undefined }> = ({ vin }) => {
  const { data, isLoading } = useGetEstimatedRange(vin);

  return (
    <div className="w-full h-auto bg-white rounded-[20px] p-4 px-8 box-border mt-4">
      {isLoading ? (
        <div className="flex justify-center items-center h-full">
          <LoadingSmall />
        </div>
      ) : data && Object.keys(data).length > 0 ? (
        <HorizontalBarPassport data={data} />
      ) : (
        <div className="flex justify-center items-center h-full">
          <p className="text-gray-500">No data available</p>
        </div>
      )}
    </div>
  );
};

export default EstimatedRange;
