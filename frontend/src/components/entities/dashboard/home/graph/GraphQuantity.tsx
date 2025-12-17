import useGetNewVehicles from '@/hooks/dashboard/home/useGetNewVehicles';
import React, { useState } from 'react';

import LoadingSmall from '@/components/common/loading/loadingSmall';
import DisplayFilterButtons from '@/components/filters/filter-buttons/DisplayFilterButtons';
import TitleBox from '@/components/common/TitleBox';
import HorizontalBarChart from '@/components/charts/home/HorizontalBarChart';

const GraphQuantity: React.FC<{ fleet: string | null }> = ({ fleet }) => {
  const [selected, setSelected] = useState<'Monthly' | 'Annually'>('Monthly');
  const { data, isLoading } = useGetNewVehicles(fleet, selected);

  return (
    <div className="flex flex-col">
      <div className="flex justify-between items-center">
        <TitleBox title={'Fleet overview'} titleSecondary={'New vehicles'} />
        <div className="flex gap-2 bg-white-ghost rounded-xl h-12  items-center">
          <DisplayFilterButtons
            selected={selected}
            setSelected={setSelected}
            filters={['Monthly', 'Annually']}
          />
        </div>
      </div>
      <div className="flex justify-center items-center mt-5">
        {isLoading ? (
          <LoadingSmall />
        ) : data && data.length > 0 ? (
          <div
            className="w-full"
            style={{ height: `${data.length * 50}px`, minHeight: '250px' }}
          >
            <HorizontalBarChart data={data} period={selected} />
            <p className="text-xs text-gray-500 text-center italic">Fleet size</p>
          </div>
        ) : (
          <p className="text-sm text-gray-blue text-center h-[200px] flex items-center justify-center">
            No new vehicles during last 4 months
          </p>
        )}
      </div>
    </div>
  );
};

export default GraphQuantity;
