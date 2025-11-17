import useGetSohRange from '@/hooks/dashboard/home/useGetSohRange';
import React, { useState } from 'react';
import LoadingSmall from '@/components/common/loading/loadingSmall';
import DisplayFilterButtons from '@/components/common/DisplayFilterButtons';
import TitleBox from '@/components/common/TitleBox';
import BarChart from '../chart/BarChart';

type GraphSoHProps = {
  fleet: string | null;
  setIsDisplay: (isDisplay: boolean, clickedData: string | null) => void;
};

const GraphSoH: React.FC<GraphSoHProps> = ({ fleet, setIsDisplay }) => {
  const [selected, setSelected] = useState<'SoH' | 'Mileage'>('SoH');

  const { data, isLoading } = useGetSohRange(fleet, selected);

  return (
    <div className="p-6">
      <div className="flex flex-row justify-between items-center">
        <TitleBox title={'Fleet overview'} titleSecondary={'Real Time data'} />
        <div className="flex gap-2 bg-white-ghost rounded-xl h-12 items-center">
          <DisplayFilterButtons
            selected={selected}
            setSelected={setSelected}
            filters={['SoH', 'Mileage']}
          />
        </div>
      </div>
      <div className="w-full h-full mt-8">
        {isLoading ? (
          <LoadingSmall />
        ) : (
          <div className="w-full h-[250px] min-h-[250px]">
            <BarChart data={data} type={selected} setIsDisplay={setIsDisplay} />
            <p className="text-xs text-gray-500 text-center italic">
              {selected === 'SoH' ? 'SoH (%)' : 'Mileage (km)'}
            </p>
          </div>
        )}
      </div>
    </div>
  );
};

export default GraphSoH;
