import React from 'react';
import { SelectedFilter } from '@/interfaces/common/optionSelector/DataFilter';
import useGetScatterGlobal from '@/hooks/dashboard/global/useGetScatterGlobal';
import LoadingSmall from '@/components/common/loading/loadingSmall';
import DisplayLineChartScatterGlobal from '@/components/charts/global/DisplayLineChartScatterGlobal';
import useGetRegionGlobal from '@/hooks/dashboard/global/useGetRegionGlobal';
import TitleBox from '@/components/common/TitleBox';
import { FleetBrandData } from '@/interfaces/dashboard/global/GlobalDashboard';

const GraphTrendlineGlobal: React.FC<{ filters: SelectedFilter[] }> = ({ filters }) => {
  const { data, isLoading, fleetSelected } = useGetScatterGlobal(filters);
  const { data: dataRegion, isLoading: isLoadingRegion } = useGetRegionGlobal(
    filters,
    fleetSelected,
  );

  const hasEnoughDataPoints = (data: FleetBrandData | null): boolean => {
    if (!data) return false;
    return Object.values(data).some((brands) =>
      Object.values(brands).some((items) => {
        const validItems = items.filter((item) => item.soh !== null);
        return validItems.length >= 10;
      }),
    );
  };

  return (
    <>
      <div
        className={`bg-white w-full ${
          fleetSelected &&
          !isLoading &&
          data &&
          Object.keys(data).length > 0 &&
          hasEnoughDataPoints(data)
            ? 'h-[550px]'
            : 'max-h-[400px]'
        } overflow-auto p-5 rounded-lg box-boder`}
      >
        <TitleBox
          title={'Click on the make you want to display'}
          titleSecondary={'SoH trendline by make'}
        />
        {!fleetSelected ? (
          <div className="flex justify-center items-center h-[200px]">
            <p className="text-gray-blue text-base">
              Please select a fleet to view the trendlines
            </p>
          </div>
        ) : isLoading ? (
          <LoadingSmall />
        ) : !data || Object.keys(data || {}).length === 0 ? (
          <div className="flex justify-center items-center h-[200px]">
            <p className="text-gray-blue text-base">No data available</p>
          </div>
        ) : !hasEnoughDataPoints(data) ? (
          <div className="flex justify-center items-center h-[200px]">
            <p className="text-gray-blue text-base">Cannot display trendlines</p>
          </div>
        ) : (
          <div className="h-[430px]">
            <DisplayLineChartScatterGlobal data={data} />
          </div>
        )}
      </div>
      <div
        className={`bg-white w-full ${
          fleetSelected &&
          !isLoadingRegion &&
          dataRegion &&
          Object.keys(dataRegion).length > 0 &&
          hasEnoughDataPoints(dataRegion)
            ? 'h-[550px]'
            : 'max-h-[400px]'
        } overflow-hidden p-5 rounded-lg box-boder mt-8`}
      >
        <TitleBox
          title={'Click on the region you want to display'}
          titleSecondary={'SoH trendline by region'}
        />
        {!fleetSelected ? (
          <div className="flex justify-center items-center h-[200px]">
            <p className="text-gray-blue text-base">
              Please select a fleet to view the trendlines
            </p>
          </div>
        ) : isLoadingRegion ? (
          <LoadingSmall />
        ) : !dataRegion || Object.keys(dataRegion || {}).length === 0 ? (
          <div className="flex justify-center items-center h-[200px]">
            <p className="text-gray-blue text-base">No data available</p>
          </div>
        ) : !hasEnoughDataPoints(dataRegion) ? (
          <div className="flex justify-center items-center h-[200px]">
            <p className="text-gray-blue text-base">Cannot display trendlines</p>
          </div>
        ) : (
          <div className="h-[430px]">
            <DisplayLineChartScatterGlobal data={dataRegion} />
          </div>
        )}
      </div>
    </>
  );
};

export default GraphTrendlineGlobal;
