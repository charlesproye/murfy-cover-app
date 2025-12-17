'use client';

import LoadingSmall from '@/components/common/loading/loadingSmall';
import useGetGlobalDashboard from '@/hooks/dashboard/global/useGetGlobalDashboard';
import { SelectedFilter } from '@/interfaces/common/optionSelector/DataFilter';
import React from 'react';
import { GlobalDashboardResult } from '@/interfaces/dashboard/global/GlobalDashboard';
import useFilteredAndSortedData from '@/hooks/dashboard/common/table/useFilteredAndSortedData';
import SortTableHeader from '@/components/table/SortTableHeader';
import useSortableFilter from '@/hooks/dashboard/common/table/useSortableFilter';
import TitleBox from '@/components/common/TitleBox';
import { formatNumber } from '@/lib/dataDisplay';

const headers: { label: string; filter: keyof GlobalDashboardResult }[] = [
  { label: 'Fleet', filter: 'fleet_name' },
  { label: 'Region', filter: 'region_name' },
  { label: 'Make', filter: 'oem_name' },
  { label: 'Avg Soh (%)', filter: 'avg_soh' },
  { label: 'Avg Mileage (km)', filter: 'avg_odometer' },
  { label: 'Fleet Size (#)', filter: 'vehicle_count' },
];

type ActiveFilter = keyof GlobalDashboardResult;

const TableBatteryInfo: React.FC<{ filters: SelectedFilter[] }> = ({ filters }) => {
  const { data, isLoading } = useGetGlobalDashboard(filters);
  const { activeFilter, sortOrder, handleChangeFilter } =
    useSortableFilter<ActiveFilter>();

  const memoizedData = useFilteredAndSortedData({ data, activeFilter, sortOrder });

  return (
    <div className="bg-white w-full max-h-[402px] overflow-auto p-5 rounded-lg box-boder">
      <TitleBox
        title={'SoH on Mileage ratio'}
        titleSecondary={'Global Fleet Dashboard'}
        dataDownload={data}
        downloadName={`global-fleet-dashboard`}
      />
      {isLoading ? (
        <LoadingSmall />
      ) : (
        <table className="w-full mobile:min-w-auto bg-white text-[14px] leading-4 border-spacing-y-14">
          <thead>
            <tr className="text-gray-light whitespace-nowrap">
              {headers.map(({ label, filter }) => (
                <SortTableHeader
                  key={label}
                  label={label}
                  filter={filter}
                  activeFilter={activeFilter}
                  sortOrder={sortOrder}
                  onChangeFilter={handleChangeFilter}
                />
              ))}
            </tr>
          </thead>
          <tbody className="">
            {memoizedData &&
              memoizedData?.map((row, index) => (
                <tr
                  key={index}
                  className={`${index === memoizedData.length - 1 ? '' : 'border-b border-b-gray-light'}`}
                >
                  <td className="py-4 font-medium">{row.fleet_name}</td>
                  <td className="py-4 font-medium">{row.region_name}</td>
                  <td className="font-medium">
                    {row.oem_name
                      ? row.oem_name.charAt(0).toUpperCase() + row.oem_name.slice(1)
                      : ''}
                  </td>
                  <td className="font-medium">{row.avg_soh}%</td>
                  <td className="font-medium">{formatNumber(row.avg_odometer)}</td>
                  <td className="font-medium">{row.vehicle_count} vehicles</td>
                </tr>
              ))}
          </tbody>
        </table>
      )}
    </div>
  );
};

export default TableBatteryInfo;
