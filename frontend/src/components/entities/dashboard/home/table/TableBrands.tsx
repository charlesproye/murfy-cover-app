import useGetTableBrands from '@/hooks/dashboard/home/useGetTableBrands';
import React, { useState } from 'react';
import LoadingSmall from '@/components/common/loading/loadingSmall';
import { TableBrandsResult } from '@/interfaces/dashboard/home/table/TablebrandResult';
import useFilteredAndSortedData from '@/hooks/dashboard/common/table/useFilteredAndSortedData';
import SortTableHeader from '@/components/table/SortTableHeader';
import useSortableFilter from '@/hooks/dashboard/common/table/useSortableFilter';
import DisplayFilterButtons from '@/components/filters/filter-buttons/DisplayFilterButtons';
import TitleBox from '@/components/common/TitleBox';
import { formatNumber } from '@/lib/dataDisplay';

type ActiveFilter = keyof TableBrandsResult;

const TableBrands: React.FC<{ fleet: string | null }> = ({ fleet }) => {
  const [selected, setSelected] = useState<'Make' | 'Region'>('Make');
  const { data, isLoading } = useGetTableBrands(fleet, selected);
  const { activeFilter, sortOrder, handleChangeFilter } =
    useSortableFilter<ActiveFilter>();

  const headers: { label: string; filter: keyof TableBrandsResult; show: boolean }[] = [
    {
      label: 'Make',
      filter: 'oem_name',
      show: selected === 'Make',
    },
    {
      label: 'Region',
      filter: 'region_name',
      show: selected === 'Region',
    },
    { label: 'Avg Soh (%)', filter: 'avg_soh', show: true },
    { label: 'Avg Mileage (km)', filter: 'avg_odometer', show: true },
    { label: 'Fleet Size', filter: 'nb_vehicle', show: true },
  ];

  const memoizedData = useFilteredAndSortedData({ data, activeFilter, sortOrder });

  if (isLoading) return <LoadingSmall />;
  return (
    <div className="w-full max-h-[402px] overflow-auto rounded-lg box-boder p-6">
      <div className="flex justify-between items-center">
        <TitleBox
          title={'Fleet overview'}
          titleSecondary={'Global'}
          dataDownload={data}
          downloadName={`fleet-overview-${selected.toLowerCase()}`}
        />
        <div className="flex items-center gap-2">
          <DisplayFilterButtons
            selected={selected}
            setSelected={setSelected}
            filters={['Make', 'Region']}
          />
        </div>
      </div>
      <table className="mt-4 w-full bg-white text-[14px] leading-4 border-spacing-y-14">
        <thead>
          <tr className="text-gray-light whitespace-nowrap">
            {headers.map(
              (header) =>
                header.show && (
                  <SortTableHeader
                    key={header.label}
                    label={header.label}
                    filter={header.filter}
                    activeFilter={activeFilter}
                    sortOrder={sortOrder}
                    onChangeFilter={handleChangeFilter}
                  />
                ),
            )}
          </tr>
        </thead>
        <tbody>
          {memoizedData &&
            memoizedData?.map((row, index) => (
              <tr
                key={index}
                className={`${index === memoizedData.length - 1 ? '' : 'border-b border-b-gray-light'}`}
              >
                {selected === 'Make' ? (
                  <td className="py-4 font-medium">
                    {row.oem_name
                      ? row.oem_name.charAt(0).toUpperCase() + row.oem_name.slice(1)
                      : ''}
                  </td>
                ) : selected === 'Region' ? (
                  <td className="py-4 font-medium">{row.region_name}</td>
                ) : (
                  <>
                    <td className="py-4 font-medium">{row.oem_name}</td>
                    <td className="py-4 font-medium">{row.region_name}</td>
                  </>
                )}
                <td className="font-medium">{formatNumber(row.avg_soh)}</td>
                <td className="font-medium">{formatNumber(row?.avg_odometer)}</td>
                <td className="font-medium">{formatNumber(row?.nb_vehicle)} vehicles</td>
              </tr>
            ))}
        </tbody>
      </table>
    </div>
  );
};

export default TableBrands;
