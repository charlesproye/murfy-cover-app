'use client';

import React, { useCallback, useMemo, useState } from 'react';
import {
  PinnedVehicleColumns,
  PinnedVehicleDataResponse,
  usePinnedVehicles,
} from '@/hooks/pinned/useGetPinnnedVehicle';
import { useAuth } from '@/contexts/AuthContext';
import useInfiniteScroll from '@/hooks/dashboard/common/useInfiniteScroll';
import { capitalizeFirstLetter, formatNumber } from '@/lib/dataDisplay';
import { SortOrder } from '@/interfaces/common/filter/SortOrder';
import SortTableHeader from '@/components/table/SortTableHeader';
import Link from 'next/link';
import ChangeFleet from '@/components/common/ChangeFleet';

const headers = [
  { label: 'Vin', filter: 'vin' },
  { label: 'Make', filter: 'makeName' },
  { label: 'Odometer (km)', filter: 'odometer' },
  { label: 'SoH (%)', filter: 'soh' },
  {
    label: (
      <div>
        SoH/10 000km
        <span className="relative group ml-1 cursor-pointer">
          <span className="text-primary">ⓘ</span>
          <span className="absolute top-full right-0 transform translate-x-[5%] mt-2 w-64 p-2 bg-white text-gray-800 text-xs rounded-lg shadow-lg opacity-0 group-hover:opacity-100 transition-all duration-200 z-10 border border-gray-100">
            Soh lost per 10 000km based on the odometer and soh
            <div className="absolute -top-2 right-[15%] transform translate-x-1/2 w-4 h-4 bg-white border-l border-t border-gray-100 rotate-45"></div>
          </span>
        </span>
      </div>
    ),
    filter: 'sohPer10000km',
  },
  { label: 'Start contract date', filter: 'startDate' },
];

const FavoritesPage: React.FC = () => {
  const [inputValue, setInputValue] = useState('');
  const { fleet } = useAuth();
  const [sortingColumn, setSortingColumn] = useState<
    keyof PinnedVehicleDataResponse | ''
  >('');
  const [sortDirection, setSortDirection] = useState<SortOrder>('desc');

  const {
    setListRef: setPinnedRef,
    data: pinnedData,
    isLoading,
  } = useInfiniteScroll<PinnedVehicleDataResponse>({
    fleet: fleet.id,
    label: 'pinned',
    fetchFunction: usePinnedVehicles,
  });

  const filteredVehicles = useMemo(() => {
    const filteredVehicles = inputValue
      ? pinnedData.filter(
          (v) => v.vin && v.vin.toLowerCase().includes(inputValue.toLowerCase()),
        )
      : [...pinnedData];

    if (!sortingColumn || !PinnedVehicleColumns.includes(sortingColumn)) {
      return filteredVehicles;
    }

    const numericColumns: (keyof PinnedVehicleDataResponse)[] = [
      'odometer',
      'soh',
      'sohPer10000km',
    ];

    if (numericColumns.includes(sortingColumn)) {
      return filteredVehicles.sort((a, b) => {
        const aValue = a[sortingColumn] ?? 0;
        const bValue = b[sortingColumn] ?? 0;
        if (sortDirection === 'asc') {
          return (aValue as number) - (bValue as number);
        }
        return (bValue as number) - (aValue as number);
      });
    }

    // For string columns (vin, makeName, startDate)
    return filteredVehicles.sort((a, b) => {
      const aValue = (a[sortingColumn] ?? '') as string;
      const bValue = (b[sortingColumn] ?? '') as string;
      if (sortDirection === 'asc') {
        return aValue.localeCompare(bValue);
      }
      return bValue.localeCompare(aValue);
    });
  }, [pinnedData, inputValue, sortingColumn, sortDirection]);

  const handleChangeFilter = useCallback(
    (newSortingColumn: keyof PinnedVehicleDataResponse) => {
      setSortDirection((prevSortDirection) =>
        prevSortDirection === 'asc' ? 'desc' : 'asc',
      );
      if (sortingColumn !== newSortingColumn) setSortingColumn(newSortingColumn);
    },
    [sortingColumn],
  );

  return (
    <div className="w-full bg-white rounded-2xl shadow-sm p-6 space-y-4">
      {/* Header */}
      <div className="flex items-center gap-2 mb-6">
        <h1 className="text-lg font-medium text-black">Favorite vehicles</h1>
        <span role="img" aria-label="download">
          ⬇️
        </span>
        {isLoading && (
          <div className="animate-spin rounded-full h-4 w-4 border-2 border-primary border-t-transparent" />
        )}
      </div>

      {/* Filters Card */}
      <div className="bg-primary/5 border-2 border-primary/20 rounded-xl flex gap-2 w-fit">
        <div className="flex-shrink-0">
          <span className="text-primary font-medium text-sm pl-2">Filters</span>
        </div>
        <div className="flex-1 flex flex-col sm:flex-row gap-3 sm:gap-8 p-4">
          <div className="w-60 relative border border-2 rounded-md border-primary/30 bg-white focus:border-primary focus:ring-2 focus:ring-primary/20 transition-colors">
            <label
              htmlFor="vin"
              className="absolute -top-2.5 left-3 px-2 text-sm text-gray rounded-lg"
              style={{
                backgroundColor: 'color-mix(in oklab, var(--color-primary) 5%, white)',
              }}
            >
              Filter by VIN
            </label>
            <input
              type="text"
              id="vin"
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              className="shadow-none font-medium text-base w-full h-full px-4 py-2 border-none bg-white focus:outline-none"
            />
          </div>
          <ChangeFleet className="border-primary/30 bg-white" />
        </div>
      </div>

      <div ref={setPinnedRef} className="w-full max-h-[500px] overflow-y-auto">
        <div className="overflow-x-auto scrollbar-thin scrollbar-thumb-primary/30 scrollbar-track-white rounded-xl">
          <table className="min-w-[600px] w-full text-left table-fixed text-sm">
            <thead>
              <tr className="text-gray font-light">
                {headers.map((header, index) => (
                  <SortTableHeader<keyof PinnedVehicleDataResponse>
                    key={header.filter + '-' + index}
                    label={header.label}
                    filter={header.filter as keyof PinnedVehicleDataResponse}
                    activeFilter={sortingColumn}
                    sortOrder={sortDirection}
                    onChangeFilter={handleChangeFilter}
                  />
                ))}
              </tr>
            </thead>
            <tbody>
              {filteredVehicles.length === 0 && !isLoading ? (
                <tr>
                  <td colSpan={6} className="text-center py-4 text-gray">
                    No vehicle found
                  </td>
                </tr>
              ) : (
                filteredVehicles.map((v, index) => (
                  <tr
                    key={index}
                    className={`py-12 ${index % 2 === 0 ? 'bg-primary/5 border-b border-primary/10' : ''}`}
                  >
                    <td className="py-4 text-black text-left text-primary pl-1">
                      <Link href={`/dashboard/passport/${v.vin}`}>{v.vin}</Link>
                    </td>
                    <td className="py-4 text-left">
                      {capitalizeFirstLetter(v.makeName)}
                    </td>
                    <td className="py-4 text-black text-left">
                      {formatNumber(v.odometer?.toFixed(0))}
                    </td>
                    <td className="py-4 text-black text-left">{formatNumber(v.soh)}</td>
                    <td className="py-4 text-black text-left">
                      {formatNumber(v.sohPer10000km)}
                    </td>
                    <td className="py-4 text-left">{v.startDate}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default FavoritesPage;
