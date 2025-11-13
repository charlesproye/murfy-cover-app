import React, { useState, ReactNode, useCallback, Dispatch, SetStateAction } from 'react';
import LoadingSmall from '@/components/common/loading/loadingSmall';
import SortTableHeader from '@/components/common/SortTableHeader';
import useInfiniteTableExtremum from '@/hooks/dashboard/home/useInfiniteTableExtremum';
import useInfiniteScrollNew from '@/hooks/dashboard/common/useInfiniteScrollGeneric';
import TitleBoxExtremum from '@/components/home/extremumSection/TitleBoxExtremum';
import Link from 'next/link';
import { TableExtremumResult } from '@/interfaces/dashboard/home/table/TablebrandResult';
import BrandSelector from '../BrandSelector';
import DisplayFilterButtons from '@/components/common/DisplayFilterButtons';
import { SortOrder } from '@/interfaces/common/filter/SortOrder';
import { formatNumber } from '@/utils/formatNumber';

const TableExtremum: React.FC<{ fleet: string | null }> = ({ fleet }): ReactNode => {
  const [selectedBrand, setSelectedBrand] = useState<string>('');
  const [quality, setQuality] = useState<'Best' | 'Worst' | ''>('Best');
  const [sortingColumn, setSortingColumn] = useState<
    keyof TableExtremumResult['vehicles'][0] | ''
  >('');
  const [sortingOrder, setSortingOrder] = useState<SortOrder>('asc');
  const { data, brands, pagination, isLoading, isLoadingMore, hasNextPage, loadMore } =
    useInfiniteTableExtremum(
      fleet,
      selectedBrand,
      10,
      quality,
      sortingColumn,
      sortingOrder,
    );

  const handleChangeFilter = useCallback(
    (newSortingColumn: keyof TableExtremumResult['vehicles'][0]) => {
      setSortingOrder((prevSortOrder) => (prevSortOrder === 'asc' ? 'desc' : 'asc'));
      if (sortingColumn !== newSortingColumn) setSortingColumn(newSortingColumn);
      if (newSortingColumn && quality) setQuality('');
    },
    [sortingColumn],
  );

  const handleChangeQuality = useCallback(
    (newQuality: 'Best' | 'Worst' | '') => {
      if (newQuality && sortingColumn) setSortingColumn('');
      setQuality(newQuality);
    },
    [sortingColumn],
  );

  const containerRef = useInfiniteScrollNew({
    hasNextPage,
    isLoading: isLoadingMore,
    onLoadMore: loadMore,
    threshold: 10,
  });

  const headers = [
    { label: 'Make', filter: '', show: true },
    { label: 'Vin', filter: 'vin', show: true },
    { label: 'Mileage', filter: 'odometer', show: true },
    { label: 'SoH', filter: 'soh', show: true },
    { label: 'Score', filter: 'score', show: true },
    { label: 'Warranty Date', filter: '', show: true },
  ];

  if (isLoading) return <LoadingSmall />;
  return (
    <div className="w-full rounded-lg box-border py-2">
      {/* Fixed header section */}
      <div className="flex justify-between items-center mb-4">
        <TitleBoxExtremum
          title="Extremum performance vehicle"
          titleSecondary="Sort by brand"
          quality={quality}
          fleet={fleet}
          downloadName={`extremum-performance-vehicle-${selectedBrand}-${quality}`}
        />
        <div className="flex gap-4">
          <DisplayFilterButtons<'Best' | 'Worst' | ''>
            selected={quality}
            setSelected={
              handleChangeQuality as Dispatch<SetStateAction<'Best' | 'Worst' | ''>>
            }
            filters={['Best', 'Worst']}
          />
          <BrandSelector
            selected={selectedBrand}
            setSelected={setSelectedBrand}
            brands={brands}
            className="h-12"
          />
        </div>
      </div>

      {/* Scrollable table section */}
      <div ref={containerRef} className="max-h-[500px] overflow-y-auto">
        <table className="min-w-full bg-white text-[14px] leading-4 border-spacing-y-14">
          <thead className="sticky top-0 bg-white z-10">
            <tr className="text-gray-light whitespace-nowrap">
              {headers.map(
                (header) =>
                  header.show && (
                    <SortTableHeader
                      key={header.label}
                      label={header.label}
                      filter={header.filter as keyof TableExtremumResult['vehicles'][0]}
                      activeFilter={sortingColumn}
                      sortOrder={sortingOrder}
                      onChangeFilter={handleChangeFilter}
                    />
                  ),
              )}
            </tr>
          </thead>
          <tbody>
            {data &&
              data.map((row, index) => (
                <tr
                  key={index}
                  className={`${index === data.length - 1 ? '' : 'border-b border-b-gray-light'}`}
                >
                  <td className="py-4 font-medium">
                    {row.oem_name
                      ? row.oem_name.charAt(0).toUpperCase() + row.oem_name.slice(1)
                      : ''}
                  </td>
                  <td className="py-4 font-medium">
                    <Link href={`/dashboard/passport/${row.vin}`}>
                      <span className="text-primary">{row.vin}</span>
                    </Link>
                  </td>
                  <td className="font-medium">{formatNumber(row.odometer)}</td>
                  <td className="font-medium">{formatNumber(row.soh)}</td>
                  <td className="font-medium">{row.score}</td>
                  <td className="font-medium">
                    {formatNumber(row.years_remaining, 'years')}
                  </td>
                </tr>
              ))}
          </tbody>
        </table>

        {/* Pagination info */}
        {pagination && (
          <div className="flex justify-between items-center px-4 py-2 text-sm text-gray-600 border-t border-gray-200">
            <span>
              Showing {data.length} of {pagination.total_items} vehicles
            </span>
          </div>
        )}

        {/* Load More Indicator */}
        {hasNextPage && !isLoadingMore && (
          <div className="flex flex-col items-center justify-center py-6 px-4 border-t border-gray-200 bg-gray-50">
            <div className="flex flex-col items-center space-y-2">
              <div className="w-6 h-6 rounded-full bg-primary/10 flex items-center justify-center">
                <svg
                  className="w-3 h-3 text-primary"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M19 14l-7 7m0 0l-7-7m7 7V3"
                  />
                </svg>
              </div>
              <p className="text-xs text-gray-500">Scroll down to load more data</p>
            </div>
          </div>
        )}

        {/* Loading more indicator */}
        {isLoadingMore && (
          <div className="flex justify-center items-center py-8 px-4 border-t border-gray-200 bg-gray-50">
            <div className="flex flex-col items-center space-y-3">
              <LoadingSmall />
              <p className="text-sm text-gray-600">Loading more vehicles...</p>
            </div>
          </div>
        )}

        {data && data.length === 0 && !isLoading && (
          <div className="flex justify-center items-center h-[200px]">
            <p className="text-gray-blue text-base">No data available</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default TableExtremum;
