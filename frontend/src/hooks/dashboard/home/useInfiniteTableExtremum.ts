import { useState, useEffect, useCallback } from 'react';
import { ROUTES } from '@/routes';
import { TableExtremumResult } from '@/interfaces/dashboard/home/table/TablebrandResult';
import fetchWithAuth from '@/services/fetchWithAuth';
import { SortOrder } from '@/interfaces/common/filter/SortOrder';

const minLoadingTime = 500; // Minimum 500ms loading time

interface InfiniteTableExtremumResult {
  data: TableExtremumResult['vehicles'];
  brands: TableExtremumResult['brands'];
  pagination: TableExtremumResult['pagination'] | null;
  isLoading: boolean;
  isLoadingMore: boolean;
  error: unknown;
  hasNextPage: boolean;
  loadMore: () => void;
  reset: () => void;
}

const useInfiniteTableExtremum = (
  fleet: string | null,
  selected: string,
  pageSize: number = 10,
  quality: 'Best' | 'Worst' | '',
  sortingColumn: keyof TableExtremumResult['vehicles'][0] | '',
  sortingOrder: SortOrder,
): InfiniteTableExtremumResult => {
  const [data, setData] = useState<TableExtremumResult['vehicles']>([]);
  const [brands, setBrands] = useState<TableExtremumResult['brands']>([]);
  const [pagination, setPagination] = useState<TableExtremumResult['pagination'] | null>(
    null,
  );
  const [isLoading, setIsLoading] = useState(false);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [currentPage, setCurrentPage] = useState(1);

  const fetchData = useCallback(
    async (page: number, isLoadMore: boolean = false) => {
      if (!fleet) return;

      const startTime = Date.now();

      try {
        if (isLoadMore) {
          setIsLoadingMore(true);
        } else {
          setIsLoading(true);
          setError(null);
        }

        const url = `${ROUTES.TABLE_EXTREMUM}?fleet_id=${fleet}&brand=${selected}&page=${page}&page_size=${pageSize}&extremum=${quality}&sorting_column=${sortingColumn}&sorting_order=${sortingOrder}`;
        const response = (await fetchWithAuth(url)) as TableExtremumResult | null;

        // Calculate remaining time to ensure minimum 500ms loading
        const elapsedTime = Date.now() - startTime;
        const remainingTime = Math.max(0, minLoadingTime - elapsedTime);

        if (response) {
          const newData = response.vehicles || [];
          const newBrands = response.brands || [];
          const newPagination = response.pagination || null;

          // Wait for the remaining time before updating data
          if (remainingTime > 0) {
            await new Promise((resolve) => setTimeout(resolve, remainingTime));
          }

          // Update data after the minimum loading time
          if (isLoadMore) {
            setData((prev) => [...prev, ...newData]);
          } else {
            setData(newData);
            setBrands([{ oem_id: 'All', oem_name: 'All brands' }, ...newBrands]);
          }

          setPagination(newPagination);
          setCurrentPage(page);
        }
      } catch (err) {
        setError(err);
      } finally {
        setIsLoading(false);
        setIsLoadingMore(false);
      }
    },
    [fleet, selected, quality, pageSize, sortingColumn, sortingOrder],
  );

  const loadMore = useCallback(() => {
    if (pagination?.has_next && !isLoadingMore && !isLoading) {
      fetchData(currentPage + 1, true);
    }
  }, [pagination?.has_next, isLoadingMore, isLoading, currentPage, fetchData]);

  const reset = useCallback(() => {
    setData([]);
    setBrands([]);
    setPagination(null);
    setCurrentPage(1);
    setError(null);
  }, []);

  // Initial load and reset when dependencies change
  useEffect(() => {
    reset();
    fetchData(1, false);
  }, [fleet, selected, quality, pageSize, sortingColumn, sortingOrder, reset, fetchData]);

  return {
    data,
    brands,
    pagination,
    isLoading,
    isLoadingMore,
    error,
    hasNextPage: pagination?.has_next || false,
    loadMore,
    reset,
  };
};

export default useInfiniteTableExtremum;
