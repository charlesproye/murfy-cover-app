'use client';

import { UseFilteredAndSortedDataProps } from '@/interfaces/common/filter/FilteredAndSortedDataProps';
import { useMemo } from 'react';

const useFilteredAndSortedData = <T>({
  data,
  activeFilter,
  sortOrder,
}: UseFilteredAndSortedDataProps<T>): T[] =>
  useMemo(() => {
    if (!Array.isArray(data)) return [];

    if (!activeFilter) return data;

    const sortKey = activeFilter;
    if (!sortKey) return data;
    return [...data].sort((a, b) => {
      if (typeof a[sortKey] === 'string' && typeof b[sortKey] === 'string') {
        return sortOrder === 'asc'
          ? a[sortKey].localeCompare(b[sortKey])
          : b[sortKey].localeCompare(a[sortKey]);
      }
      if (typeof a[sortKey] === 'number' && typeof b[sortKey] === 'number') {
        return sortOrder === 'asc' ? a[sortKey] - b[sortKey] : b[sortKey] - a[sortKey];
      }
      return 0;
    });
  }, [data, activeFilter, sortOrder]);

export default useFilteredAndSortedData;
