import { SortOrder } from '@/interfaces/common/filter/SortOrder';
import { useMemo } from 'react';

export const useSortedData = <T>(
  data: T[] | undefined,
  activeFilter: string,
  sortOrder: SortOrder,
): T[] => {
  return useMemo(() => {
    if (!data) return [];

    return [...data].sort((a, b) => {
      const aValue = a[activeFilter as keyof T];
      const bValue = b[activeFilter as keyof T];

      if (aValue === null || aValue === undefined) return 1;
      if (bValue === null || bValue === undefined) return -1;

      const comparison = aValue > bValue ? 1 : aValue < bValue ? -1 : 0;
      return sortOrder === 'asc' ? comparison : -comparison;
    });
  }, [data, activeFilter, sortOrder]);
};
