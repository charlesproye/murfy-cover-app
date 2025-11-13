'use client';

import { UseSortableFilterReturn } from '@/interfaces/common/optionSelector/DataFilter';
import { SortOrder } from '@/interfaces/common/filter/SortOrder';
import { useState, useCallback } from 'react';

const useSortableFilter = <
  ActiveFilterType extends string | number,
>(): UseSortableFilterReturn<ActiveFilterType> => {
  const [activeFilter, setActiveFilter] = useState<ActiveFilterType | ''>('');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');

  const handleChangeFilter = useCallback(
    (filter: ActiveFilterType) => {
      setSortOrder((prevSortOrder) => (prevSortOrder === 'asc' ? 'desc' : 'asc'));
      if (activeFilter !== filter) setActiveFilter(filter);
    },
    [activeFilter],
  );

  return {
    activeFilter,
    sortOrder,
    handleChangeFilter,
  };
};

export default useSortableFilter;
