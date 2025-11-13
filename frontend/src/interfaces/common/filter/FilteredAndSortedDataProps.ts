import { SortOrder } from '@/interfaces/common/filter/SortOrder';

export type UseFilteredAndSortedDataProps<T> = {
  data: T[] | undefined;
  activeFilter: keyof T | '';
  sortOrder: SortOrder;
};
