import { SortOrder } from '@/interfaces/common/filter/SortOrder';

export type UseSortableFilterReturn<ActiveFilterType> = {
  activeFilter: ActiveFilterType | '';
  sortOrder: SortOrder;
  handleChangeFilter: (filter: ActiveFilterType) => void;
};
