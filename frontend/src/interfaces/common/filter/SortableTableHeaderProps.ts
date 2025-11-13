import { SortOrder } from '@/interfaces/common/filter/SortOrder';

export type SortableTableHeaderProps<ActiveFilterType extends string> = {
  label: string;
  filter: ActiveFilterType;
  activeFilter: ActiveFilterType | '';
  sortOrder: SortOrder;
  onChangeFilter: (filter: ActiveFilterType) => void;
};
