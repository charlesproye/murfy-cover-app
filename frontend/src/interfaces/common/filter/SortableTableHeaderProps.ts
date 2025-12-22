import { SortOrder } from '@/interfaces/common/filter/SortOrder';
import { ReactNode } from 'react';

export type SortableTableHeaderProps<ActiveFilterType extends string> = {
  label: string | ReactNode;
  filter: ActiveFilterType;
  activeFilter: ActiveFilterType | '';
  sortOrder: SortOrder;
  onChangeFilter: (filter: ActiveFilterType) => void;
};
