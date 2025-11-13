import { SortOrder } from '@/interfaces/common/filter/SortOrder';

export type FilterData = {
  vehicle_model_id: string;
  id: string;
  name: string;
};

export type FilterOption = {
  title: string;
  data: FilterData[];
};

export type OptionSelectorProps = {
  data: FilterOption;
  selectedValue: string;
  changeDataOption: (selectedValue: string) => void;
};

export type OptionSelectorMultipleProps = {
  data: FilterOption;
  selectedValues: string[];
  changeDataOption: (selectedValues: string[]) => void;
};

export type DataRequestFilter = {
  data: FilterOption[] | undefined;
  isLoading: boolean;
  error: unknown;
};

export type SelectedFilter = {
  key: string;
  values: string[];
};

export type ListFilterProps = {
  selectedFilters: SelectedFilter[];
  onFiltersChange: (filters: SelectedFilter[]) => void;
};

export type UseSortableFilterReturn<ActiveFilterType> = {
  activeFilter: ActiveFilterType | '';
  sortOrder: SortOrder;
  handleChangeFilter: (filter: ActiveFilterType) => void;
};
