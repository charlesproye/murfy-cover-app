import React from 'react';
import { useAuth } from '@/contexts/AuthContext';
import useGetFilters from '@/hooks/dashboard/global/useGetFilters';
import {
  FilterOption,
  ListFilterProps,
} from '@/interfaces/common/optionSelector/DataFilter';
import OptionSelectorMultiple from './OptionSelectorMultiple';

const ListFilter: React.FC<ListFilterProps> = ({ selectedFilters, onFiltersChange }) => {
  const { fleet } = useAuth();
  const [showPinnedOnly, setShowPinnedOnly] = React.useState(false);

  const { data } = useGetFilters(fleet?.id ?? null);

  const handleFilterChange = (filterId: string, selectedValues: string[]): void => {
    const filterIndex = selectedFilters.findIndex((filter) => filter.key === filterId);

    let newFilters;
    if (filterIndex === -1) {
      newFilters = [...selectedFilters, { key: filterId, values: selectedValues }];
    } else {
      newFilters = selectedFilters.map((filter) =>
        filter.key === filterId ? { ...filter, values: selectedValues } : filter,
      );
    }

    onFiltersChange(newFilters);
  };

  const handlePinnedChange = (isPinned: boolean): void => {
    setShowPinnedOnly(isPinned);
    const filterIndex = selectedFilters.findIndex((filter) => filter.key === 'pinned');

    let newFilters;
    if (filterIndex === -1) {
      newFilters = [
        ...selectedFilters,
        { key: 'pinned', values: isPinned ? ['true'] : [] },
      ];
    } else {
      newFilters = selectedFilters.map((filter) =>
        filter.key === 'pinned'
          ? { ...filter, values: isPinned ? ['true'] : [] }
          : filter,
      );
    }

    onFiltersChange(newFilters);
  };

  const getSelectedValues = (filterId: string): string[] => {
    const filter = selectedFilters.find((filter) => filter.key === filterId);
    return filter?.values || [];
  };

  return (
    <>
      <div className="flex flex-col gap-4">
        {/* Toggle pour les véhicules épinglés */}
        <div className="flex items-center gap-4 bg-white rounded-xl shadow px-4 py-3 w-fit">
          <span className="font-light text-sm text-gray-blue">
            {showPinnedOnly ? 'Show all vehicles' : 'Show favorite vehicles only'}
          </span>
          <label className="relative inline-flex items-center cursor-pointer">
            <input
              type="checkbox"
              className="sr-only peer"
              checked={showPinnedOnly}
              onChange={(e) => handlePinnedChange(e.target.checked)}
            />
            <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-green-500 rounded-full peer peer-checked:bg-green-500 transition-all"></div>
            <div className="absolute left-1 top-1 bg-white w-4 h-4 rounded-full shadow peer-checked:translate-x-5 transition-all"></div>
          </label>
        </div>

        {/* Filtres existants */}
      </div>
      <div className="flex flex-col gap-4 md:gap-2 md:flex-row justify-center md:justify-evenly w-full">
        {data?.map((filter: FilterOption) => (
          <div
            className="max-w-96 w-[90%] sm:w-[50%] md:w-full mx-auto flex"
            key={filter.title}
          >
            <OptionSelectorMultiple
              data={filter}
              selectedValues={getSelectedValues(filter.title)}
              changeDataOption={(selectedValues) =>
                handleFilterChange(filter.title, selectedValues)
              }
            />
          </div>
        ))}
      </div>
    </>
  );
};

export default ListFilter;
